"""
Background Scheduler Service - Phase 3
Automates news fetching and database cleanup using APScheduler
"""
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timedelta
import logging
import pytz

from app.services.news_aggregator import NewsAggregator
from app.services.appwrite_db import get_appwrite_db
from app.services.cache_service import CacheService
from app.services.upstash_cache import get_upstash_cache   # Needed to bust stale news_v3 keys
from app.services.adaptive_scheduler import get_adaptive_scheduler, AdaptiveScheduler
from app.services.research_aggregator import ResearchAggregator
from app.config import settings
# Phase 13: Global image enrichment — fills missing og:image across ALL providers
from app.services.utils.image_enricher import extract_top_image

# Phase 23: Upgraded to the custom ANSI-aligned logger.
# get_logger() wraps the standard logging.getLogger() with our AlignedColorFormatter.
# The output format is: timestamp | LEVEL | module-name | message
# This makes async logs from 22 concurrent categories scannable by human eyes.
from app.utils.custom_logger import get_logger, TAG_START, TAG_GATE, TAG_ENRICH, TAG_DB, TAG_ERROR
logger = get_logger(__name__)

# Initialize scheduler
scheduler = AsyncIOScheduler()

# Import the single source of truth for categories.
# The full list now lives in app/config.py — edit it there, not here.
from app.config import CATEGORIES

# --------------------------------------------------------------------------
# MODULE-LEVEL SINGLETONS (Phase 6)
# --------------------------------------------------------------------------
# These two objects are created ONCE when the server starts and are shared
# by all 22 per-category jobs for the entire lifetime of the process.
#
# _shared_aggregator  — one NewsAggregator for all categories (Phase 1 fix).
#   It holds provider state (quota counts, circuit-breaker) that must
#   survive across job runs. Creating a new one for every job would reset
#   all that carefully maintained state.
#
# _adaptive  — the AdaptiveScheduler that tracks how many articles each
#   category produces and adjusts its fetch interval accordingly.
#   Also persists to disk (data/velocity_tracking.json) so intervals
#   survive server restarts.
# --------------------------------------------------------------------------
_shared_aggregator = None
_adaptive          = None


def _get_shared_aggregator():
    """Return (creating if needed) the one shared NewsAggregator instance."""
    global _shared_aggregator
    if _shared_aggregator is None:
        _shared_aggregator = NewsAggregator()
        logger.info("[AGGREGATOR] Shared NewsAggregator created (singleton).")
    return _shared_aggregator


def _get_adaptive():
    """Return (creating if needed) the one shared AdaptiveScheduler instance."""
    global _adaptive
    if _adaptive is None:
        _adaptive = get_adaptive_scheduler(CATEGORIES)
        logger.info("[ADAPTIVE] AdaptiveScheduler created for %d categories.", len(CATEGORIES))
    return _adaptive


async def fetch_all_news():
    """
    Background Job: Parallel news fetching for all categories
    
    Performance Improvements:
    - Parallel (NEW): All categories at once = ~30 seconds
    
    Runs every 1 hour to keep database fresh with latest articles.
    """
    start_time = datetime.now()
    
    logger.info("═" * 80)
    logger.info("📰 [NEWS FETCHER] Starting PARALLEL news fetch...")
    logger.info("🕐 Start Time: %s", start_time.strftime('%Y-%m-%d %H:%M:%S'))
    logger.info("🚀 Mode: Concurrent (asyncio.gather)")
    logger.info("═" * 80)
    
    # Tracking for observability
    total_fetched = 0
    total_saved = 0
    total_duplicates = 0
    total_errors = 0
    total_invalid = 0
    total_irrelevant = 0
    category_stats = {}
    
    # Parallel fetch all categories at once.
    # We create ONE shared aggregator here so all 22 category tasks share
    # the same provider state (quota counts, circuit states, etc.).
    # Fix #3 (Phase 7): Use the permanent module-level singleton instead of
    # creating a fresh instance here. This ensures that even manual triggers
    # respect the live quota counts and circuit-breaker state from the
    # adaptive jobs that may already be running.
    shared_aggregator = _get_shared_aggregator()
    
    # Bounded concurrency: fetch a maximum of 3 categories simultaneously 
    # to prevent Hugging Face Space network / DNS overload (503 errors).
    semaphore = asyncio.Semaphore(3)
    
    async def fetch_with_semaphore(category):
        async with semaphore:
            return await fetch_and_validate_category(category, shared_aggregator)

    fetch_tasks = []
    for category in CATEGORIES:
        fetch_tasks.append(fetch_with_semaphore(category))
    
    # Execute all fetches concurrently with error isolation and bounded concurrency
    logger.info("⚡ Launching %d fetch tasks (max 3 concurrently)...", len(CATEGORIES))
    results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    
    # Process results
    appwrite_db = get_appwrite_db()
    cache_service = CacheService()
    
    for result in results:
        # Handle errors gracefully
        if isinstance(result, Exception):
            logger.error("❌ Fetch task failed: %s", str(result))
            total_errors += 1
            continue
        
        # Unpack 5-tuple — relevant_count (5th item) is not needed here,
        # it is only used by fetch_single_category_job for adaptive velocity.
        category, articles, invalid_count, irrelevant_count, _ = result
        
        if not articles:
            logger.warning("⚠️  No valid articles for category: %s", category)
            category_stats[category] = {
                'fetched': 0,
                'saved': 0,
                'duplicates': 0,
                'invalid': invalid_count,
                'irrelevant': irrelevant_count
            }
            continue
        
        try:
            # Save to Appwrite database (L2)
            logger.info("💾 Saving %d articles for %s...", len(articles), category.upper())
            saved_count, duplicate_count, error_count, saved_docs = await appwrite_db.save_articles(articles)
            
            # Note: Shadow Path (Agentic RAG) removed.
            # We now rely solely on direct fetch -> store.
            
            total_fetched += len(articles)
            total_saved += saved_count
            total_duplicates += duplicate_count
            
            # If there were errors, add them to total errors
            if error_count > 0:
                total_errors += 1 
                logger.error(f"❌ {error_count} articles failed to save in {category}")
            
            total_invalid += invalid_count
            total_irrelevant += irrelevant_count
            
            # Store category stats
            category_stats[category] = {
                'fetched': len(articles),
                'saved': saved_count,
                'duplicates': duplicate_count,
                'errors': error_count,
                'invalid': invalid_count,
                'irrelevant': irrelevant_count
            }
            
            # Update Redis cache (L1) if available
            try:
                await cache_service.set(f"news:{category}", articles, ttl=settings.CACHE_TTL)
                logger.info("⚡ Redis cache updated for %s", category)
            except Exception as e:
                logger.debug("⚠️  Redis unavailable: %s", e)
            
            logger.info("✅ %s: %d fetched, %d saved, %d duplicates, %d errors, %d invalid", 
                       category.upper(), len(articles), saved_count, duplicate_count, error_count, invalid_count)
                       
        except Exception as e:
            total_errors += 1
            category_stats[category] = {'error': str(e), 'invalid': invalid_count}
            logger.error("❌ Error saving %s: %s", category, str(e))
    
    # End-of-run report
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("")
    logger.info("═" * 80)
    logger.info("🎉 [NEWS FETCHER] RUN COMPLETED")
    logger.info("═" * 80)
    logger.info("📊 SUMMARY STATISTICS:")
    logger.info("   🔹 Total Fetched: %d articles", total_fetched)
    logger.info("   🔹 Total Saved (New): %d articles", total_saved)
    logger.info("   🔹 Total Duplicates Skipped: %d articles", total_duplicates)
    logger.info("   🔹 Total Invalid Rejected: %d articles", total_invalid)
    logger.info("   🔹 Total Irrelevant Rejected: %d articles", total_irrelevant)
    logger.info("   🔹 Total Errors: %d categories", total_errors)
    logger.info("   🔹 Categories Processed: %d/%d", len(CATEGORIES) - total_errors, len(CATEGORIES))
    logger.info("   🔹 Deduplication Rate: %.1f%%", (total_duplicates / total_fetched * 100) if total_fetched > 0 else 0)
    logger.info("")
    logger.info("⏱️  PERFORMANCE:")
    logger.info("   🔹 Start: %s", start_time.strftime('%H:%M:%S'))
    logger.info("   🔹 End: %s", end_time.strftime('%H:%M:%S'))
    logger.info("   🔹 Duration: %.2f seconds", duration)
    logger.info("   🔹 Throughput: %.1f articles/second", total_fetched / duration if duration > 0 else 0)
    logger.info("═" * 80)
    
    # Record ingestion metrics for monitoring
    from app.services.ingestion_metrics import get_ingestion_metrics
    
    ingestion_metrics = get_ingestion_metrics()
    ingestion_metrics.record_run(
        fetched=total_fetched,
        saved=total_saved,
        duplicates=total_duplicates,
        errors=total_errors,
        categories_processed=len(CATEGORIES) - total_errors
    )
    
    # Update adaptive scheduler intervals
    # (kept for backward compat — manual trigger may still call this)
    adaptive = _get_adaptive()
    if adaptive:
        for cat, stats in category_stats.items():
            if 'fetched' in stats:
                adaptive.update_category_velocity(cat, stats['fetched'])
        adaptive.print_summary()


async def fetch_single_category_job(category: str):
    """
    Per-category background job (Phase 6).

    This is what each of the 22 adaptive jobs calls every N minutes.
    It is a self-contained unit: fetch → validate → save → report → reschedule.

    In plain English:
      Think of this like a delivery driver who has a single route (one category).
      After every delivery run, the dispatcher (adaptive scheduler) checks how
      many packages were delivered. If the route is always busy (lots of news),
      the driver gets sent out more often. If the route is quiet, the driver
      waits longer before going out again.
    """
    aggregator = _get_shared_aggregator()
    adaptive   = _get_adaptive()

    logger.info("[ADAPTIVE JOB] Starting fetch for category: %s", category.upper())

    try:
        # Step 1: Fetch + validate (calls the full Phase 1-4 pipeline).
        result = await fetch_and_validate_category(category, aggregator)

        if isinstance(result, Exception):
            logger.error("[ADAPTIVE JOB] %s fetch failed: %s", category, result)
            return

        # Unpack the 5-tuple returned by fetch_and_validate_category.
        # relevant_count = articles that passed Steps 1+2 (valid + on-topic)
        # but before Step 3 (Redis 48h dedup) filtered them.
        # This is the true measure of how active a category's news feed is.
        cat, articles, invalid_count, irrelevant_count, relevant_count = result

        if not articles:
            logger.info("[ADAPTIVE JOB] %s: No valid articles this run.", category.upper())
            saved_count = 0
        else:
            # Step 2: Save to Appwrite.
            appwrite_db   = get_appwrite_db()
            cache_service = CacheService()

            logger.info("[ADAPTIVE JOB] %s: Saving %d articles...", category.upper(), len(articles))
            saved_count, duplicate_count, error_count, _ = await appwrite_db.save_articles(articles)

            logger.info(
                "[ADAPTIVE JOB] %s: %d saved, %d duplicates, %d errors, "
                "%d invalid, %d irrelevant.",
                category.upper(), saved_count, duplicate_count, error_count,
                invalid_count, irrelevant_count
            )

            # Step 3a: Bust the Upstash news_v3 cache for this category.
            #
            # The news route (/api/news/<category>) caches its response in Upstash
            # under the key  "news_v3:<category>:page:<N>:l<limit>".
            # Without this delete, a user hitting the page right after an Appwrite
            # save would still get the stale 5-minute-old response (which may be
            # empty), because the cache has not expired yet.
            #
            # Fix: the moment we save new articles, we surgically delete page-1
            # of this category's cache. This forces the very next API call to
            # bypass the cache and read fresh data from Appwrite.
            if saved_count > 0:
                try:
                    upstash = get_upstash_cache()
                    # Delete the most-visited page (page 1, default limit 20).
                    # Other pages will expire naturally on their 5-min TTL.
                    stale_key = f"news_v3:{category}:page:1:l20"
                    await upstash.delete(stale_key)
                    logger.info("[CACHE BUST] Deleted stale key '%s' — fresh articles will appear immediately.", stale_key)
                except Exception as bust_err:
                    # Cache bust failure is not fatal — articles are already in Appwrite.
                    # The stale cache will expire on its own in at most 5 minutes.
                    logger.debug("[CACHE BUST] Could not delete stale key: %s", bust_err)

            # Step 3b: Also update the legacy Redis L1 article cache.
            try:
                await cache_service.set(f"news:{category}", articles, ttl=settings.CACHE_TTL)
            except Exception as cache_err:
                logger.debug("[ADAPTIVE JOB] Redis cache update skipped: %s", cache_err)

        # Step 4: Feed result count back to the adaptive scheduler.
        # We use relevant_count (articles that passed validation + keyword relevance)
        # rather than saved_count (articles actually new to Appwrite).
        #
        # Why? A busy category with a slow-updating RSS feed will have high
        # relevant_count but low saved_count (we already have the articles).
        # Using saved_count would incorrectly mark it as "quiet" and slow it down.
        # relevant_count correctly reflects: "how much real news is out there?"
        if adaptive:
            # Fix #1 (Phase 7): Read old_interval NOW, before update_category_velocity
            # overwrites data['interval'] inside the AdaptiveScheduler.
            # The comparison new_interval != old_interval was always False before
            # because we were reading the interval AFTER it was already updated.
            old_interval = adaptive.get_interval(category)

            # Now update velocity with the correct metric (in-memory only — instant).
            new_interval = adaptive.update_category_velocity(category, relevant_count)

            # Persist the updated velocity to Redis asynchronously.
            # async_persist() uses httpx.AsyncClient so it never blocks the event loop.
            # Think of it like dropping a letter in a post box — we do not stand
            # and wait for the postman to deliver it. We just drop it and walk on.
            await adaptive.async_persist()

            # Step 5: If the interval genuinely changed, tell APScheduler
            # to reschedule this specific job live — no server restart needed.
            if new_interval != old_interval:
                job_id = f"fetch_{category}"
                try:
                    scheduler.reschedule_job(
                        job_id,
                        trigger=IntervalTrigger(minutes=new_interval)
                    )
                    logger.info(
                        "[ADAPTIVE] %s interval changed: %dmin → %dmin. Job rescheduled live.",
                        category.upper(), old_interval, new_interval
                    )
                except Exception as reschedule_err:
                    logger.warning(
                        "[ADAPTIVE] Could not reschedule %s job: %s",
                        job_id, reschedule_err
                    )

    except Exception as e:
        logger.exception("[ADAPTIVE JOB] Unhandled error for category %s: %s", category, e)


async def fetch_daily_research():
    """
    Background Job: Fetch Research Papers from ArXiv
    Runs daily at 02:00 IST
    """
    logger.info("═" * 80)
    logger.info("🔬 [RESEARCH FETCHER] Starting daily research fetch...")
    logger.info("🕐 Start Time: %s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    logger.info("═" * 80)
    
    try:
        aggregator = ResearchAggregator()
        saved_count = await aggregator.fetch_and_process_daily_papers()
        logger.info(f"✅ [RESEARCH FETCHER] Completed. Saved {saved_count} new papers.")
        
    except Exception as e:
        logger.error(f"❌ [RESEARCH FETCHER] Failed: {e}", exc_info=True)
    
    logger.info("═" * 80)


# ──────────────────────────────────────────────────────────────────────────────
# PHASE 13: GLOBAL IMAGE ENRICHMENT SAFETY NET
# ──────────────────────────────────────────────────────────────────────────────
#
# What this does:
#   After all validation and deduplication gates have passed, some articles
#   still arrive with an empty or missing image_url. This happens most often
#   with providers like OpenRSS (blog feeds without media tags), Webz.io
#   (small sites without a thread.main_image), and SauravKanchan (NewsAPI
#   null urlToImage). This function visits the article's URL and tries to
#   extract the og:image meta tag — the standard way websites declare their
#   main thumbnail image.
#
# Why AFTER deduplication?
#   We only enrich articles that actually passed every gate and are about to
#   be saved. We never spend HTTP calls on articles that will be thrown away.
#
# Safety guards:
#   1. MAX_ENRICH_PER_RUN = 20  — Hard cap. If 50 no-image articles arrive,
#      we only enrich the first 20, leave the rest as "", and the Pulse banner
#      shows on the frontend. This stops a rogue provider from bottlenecking
#      the cron job.
#   2. asyncio.Semaphore(10)    — At most 10 web-page fetches happen at the
#      same time. This prevents memory spikes and avoids hammering websites.
#   3. Individual 4-second timeout (inside extract_top_image) — A broken URL
#      is cancelled in 4 seconds. With Semaphore(10) and MAX 20 articles:
#      worst-case total overhead = (20 / 10) × 4 = 8 seconds per category run.
#   4. Zero side-effects — A failed enrichment returns the article unchanged.
#      The enricher NEVER removes an article from the pipeline.
#
async def enrich_missing_images_in_batch(articles: list, delay_seconds: float = 0.0) -> list:
    """
    Scan a list of fully-vetted articles and fill in any missing images.

    Only enriches up to MAX_ENRICH_PER_RUN articles that have no valid
    image_url. Articles that already have an image are passed through
    instantly with zero network cost.

    Args:
        articles (list): Final, deduplicated, validated Article objects.
        delay_seconds (float): Optional delay between concurrent requests to avoid IP bans.

    Returns:
        list: Same articles, with image_url filled where possible.
              Never raises. Never removes an article.
    """
    if not articles:
        return articles

    # ── Constants ─────────────────────────────────────────────────────────────
    # Cap: only attempt image enrichment on the first 20 articles that need it.
    # The rest go to the database as-is (empty image = Pulse banner fallback).
    MAX_ENRICH_PER_RUN = 20

    # Semaphore: at most 10 website fetches run simultaneously.
    # Think of it like a queue of 10 checkout lanes at a supermarket.
    # If 20 people arrive at once, 10 go straight through and 10 wait
    # in line. Nobody gets turned away, but the store doesn't explode.
    sem = asyncio.Semaphore(10)

    # ── Count how many articles actually need enrichment ───────────────────────
    articles_needing_images = [
        a for a in articles
        if not a.image_url or not a.image_url.startswith("http")
    ]
    enrich_count = min(len(articles_needing_images), MAX_ENRICH_PER_RUN)

    if enrich_count == 0:
        # Every article already has a valid image. Nothing to do.
        return articles

    logger.info(
        "🖼️  [IMAGE ENRICHER] %d article(s) missing images — enriching up to %d...",
        len(articles_needing_images), enrich_count
    )

    # Build a lookup set of URLs to enrich (only the capped subset).
    urls_to_enrich = {
        str(a.url) for a in articles_needing_images[:MAX_ENRICH_PER_RUN]
    }

    # ── Internal worker: enrich one article ───────────────────────────────────
    async def _enrich_one(article) -> object:
        """
        If this article needs an image, fetch it under the semaphore guard.
        Returns the article (updated or unchanged).
        """
        url_str = str(article.url) if article.url else ""

        # Article already has a valid image, or it's outside the cap — skip.
        if url_str not in urls_to_enrich:
            return article

        async with sem:
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
            # Semaphore acquired: one of our 10 lanes is now occupied.
            # extract_top_image has its own 4-second internal timeout,
            # so this will release the lane quickly regardless of outcome.
            image_url = await extract_top_image(url_str)

        if image_url and image_url.startswith("http"):
            # Got a valid image — update the article cleanly.
            # model_copy() is the correct Pydantic v2 pattern for immutable models.
            return article.model_copy(update={"image_url": image_url})

        # No image found or fetch failed — return article unchanged.
        return article

    # ── Run all workers concurrently ───────────────────────────────────────────
    # All articles go into gather() at once. The semaphore controls how many
    # actually hit the network at the same time (max 10). The rest wait
    # in asyncio's queue without blocking the event loop.
    try:
        enriched_articles = await asyncio.gather(
            *[_enrich_one(a) for a in articles],
            return_exceptions=True
        )

        # Replace any Exception results with the original article (safe fallback).
        final = []
        for original, result in zip(articles, enriched_articles):
            if isinstance(result, Exception):
                logger.debug(
                    "[IMAGE ENRICHER] Worker exception for %s: %s",
                    str(original.url)[:60], result
                )
                final.append(original)           # Keep original if worker crashed
            else:
                final.append(result)

        enriched_total = sum(
            1 for a in final if a.image_url and a.image_url.startswith("http")
        )
        logger.info(
            "✅ [IMAGE ENRICHER] Done — %d/%d articles now have images.",
            enriched_total, len(final)
        )
        return final

    except Exception as e:
        # If the entire gather somehow fails, return the original list untouched.
        logger.error("[IMAGE ENRICHER] Gather failed: %s — returning articles unchanged.", e)
        return articles


async def fetch_and_validate_category(category: str, aggregator) -> tuple:
    """
    Fetch and validate articles for a single category.

    Args:
        category:   The news category (e.g. 'ai', 'cloud-aws').
        aggregator: The shared NewsAggregator instance for this run.
                    Using a shared instance means all 22 parallel tasks
                    share the same quota counters and circuit-breaker state.

    Returns: (category, valid_articles, invalid_count, irrelevant_count, relevant_count)
    """
    from app.utils.data_validation import is_valid_article, sanitize_article, is_relevant_to_category
    from app.utils.date_parser import normalize_article_date
    from app.utils.url_canonicalization import canonicalize_url
    from app.utils.redis_dedup import is_url_seen_or_mark
    from app.models import Article   # Needed to reconstruct Pydantic model after date normalization
    
    try:
        logger.info("%s Fetching category [%s]...", TAG_START, category.upper())
        
        # Ask the aggregator for all articles from all sources for this category.
        # fetch_by_category (Phase 5) internally runs:
        #   1. Paid waterfall  — GNews → NewsAPI → NewsData (stops on first success)
        #   2. Free parallel   — Google RSS + Medium + Official Cloud, all at once
        #   3. Returns the merged list
        # We no longer need to call fetch_from_provider for medium/official_cloud
        # separately here. That would duplicate the work Phase 5 already does.
        raw_articles = await aggregator.fetch_by_category(category)
        
        if not raw_articles:
            return (category, [], 0, 0, 0)

        # ------------------------------------------------------------------
        # IN-BATCH DEDUPLICATION
        # ------------------------------------------------------------------
        # When 3 providers run at the same time for the same category, they
        # sometimes return the exact same article (e.g. a TechCrunch AI story
        # can come from both GNews AND Google RSS in the same fetch cycle).
        # We catch and remove these same-batch duplicates RIGHT HERE, before
        # the expensive validation loop even starts.
        # This is like a quick ID-card check at the entrance before people
        # join the full security screening queue.
        _seen_in_batch: set = set()
        _deduplicated_raw = []
        for _art in raw_articles:
            _raw_url = str(_art.url) if _art.url else ''
            _canonical = canonicalize_url(_raw_url) if _raw_url else ''
            # If we have a valid canonical URL and we've already seen it → skip
            if _canonical and _canonical in _seen_in_batch:
                continue
            if _canonical:
                _seen_in_batch.add(_canonical)
            _deduplicated_raw.append(_art)

        _batch_dupes_removed = len(raw_articles) - len(_deduplicated_raw)
        if _batch_dupes_removed > 0:
            logger.info(
                "   🔄 [BATCH DEDUP] %s: Removed %d within-batch duplicates before validation",
                category.upper(), _batch_dupes_removed
            )
        raw_articles = _deduplicated_raw
        # ------------------------------------------------------------------
        
        # Validate, filter, and sanitize
        valid_articles = []
        invalid_count = 0
        irrelevant_count = 0
        relevant_count = 0   # articles that are valid + relevant, before Redis dedup
        
        for article in raw_articles:
            # Step 1: Basic validation — must have a title, URL, and publication date.
            if not is_valid_article(article):
                invalid_count += 1
                continue

            # Step 2: Category relevance check — title+description must match category keywords.
            if not is_relevant_to_category(article, category):
                irrelevant_count += 1
                continue

            # Checkpoint: count articles that are valid AND relevant, but before
            # the Redis 48-hour check strips out the ones we have already stored.
            # This is the true "how much real news is in this category?" signal.
            # The adaptive scheduler uses this number to decide fetch frequency.
            # (Fix #2 - Phase 7: was using saved_count, which confused "quiet feed"
            # with "feed we already have fully stored" — two very different things.)
            relevant_count += 1

            # Step 3: Redis 48-hour dedup check — THE MAIN BOUNCER.
            # Check if we have already stored this exact article URL in the last 48 hours.
            # If yes, skip silently — it's a repeat. If no, mark it as seen and continue.
            # This stops the same article being saved every hour from a slow-updating RSS feed.
            if await is_url_seen_or_mark(str(article.url) if article.url else ''):
                logger.debug(
                    "   [REDIS DEDUP] Skipped article already seen in last 48 hours: %s",
                    str(article.url)[:80]
                )
                continue

            # Step 4: Normalize date to UTC ISO-8601.
            # IMPORTANT: normalize_article_date() always returns a plain dict
            # (it calls model_dump() internally). We reconstruct the Pydantic
            # Article right after so that enrich_missing_images_in_batch()
            # (Phase 13, below) gets the .image_url attribute it needs.
            normalized_dict = normalize_article_date(article)
            try:
                article = Article(**normalized_dict)
            except Exception:
                # If reconstruction fails for any reason, skip this article.
                # The dict is malformed — better to drop it than crash.
                invalid_count += 1
                continue

            # Step 5: Article is now a clean Pydantic object with a normalized date.
            # We intentionally do NOT call sanitize_article() yet — that step
            # runs AFTER image enrichment below.
            valid_articles.append(article)

        # ── PHASE 13: GLOBAL IMAGE ENRICHMENT ─────────────────────────────────
        # This is the bottom of the funnel. Every article here has already:
        #   ✓ Passed basic validation (title, URL, date exist)
        #   ✓ Passed category relevance check
        #   ✓ Passed Redis 48-hour deduplication (it is a NEW article)
        #   ✓ Been date-normalized
        # Articles are still Pydantic objects here — enrichment needs .image_url.
        if valid_articles:
            valid_articles = await enrich_missing_images_in_batch(valid_articles)

        # ── SANITIZE (after enrichment) ────────────────────────────────────────
        # Now that images are filled, convert each Pydantic Article to a clean
        # dict for Appwrite storage. sanitize_article() strips unsafe chars,
        # trims lengths, and returns the final dict payload.
        valid_articles = [sanitize_article(a) for a in valid_articles]
        # ──────────────────────────────────────────────────────────────────────

        logger.info("%s [%s] Valid: %d | Invalid: %d | Irrelevant: %d | Time: see APScheduler",
                    TAG_GATE, category.upper(), len(valid_articles), invalid_count, irrelevant_count)
        return (category, valid_articles, invalid_count, irrelevant_count, relevant_count)
        
    except asyncio.TimeoutError:
        logger.error("%s Timeout fetching [%s] (>30s)", TAG_ERROR, category)
        return (category, [], 0, 0, 0)
    except Exception as e:
        logger.exception("%s Error fetching [%s]", TAG_ERROR, category)
        return (category, [], 0, 0, 0)


async def cleanup_old_news():
    """
    Background Job: Delete articles older than 48 hours from ALL collections
    
    Runs every 30 minutes to keep Appwrite database within free tier limits.
    Only keeps the last 2 days of articles.
    """
    logger.info("")
    logger.info("═" * 80)
    logger.info("🧹 [CLEANUP JANITOR] Starting cleanup of old articles...")
    logger.info("🕐 Cleanup Time: %s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    logger.info("═" * 80)
    
    appwrite_db = get_appwrite_db()
    
    if not appwrite_db.initialized:
        logger.error("❌ CRITICAL: Appwrite database not initialized!")
        return
    
    try:
        # Calculate cutoff date (48 hours ago)
        retention_hours = 48
        cutoff_date = datetime.now() - timedelta(hours=retention_hours)
        cutoff_iso = cutoff_date.isoformat()
        
        logger.info("📋 Retention Policy: %d hours", retention_hours)
        logger.info("📅 Cutoff Date: %s", cutoff_date.strftime('%Y-%m-%d %H:%M:%S'))
        
        # Define all collections to clean
        target_collections = [
            ("Regular News", settings.APPWRITE_COLLECTION_ID),
            ("Cloud News", settings.APPWRITE_CLOUD_COLLECTION_ID),
            ("AI News", settings.APPWRITE_AI_COLLECTION_ID),
            ("Data News", settings.APPWRITE_DATA_COLLECTION_ID),
            ("Magazines", settings.APPWRITE_MAGAZINE_COLLECTION_ID),
            ("Medium Blogs", settings.APPWRITE_MEDIUM_COLLECTION_ID)
        ]
        
        total_deleted = 0
        from appwrite.query import Query
        
        for name, collection_id in target_collections:
            if not collection_id:
                logger.debug(f"⏭️  Skipping {name} (Not configured)")
                continue
                
            logger.info("")
            logger.info(f"📂 [{name}] Cleaning collection: {collection_id}...")
            
            try:
                # -------------------------------------------------------------
                # 1. SMART CHECK: "Hey collection, do you have old data?"
                # -------------------------------------------------------------
                check_response = await appwrite_db.tablesDB.list_rows(
                    database_id=settings.APPWRITE_DATABASE_ID,
                    collection_id=collection_id,
                    queries=[
                        Query.less_than('published_at', cutoff_iso),
                        Query.limit(1)  # Minimal query to check existence
                    ]
                )
                
                if len(check_response['documents']) == 0:
                    logger.info(f"✨ [{name}] Collection is clean (Smart Check Passed)")
                    continue
                    
                logger.info(f"🔍 [{name}] Found legacy data. Initiating cleanup sequence...")
                
                # -------------------------------------------------------------
                # 2. DEEP CLEAN: Delete full rows (attributes, engagement, etc.)
                # -------------------------------------------------------------
                total_collection_deleted = 0
                
                while True:
                    # Query old articles (Batch of 500)
                    response = await appwrite_db.tablesDB.list_rows(
                        database_id=settings.APPWRITE_DATABASE_ID,
                        collection_id=collection_id,
                        queries=[
                            Query.less_than('published_at', cutoff_iso),
                            Query.limit(500)
                        ]
                    )
                    
                    batch_count = len(response['documents'])
                    
                    if batch_count == 0:
                        logger.info(f"✅ [{name}] Cleanup complete. Total rows deleted: {total_collection_deleted}")
                        break
                        
                    logger.info(f"   [{name}] processing batch of {batch_count} rows...")
                    
                    batch_deleted = 0
                    for doc in response['documents']:
                        try:
                            # This deletes the FULL DOCUMENT (Row) including all attributes
                            # (published_at, url, image, likes, views, dislikes, etc.)
                            await appwrite_db.tablesDB.delete_row(
                                database_id=settings.APPWRITE_DATABASE_ID,
                                collection_id=collection_id,
                                document_id=doc['$id']
                            )
                            batch_deleted += 1
                        except Exception as e:
                            logger.error(f"❌ Error deleting row {doc['$id']}: {e}")
                            
                    total_collection_deleted += batch_deleted
                    total_deleted += batch_deleted
                    
                    # Safety break (User Request: 5,000 limit)
                    if total_collection_deleted >= 5000:
                         logger.warning(f"⚠️  [{name}] Hit safety limit (5,000). Pausing cleanup for next run.")
                         break

            except Exception as e:
                logger.warning(f"⚠️  Error accessing {name} collection: {e}")
        
        # =========================================================================
        # Clear Redis Cache
        # =========================================================================
        logger.info("")
        logger.info("🔄 Clearing Redis cache...")
        cache_service = CacheService()
        cache_cleared = 0
        for category in CATEGORIES:
            try:
                await cache_service.delete(f"news:{category}")
                cache_cleared += 1
            except Exception as e:
                logger.debug("⚠️  Cache clear skipped for %s: %s", category, e)
        
        if cache_cleared > 0:
            logger.info("✅ Cache cleared for %d categories", cache_cleared)
        
        # =========================================================================
        # Final Summary
        # =========================================================================
        logger.info("")
        logger.info("═" * 80)
        logger.info("🎉 [CLEANUP JANITOR] COMPLETED!")
        logger.info("🗑️  Total Deleted: %d articles across all collections", total_deleted)
        logger.info("⏰ Retention: Articles older than %d hours removed", retention_hours)
        logger.info("═" * 80)
        
    except Exception as e:
        logger.error("")
        logger.error("═" * 80)
        logger.error("❌ [CLEANUP JANITOR] FAILED!")
        logger.error("Error: %s", str(e))
        logger.error("═" * 80)
        logger.exception("Full traceback:")


async def background_image_enricher_job():
    """
    Background Job: Fetch articles across collections missing images and enrich them.
    Runs every 1 hour. Applies delays to avoid IP bans.
    """
    logger.info("")
    logger.info("═" * 80)
    logger.info("🖼️  [BACKGROUND ENRICHER] Starting missing image scan...")
    logger.info("═" * 80)
    
    appwrite_db = get_appwrite_db()
    if not appwrite_db.initialized:
        return
        
    try:
        from appwrite.query import Query
        from app.models import Article
        
        target_collections = [
            ("Regular News", settings.APPWRITE_COLLECTION_ID),
            ("Cloud News", settings.APPWRITE_CLOUD_COLLECTION_ID),
            ("AI News", settings.APPWRITE_AI_COLLECTION_ID),
            ("Data News", settings.APPWRITE_DATA_COLLECTION_ID),
        ]
        
        total_enriched = 0
        
        for name, collection_id in target_collections:
            if not collection_id:
                continue
                
            # Fetch 50 recent articles and locally filter for empty images
            response = await appwrite_db.tablesDB.list_rows(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=collection_id,
                queries=[
                    Query.order_desc('published_at'),
                    Query.limit(50)
                ]
            )
            
            docs = response.get('documents', [])
            # Pick max 10 to avoid scraping too intensely in background
            empty_docs = [d for d in docs if not d.get('image_url') and not d.get('image')][:10]
            
            if not empty_docs:
                continue
                
            logger.info(f"   [{name}] Found {len(empty_docs)} recent articles missing images. Enriching...")
            
            articles_to_enrich = []
            for doc in empty_docs:
                try:
                    doc_copy = dict(doc)
                    if '$id' in doc_copy:
                        doc_copy['id'] = doc_copy['$id']
                    art = Article(**doc_copy)
                    articles_to_enrich.append(art)
                except Exception as e:
                    pass
                    
            if not articles_to_enrich:
                continue
                
            # Add 2.0s delay between concurrent requests to be polite to news servers
            enriched = await enrich_missing_images_in_batch(articles_to_enrich, delay_seconds=2.0)
            
            for original, new_art in zip(articles_to_enrich, enriched):
                if new_art.image_url and new_art.image_url.startswith("http"):
                    try:
                        await appwrite_db.tablesDB.update_row(
                            database_id=settings.APPWRITE_DATABASE_ID,
                            collection_id=collection_id,
                            document_id=new_art.id,
                            data={'image_url': new_art.image_url, 'image': new_art.image_url}
                        )
                        total_enriched += 1
                    except Exception as e:
                        logger.error(f"Error saving enriched image for {new_art.id}: {e}")
                        
        logger.info(f"✅ [BACKGROUND ENRICHER] Done. {total_enriched} missing images successfully scraped and saved.")
        
    except Exception as e:
        logger.error(f"❌ [BACKGROUND ENRICHER] Failed: {e}", exc_info=True)


def start_scheduler():
    """
    Initialize and start the background scheduler with all jobs
    """
    logger.info("")
    logger.info("═" * 80)
    logger.info("⏰ [SCHEDULER] Initializing background scheduler...")
    logger.info("═" * 80)
    
    # ── Job #1: PER-CATEGORY ADAPTIVE NEWS FETCHERS (Phase 6) ───────────
    # Instead of one giant job that fetches all 22 categories every hour,
    # we register 22 individual jobs, each on its own timer.
    #
    # The timer for each category is read from the adaptive scheduler,
    # which remembers how "active" each category was in past runs:
    #   - 'ai' category gets lots of articles → runs every 5 minutes
    #   - 'cloud-alibaba' is quiet → runs every 60 minutes
    #   - Most categories start at 15 minutes (the default)
    #
    # After every run, the job updates its own timer if the velocity changed.
    # No server restart needed.
    # -------------------------------------------------------------------------
    adaptive = _get_adaptive()   # initializes singleton + loads saved intervals

    for idx, category in enumerate(CATEGORIES, start=1):
        initial_interval = adaptive.get_interval(category)  # minutes
        job_id = f"fetch_{category}"

        scheduler.add_job(
            fetch_single_category_job,
            trigger=IntervalTrigger(minutes=initial_interval),
            args=[category],
            id=job_id,
            name=f"News Fetcher: {category} (every {initial_interval}min)",
            replace_existing=True
        )
        logger.info(
            "   ✓ [%02d/%02d] %-30s → every %d min",
            idx, len(CATEGORIES), category, initial_interval
        )

    logger.info("")
    logger.info("✅ Job #1 Group Registered: 📰 %d Adaptive News Fetchers", len(CATEGORIES))
    logger.info("   Intervals range from 5 min (high-velocity) to 60 min (quiet)")
    
    # Cleanup Job (Frequency: Every 30 minutes)
    scheduler.add_job(
        cleanup_old_news,
        trigger=IntervalTrigger(minutes=30),
        id='cleanup_old_news',
        name='Database Janitor (every 30 mins)',
        replace_existing=True
    )
    logger.info("")
    logger.info("✅ Job #2 Registered: 🧹 Database Janitor")
    logger.info("   ⏱️  Schedule: Every 30 minutes")
    logger.info("   📋 Task: Delete articles older than 48 hours")
    
    # Import newsletter service (lazy import)
    from app.services.newsletter_service import send_scheduled_newsletter
    
    # IST timezone for newsletter scheduling
    IST = pytz.timezone('Asia/Kolkata')
    
    # Newsletter Jobs
    newsletter_jobs = [
        ("Morning", 7, 0, 'mon-sat'),
        ("Afternoon", 14, 0, 'mon-fri'),
        ("Evening", 19, 0, None),
        ("Weekly", 9, 0, 'sun')
    ]
    
    job_counter = 3
    for name, hour, minute, days in newsletter_jobs:
        trigger_args = {'hour': hour, 'minute': minute, 'timezone': IST}
        if days:
            trigger_args['day_of_week'] = days
            
        scheduler.add_job(
            send_scheduled_newsletter,
            trigger=CronTrigger(**trigger_args),
            args=[name],
            id=f'newsletter_{name.lower()}',
            name=f'{name} Newsletter',
            replace_existing=True
        )
        logger.info("")
        logger.info(f"✅ Job #{job_counter} Registered: 📧 {name} Newsletter")
        job_counter += 1
        
    # Monthly Newsletter
    scheduler.add_job(
        send_scheduled_newsletter,
        trigger=CronTrigger(hour=9, minute=0, day=1, timezone=IST),
        args=["Monthly"],
        id='newsletter_monthly',
        name='Monthly Newsletter',
        replace_existing=True
    )
    logger.info("")
    logger.info(f"✅ Job #{job_counter} Registered: 📊 Monthly Newsletter")
    
    # Research Papers Job (Daily at 02:00 IST)
    scheduler.add_job(
        fetch_daily_research,
        trigger=CronTrigger(hour=2, minute=0, timezone=IST),
        id='fetch_research_papers',
        name='Research Fetcher (Daily 02:00 IST)',
        replace_existing=True
    )
    logger.info("")
    logger.info(f"✅ Job #{job_counter + 1} Registered: 🔬 Research Fetcher")

    # Background Image Enricher Job (Every 1 hour)
    scheduler.add_job(
        background_image_enricher_job,
        trigger=IntervalTrigger(hours=1),
        id='background_image_enricher',
        name='Image Enricher (every 1 hour)',
        replace_existing=True
    )
    logger.info("")
    logger.info(f"✅ Job #{job_counter + 2} Registered: 🖼️ Background Image Enricher")
    
    # Start the scheduler
    logger.info("")
    logger.info("🚀 Starting scheduler engine...")
    scheduler.start()
    logger.info("")
    logger.info("═" * 80)
    logger.info("✅ [SCHEDULER] Background scheduler started successfully!")
    logger.info("═" * 80)
    logger.info("")


def shutdown_scheduler():
    """
    Gracefully shutdown the scheduler
    """
    logger.info("")
    logger.info("═" * 80)
    logger.info("⏹️  [SCHEDULER] Shutting down background scheduler...")
    scheduler.shutdown(wait=True)
    logger.info("✅ [SCHEDULER] Background scheduler shut down successfully")
    logger.info("═" * 80)
    logger.info("")


# Manual job triggers for testing
async def trigger_fetch_now():
    """Manually trigger news fetch"""
    logger.info("🔧 [MANUAL TRIGGER] Running fetch job NOW...")
    await fetch_all_news()

async def trigger_cleanup_now():
    """Manually trigger cleanup"""
    logger.info("🔧 [MANUAL TRIGGER] Running cleanup job NOW...")
    await cleanup_old_news()

async def trigger_newsletter_now(preference: str):
    """Manually trigger newsletter"""
    from app.services.newsletter_service import send_scheduled_newsletter
    logger.info(f"🔧 [MANUAL TRIGGER] Running {preference} newsletter job NOW...")
    result = await send_scheduled_newsletter(preference)
    return result



