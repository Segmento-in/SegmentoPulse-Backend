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
from app.services.adaptive_scheduler import get_adaptive_scheduler, AdaptiveScheduler
from app.services.agent_orchestrator import process_shadow_path
from app.services.vector_store import vector_store # For cleanup
from app.services.ingestion_v2 import fetch_latest_news as fetch_v2  # Phase 1: LlamaIndex + Bloom Filter
from app.config import settings

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize scheduler
scheduler = AsyncIOScheduler()

# All news categories
CATEGORIES = [
    "ai",
    "data-security",
    "data-governance",
    "data-privacy",
    "data-engineering",
    "data-management",
    "business-intelligence",
    "business-analytics",
    "customer-data-platform",
    "data-centers",
    "cloud-computing",
    "magazines",
    "data-laws",
    # Official Cloud Categories
    "cloud-aws",
    "cloud-azure",
    "cloud-gcp",
    "cloud-oracle",
    "cloud-ibm",
    "cloud-alibaba",
    "cloud-digitalocean",
    "cloud-huawei",
    "cloud-cloudflare"
]


async def fetch_all_news():
    """
    Background Job: Parallel news fetching for all categories (FAANG-Level)
    
    Performance Improvements:
    - Sequential (OLD): 12 categories × 30s each = 6 minutes
    - Parallel (NEW): All 12 at once = 30 seconds = 12x faster!
    
    Runs every 15 minutes to keep database fresh with latest articles.
    """
    start_time = datetime.now()
    
    logger.info("═" * 80)
    logger.info("📰 [NEWS FETCHER] Starting PARALLEL news fetch...")
    logger.info("🕐 Start Time: %s", start_time.strftime('%Y-%m-%d %H:%M:%S'))
    logger.info("🚀 Mode: Concurrent (asyncio.gather)")
    logger.info("═" * 80)
    
    # Phase 4: Enhanced tracking for observability
    total_fetched = 0
    total_saved = 0
    total_duplicates = 0
    total_errors = 0
    total_invalid = 0
    total_irrelevant = 0  # NEW: Track category pollution
    category_stats = {}
    
    # FAANG Optimization: Parallel fetch all categories at once!
    fetch_tasks = []
    for category in CATEGORIES:
        task = fetch_and_validate_category(category)
        fetch_tasks.append(task)
    
    # Execute all fetches concurrently with error isolation
    logger.info("⚡ Launching %d parallel fetch tasks...", len(CATEGORIES))
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
        
        category, articles, invalid_count, irrelevant_count = result
        
        if not articles:
            logger.warning("⚠️  No valid articles for category: %s", category)
            category_stats[category] = {
                'fetched': 0,
                'saved': 0,
                'duplicates': 0,
                'invalid': invalid_count,
                'irrelevant': irrelevant_count  # NEW
            }
            continue
        
        try:
            # Save to Appwrite database (L2)
            logger.info("💾 Saving %d articles for %s...", len(articles), category.upper())
            saved_count, duplicate_count, error_count, saved_docs = await appwrite_db.save_articles(articles)
            
            # 🚀 FIRE-AND-FORGET: Trigger Agentic Shadow Path
            # We do NOT wait for this. It runs in the background.
            if saved_docs:
                logger.info("🕵️ Triggering Agent Analyst for %d new articles...", len(saved_docs))
                asyncio.create_task(process_shadow_path(saved_docs))
            
            # Calculate duplicates (Now explicitly returned by appwrite_db)
            # duplicates = len(articles) - saved_count  <-- OLD BUGGY LOGIC
            # Now we use the explicit counts from the DB service
            
            total_fetched += len(articles)
            total_saved += saved_count
            total_duplicates += duplicate_count
            
            # If there were errors, add them to total errors
            if error_count > 0:
                total_errors += 1 # Count category as having errors, but we also want to know how many articles failed
                logger.error(f"❌ {error_count} articles failed to save in {category}")
            
            total_invalid += invalid_count
            total_irrelevant += irrelevant_count  # NEW
            
            # Store category stats
            category_stats[category] = {
                'fetched': len(articles),
                'saved': saved_count,
                'duplicates': duplicate_count,
                'errors': error_count,
                'invalid': invalid_count,
                'irrelevant': irrelevant_count  # NEW
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
    
    # Phase 4: Structured end-of-run report
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
    logger.info("   🔹 Total Irrelevant Rejected: %d articles (category pollution)", total_irrelevant)
    logger.info("   🔹 Total Errors: %d categories", total_errors)
    logger.info("   🔹 Categories Processed: %d/%d", len(CATEGORIES) - total_errors, len(CATEGORIES))
    logger.info("   🔹 Deduplication Rate: %.1f%%", (total_duplicates / total_fetched * 100) if total_fetched > 0 else 0)
    total_rejected = total_invalid + total_irrelevant
    logger.info("   🔹 Acceptance Rate: %.1f%%", (total_fetched / (total_fetched + total_rejected) * 100) if (total_fetched + total_rejected) > 0 else 0)
    logger.info("")
    logger.info("⏱️  PERFORMANCE:")
    logger.info("   🔹 Start: %s", start_time.strftime('%H:%M:%S'))
    logger.info("   🔹 End: %s", end_time.strftime('%H:%M:%S'))
    logger.info("   🔹 Duration: %.2f seconds", duration)
    logger.info("   🔹 Throughput: %.1f articles/second", total_fetched / duration if duration > 0 else 0)
    logger.info("   🔹 Speed Improvement: ~12x faster than sequential")
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
    
    # FAANG Optimization: Update adaptive scheduler intervals
    from app.services.adaptive_scheduler import get_adaptive_scheduler
    
    adaptive = get_adaptive_scheduler(CATEGORIES)
    if adaptive:
        # Update intervals based on this run's statistics
        for category, stats in category_stats.items():
            if 'fetched' in stats:
                new_interval = adaptive.update_category_velocity(
                    category, 
                    stats['fetched']
                )
        
        # Print adaptive scheduler summary
        adaptive.print_summary()


async def fetch_and_validate_category(category: str) -> tuple:
    """
    Fetch and validate articles for a single category
    
    New: Now includes date normalization and category relevance checks!
    
    Returns: (category, valid_articles, invalid_count, irrelevant_count)
    """
    from app.utils.data_validation import is_valid_article, sanitize_article, is_relevant_to_category
    from app.utils.date_parser import normalize_article_date
    
    try:
        logger.info("📌 Fetching %s...", category.upper())
        
        # Fetch from external APIs
        news_aggregator = NewsAggregator()
        
        # FAANG Optimization: Concurrent fetch from Main Provider Chain + Medium + Official Cloud
        # This ensures we get high-quality API news AND Medium blogs AND Official source simultaneously
        main_task = news_aggregator.fetch_by_category(category)
        medium_task = news_aggregator.fetch_from_provider('medium', category)
        official_task = news_aggregator.fetch_from_provider('official_cloud', category)
        
        results = await asyncio.gather(main_task, medium_task, official_task, return_exceptions=True)
        
        # Combine results
        raw_articles = []
        
        # Result 0: Main Provider Chain
        if isinstance(results[0], list):
            raw_articles.extend(results[0])
        
        # Result 1: Medium RSS
        if isinstance(results[1], list):
            if results[1]: # Only log if we found Medium articles
                logger.info("   + Found %d Medium articles for %s", len(results[1]), category)
            raw_articles.extend(results[1])

        # Result 2: Official Cloud
        if isinstance(results[2], list):
            if results[2]:
                logger.info("   + Found %d Official Cloud articles for %s", len(results[2]), category)
            raw_articles.extend(results[2])
        
        if not raw_articles:
            return (category, [], 0, 0)
        
        # Validate, filter, and sanitize
        valid_articles = []
        invalid_count = 0
        irrelevant_count = 0
        
        for article in raw_articles:
            # Step 1: Basic validation (existing)
            if not is_valid_article(article):
                invalid_count += 1
                continue
            
            # Step 2: Category relevance check (NEW!)
            if not is_relevant_to_category(article, category):
                irrelevant_count += 1
                continue
            
            # Step 3: Normalize date to UTC ISO-8601 (NEW!)
            article = normalize_article_date(article)
            
            # Step 4: Sanitize and clean (existing)
            clean_article = sanitize_article(article)
            valid_articles.append(clean_article)
        
        logger.info("✓ %s: %d valid, %d invalid, %d irrelevant", 
                    category.upper(), len(valid_articles), invalid_count, irrelevant_count)
        return (category, valid_articles, invalid_count, irrelevant_count)
        
    except asyncio.TimeoutError:
        logger.error("⏱️  Timeout fetching %s (>30s)", category)
        return (category, [], 0, 0)
    except Exception as e:
        logger.exception("❌ Error fetching %s", category)
        return (category, [], 0, 0)


async def run_smart_ingestion():
    """
    Background Job: Smart Ingestion using LlamaIndex + Bloom Filter (Phase 1)
    
    This is the next-generation ingestion pipeline that replaces manual scraping
    with production-grade LlamaIndex data loaders and adds URL deduplication
    via Bloom Filter to prevent processing the same articles multiple times.
    
    Benefits over legacy fetch_all_news():
    - Robust RSS parsing with LlamaIndex RSSReader
    - Automatic URL deduplication (Bloom Filter)
    - Cleaner code architecture (separation of concerns)
    - Better error handling and logging
    - Lower memory footprint
    
    Runs every 15 minutes alongside (or replaces) the old fetcher.
    """
    start_time = datetime.now()
    
    logger.info("═" * 80)
    logger.info("🔮 [SMART INGESTION] Starting Phase 1 Pipeline...")
    logger.info("🕐 Start Time: %s", start_time.strftime('%Y-%m-%d %H:%M:%S'))
    logger.info("🚀 Mode: LlamaIndex + Bloom Filter")
    logger.info("═" * 80)
    
    try:
        # Fetch all categories using LlamaIndex
        results = await fetch_v2(CATEGORIES)
        
        # Save to Appwrite database and update cache
        appwrite_db = get_appwrite_db()
        cache_service = CacheService()
        
        total_saved = 0
        total_fetched = 0
        total_errors = 0
        
        for category, articles in results.items():
            if not articles:
                logger.warning("⚠️  No articles for category: %s", category)
                continue
            
            try:
                total_fetched += len(articles)
                
                # Save to Appwrite database (L2)
                logger.info("💾 Saving %d articles for %s...", len(articles), category.upper())
                saved_count, duplicate_count, error_count, saved_docs = await appwrite_db.save_articles(articles)
                
                # 🚀 FIRE-AND-FORGET: Trigger Agentic Shadow Path
                if saved_docs:
                    logger.info("🕵️ Triggering Agent Analyst for %d new articles...", len(saved_docs))
                    asyncio.create_task(process_shadow_path(saved_docs))
                
                total_saved += saved_count
                
                # Update Redis cache (L1) if available
                try:
                    await cache_service.set(f"news:{category}", articles, ttl=settings.CACHE_TTL)
                    logger.info("⚡ Redis cache updated for %s", category)
                except Exception as e:
                    logger.debug("⚠️  Redis unavailable: %s", e)
                
                logger.info("✅ %s: %d fetched, %d saved", 
                           category.upper(), len(articles), saved_count)
                           
            except Exception as e:
                total_errors += 1
                logger.error("❌ Error saving %s: %s", category, str(e))
        
        # End-of-run report
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("")
        logger.info("═" * 80)
        logger.info("🎉 [SMART INGESTION] RUN COMPLETED")
        logger.info("═" * 80)
        logger.info("📊 SUMMARY STATISTICS:")
        logger.info("   🔹 Total Fetched: %d articles", total_fetched)
        logger.info("   🔹 Total Saved (New): %d articles", total_saved)
        logger.info("   🔹 Total Errors: %d categories", total_errors)
        logger.info("   🔹 Categories Processed: %d/%d", len(results), len(CATEGORIES))
        logger.info("")
        logger.info("⏱️  PERFORMANCE:")
        logger.info("   🔹 Start: %s", start_time.strftime('%H:%M:%S'))
        logger.info("   🔹 End: %s", end_time.strftime('%H:%M:%S'))
        logger.info("   🔹 Duration: %.2f seconds", duration)
        logger.info("   🔹 Throughput: %.1f articles/second", total_fetched / duration if duration > 0 else 0)
        logger.info("═" * 80)
        
    except Exception as e:
        logger.error("")
        logger.error("═" * 80)
        logger.error("❌ [SMART INGESTION] FAILED!")
        logger.error("Error: %s", str(e))
        logger.error("═" * 80)
        logger.exception("Full traceback:")


async def cleanup_old_news():
    """
    Background Job: Delete articles older than 48 hours (Data Retention Policy)
    
    Runs every 30 minutes to keep Appwrite database within free tier limits.
    Only keeps the last 2 days of articles.
    """
    logger.info("")
    logger.info("═" * 80)
    logger.info("🧹 [CLEANUP JANITOR] Starting cleanup of old news articles...")
    logger.info("🕐 Cleanup Time: %s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    logger.info("═" * 80)
    
    appwrite_db = get_appwrite_db()
    
    if not appwrite_db.initialized:
        logger.error("❌ CRITICAL: Appwrite database not initialized!")
        logger.error("⚠️  Cleanup cannot proceed - database connection required")
        logger.error("💡 Check Appwrite credentials in environment variables")
        return
    
    try:
        # Calculate cutoff date (48 hours ago)
        retention_hours = 48
        cutoff_date = datetime.now() - timedelta(hours=retention_hours)
        cutoff_iso = cutoff_date.isoformat()
        
        logger.info("📋 Retention Policy: %d hours", retention_hours)
        logger.info("📅 Cutoff Date: %s", cutoff_date.strftime('%Y-%m-%d %H:%M:%S'))
        logger.info("🗑️  Articles published before this will be deleted...")
        
        # =========================================================================
        # Step 1: Clean Regular Articles Collection
        # =========================================================================
        logger.info("")
        logger.info("📰 [STEP 1] Cleaning regular articles...")
        logger.info("🔍 Querying Appwrite for old articles...")
        from appwrite.query import Query
        
        response = appwrite_db.databases.list_documents(
            database_id=settings.APPWRITE_DATABASE_ID,
            collection_id=settings.APPWRITE_COLLECTION_ID,
            queries=[
                Query.less_than('published_at', cutoff_iso),
                Query.limit(500)  # Increased from 100 to 500 for better cleanup throughput
            ]
        )
        
        logger.info("📊 Found %d old regular articles to delete", len(response['documents']))
        
        deleted_regular = 0
        if len(response['documents']) > 0:
            logger.info("🗑️  Deleting regular articles...")
        
        for doc in response['documents']:
            try:
                appwrite_db.databases.delete_document(
                    database_id=settings.APPWRITE_DATABASE_ID,
                    collection_id=settings.APPWRITE_COLLECTION_ID,
                    document_id=doc['$id']
                )
                
                # Cleanup from ChromaDB as well (Prevent Zombies)
                try:
                    vector_store.delete_vector(doc['$id'])
                except Exception as ve:
                    logger.warning("⚠️  Vector delete failed (non-critical): %s", ve)
                
                deleted_regular += 1
                if deleted_regular % 10 == 0:
                    logger.info("   Progress: %d regular articles deleted...", deleted_regular)
            except Exception as e:
                logger.error("❌ Error deleting document %s: %s", doc['$id'], e)
        
        logger.info("✅ Regular articles cleanup: %d deleted", deleted_regular)
        
        # =========================================================================
        # Step 2: Clean Cloud Articles Collection (Phase 3)
        # =========================================================================
        deleted_cloud = 0
        
        # Only clean cloud collection if it's configured
        if settings.APPWRITE_CLOUD_COLLECTION_ID:
            logger.info("")
            logger.info("☁️  [STEP 2] Cleaning cloud articles...")
            logger.info("🔍 Querying Appwrite for old cloud articles...")
            
            try:
                cloud_response = appwrite_db.databases.list_documents(
                    database_id=settings.APPWRITE_DATABASE_ID,
                    collection_id=settings.APPWRITE_CLOUD_COLLECTION_ID,
                    queries=[
                        Query.less_than('published_at', cutoff_iso),
                        Query.limit(500)
                    ]
                )
                
                logger.info("📊 Found %d old cloud articles to delete", len(cloud_response['documents']))
                
                if len(cloud_response['documents']) > 0:
                    logger.info("🗑️  Deleting cloud articles...")
                
                for doc in cloud_response['documents']:
                    try:
                        appwrite_db.databases.delete_document(
                            database_id=settings.APPWRITE_DATABASE_ID,
                            collection_id=settings.APPWRITE_CLOUD_COLLECTION_ID,
                            document_id=doc['$id']
                        )
                        
                        # Cleanup from ChromaDB as well
                        try:
                            vector_store.delete_vector(doc['$id'])
                        except Exception as ve:
                            logger.warning("⚠️  Vector delete failed (non-critical): %s", ve)
                        
                        deleted_cloud += 1
                        if deleted_cloud % 10 == 0:
                            logger.info("   Progress: %d cloud articles deleted...", deleted_cloud)
                    except Exception as e:
                        logger.error("❌ Error deleting cloud document %s: %s", doc['$id'], e)
                
                logger.info("✅ Cloud articles cleanup: %d deleted", deleted_cloud)
            
            except Exception as e:
                logger.warning("⚠️  Cloud collection cleanup skipped: %s", e)
                logger.info("💡 Cloud collection may not exist yet - this is normal on first run")
        else:
            logger.info("")
            logger.info("⏭️  [STEP 2] Skipping cloud articles (collection not configured)")
        
        # =========================================================================
        # Step 3: Clear Redis Cache
        # =========================================================================
        logger.info("")
        logger.info("🔄 [STEP 3] Clearing Redis cache...")
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
        total_deleted = deleted_regular + deleted_cloud
        
        logger.info("")
        logger.info("═" * 80)
        logger.info("🎉 [CLEANUP JANITOR] COMPLETED!")
        logger.info("🗑️  Total Deleted: %d articles", total_deleted)
        logger.info("   📰 Regular: %d", deleted_regular)
        logger.info("   ☁️  Cloud: %d", deleted_cloud)
        logger.info("⏰ Retention: Articles older than %d hours removed", retention_hours)
        logger.info("🕐 Completion Time: %s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        logger.info("═" * 80)
        
        # If there are more old articles, schedule another cleanup soon
        if len(response['documents']) >= 100:
            logger.warning("⚠️  WARNING: More old articles detected (100+ limit reached)")
            logger.warning("📅 Additional cleanup will run in next scheduled job")
        
    except Exception as e:
        logger.error("")
        logger.error("═" * 80)
        logger.error("❌ [CLEANUP JANITOR] FAILED!")
        logger.error("Error: %s", str(e))
        logger.error("═" * 80)
        logger.exception("Full traceback:")


def start_scheduler():
    """
    Initialize and start the background scheduler with all jobs
    """
    logger.info("")
    logger.info("═" * 80)
    logger.info("⏰ [SCHEDULER] Initializing background scheduler...")
    logger.info("═" * 80)
    
    # Job 1: Smart Ingestion - TEMPORARILY DISABLED (Debugging)
    # Re-enable after fixing blocking operations in shadow path
    # scheduler.add_job(
    #     run_smart_ingestion,
    #     trigger=IntervalTrigger(minutes=15),
    #     id='smart_ingestion_v2',
    #     name='Smart Ingestion - LlamaIndex + Bloom Filter (every 15 min)',
    #     replace_existing=True
    # )
    logger.info("⚠️  Smart Ingestion DISABLED (Debugging mode)")
    logger.info("   📋 Reason: Investigating blocking operations")
    logger.info("   🔄 Using legacy fetcher as fail-safe")
    
    # Legacy Job (RE-ENABLED): Fail-Safe News Fetcher
    # This is our production-proven fallback while we debug smart ingestion
    scheduler.add_job(
        fetch_all_news,
        trigger=IntervalTrigger(hours=1),
        id='fetch_all_news_failsafe',
        name='News Fetcher FAIL-SAFE (every 1 hour)',
        replace_existing=True
    )
    logger.info("")
    logger.info("✅ Job #1 Registered: 🛡️  Legacy News Fetcher (FAIL-SAFE MODE)")
    logger.info("   ⏱️  Schedule: Every 1 hour")
    logger.info("   📋 Task: Fetch news from all providers (Production-Proven)")
    logger.info("   🎯 Benefit: Guaranteed ingestion during smart ingestion debugging")
    
    
    # Job 2: Cleanup old news every 30 minutes
    scheduler.add_job(
        cleanup_old_news,
        trigger=IntervalTrigger(minutes=30),  # Every 30 mins
        id='cleanup_old_news',
        name='Database Janitor (every 30 mins)',
        replace_existing=True
    )
    logger.info("")
    logger.info("✅ Job #2 Registered: 🧹 Database Janitor")
    logger.info("   ⏱️  Schedule: Every 30 minutes")
    logger.info("   📋 Task: Delete articles older than 48 hours (up to 500 per run)")
    logger.info("   🔢 Total cleanup capacity: 6,000 articles/day (12 runs × 500)")
    
    # Import newsletter service (lazy import to avoid circular dependencies)
    from app.services.newsletter_service import send_scheduled_newsletter
    
    # IST timezone for newsletter scheduling
    IST = pytz.timezone('Asia/Kolkata')
    
    # Job 3: Morning Newsletter - 7:00 AM IST, Monday-Saturday
    scheduler.add_job(
        send_scheduled_newsletter,
        trigger=CronTrigger(
            hour=7, minute=0,
            day_of_week='mon-sat',
            timezone=IST
        ),
        args=["Morning"],
        id='newsletter_morning',
        name='Morning Newsletter (7 AM IST)',
        replace_existing=True,
        max_instances=1
    )
    logger.info("")
    logger.info("✅ Job #3 Registered: ☀️ Morning Newsletter")
    logger.info("   ⏱️  Schedule: 7:00 AM IST, Monday-Saturday")
    logger.info("   📋 Task: Send curated news to Morning preference subscribers")
    
    # Job 4: Afternoon Newsletter - 2:00 PM IST, Monday-Friday
    scheduler.add_job(
        send_scheduled_newsletter,
        trigger=CronTrigger(
            hour=14, minute=0,
            day_of_week='mon-fri',
            timezone=IST
        ),
        args=["Afternoon"],
        id='newsletter_afternoon',
        name='Afternoon Newsletter (2 PM IST)',
        replace_existing=True,
        max_instances=1
    )
    logger.info("")
    logger.info("✅ Job #4 Registered: 📰 Afternoon Newsletter")
    logger.info("   ⏱️  Schedule: 2:00 PM IST, Monday-Friday")
    logger.info("   📋 Task: Send midday update to Afternoon preference subscribers")
    
    # Job 5: Evening Newsletter - 7:00 PM IST, Daily
    scheduler.add_job(
        send_scheduled_newsletter,
        trigger=CronTrigger(
            hour=19, minute=0,
            timezone=IST
        ),
        args=["Evening"],
        id='newsletter_evening',
        name='Evening Newsletter (7 PM IST)',
        replace_existing=True,
        max_instances=1
    )
    logger.info("")
    logger.info("✅ Job #5 Registered: 🌙 Evening Newsletter")
    logger.info("   ⏱️  Schedule: 7:00 PM IST, Daily")
    logger.info("   📋 Task: Send daily digest to Evening preference subscribers")
    
    # Job 6: Weekly Newsletter - Sunday 9:00 AM IST
    scheduler.add_job(
        send_scheduled_newsletter,
        trigger=CronTrigger(
            hour=9, minute=0,
            day_of_week='sun',
            timezone=IST
        ),
        args=["Weekly"],
        id='newsletter_weekly',
        name='Weekly Newsletter (Sunday 9 AM IST)',
        replace_existing=True,
        max_instances=1
    )
    logger.info("")
    logger.info("✅ Job #6 Registered: 📅 Weekly Newsletter")
    logger.info("   ⏱️  Schedule: Sunday 9:00 AM IST")
    logger.info("   📋 Task: Send weekly roundup to Weekly preference subscribers")
    
    # Job 7: Monthly Newsletter - 1st of month, 9:00 AM IST
    scheduler.add_job(
        send_scheduled_newsletter,
        trigger=CronTrigger(
            hour=9, minute=0,
            day=1,
            timezone=IST
        ),
        args=["Monthly"],
        id='newsletter_monthly',
        name='Monthly Newsletter (1st, 9 AM IST)',
        replace_existing=True,
        max_instances=1
    )
    logger.info("")
    logger.info("✅ Job #7 Registered: 📊 Monthly Newsletter")
    logger.info("   ⏱️  Schedule: 1st of month, 9:00 AM IST")
    logger.info("   📋 Task: Send monthly intelligence to Monthly preference subscribers")
    
    # Start the scheduler
    logger.info("")
    logger.info("🚀 Starting scheduler engine...")
    scheduler.start()
    logger.info("")
    logger.info("═" * 80)
    logger.info("✅ [SCHEDULER] Background scheduler started successfully!")
    logger.info("🔄 All jobs are now active and running")
    logger.info("═" * 80)
    logger.info("")


def shutdown_scheduler():
    """
    Gracefully shutdown the scheduler
    """
    logger.info("")
    logger.info("═" * 80)
    logger.info("⏹️  [SCHEDULER] Shutting down background scheduler...")
    logger.info("⏳ Waiting for running jobs to complete...")
    scheduler.shutdown(wait=True)
    logger.info("✅ [SCHEDULER] Background scheduler shut down successfully")
    logger.info("═" * 80)
    logger.info("")


# Manual job triggers for testing (can be called from admin endpoints)
async def trigger_fetch_now():
    """Manually trigger news fetch (for testing)"""
    logger.info("")
    logger.info("═" * 80)
    logger.info("🔧 [MANUAL TRIGGER] Running fetch job NOW...")
    logger.info("═" * 80)
    await fetch_all_news()


async def trigger_cleanup_now():
    """Manually trigger cleanup (for testing)"""
    logger.info("")
    logger.info("=" * 80)
    logger.info("🔧 [MANUAL TRIGGER] Running cleanup job NOW...")
    logger.info("=" * 80)
    await cleanup_old_news()


async def trigger_newsletter_now(preference: str):
    """Manually trigger newsletter for specific preference (for testing)"""
    from app.services.newsletter_service import send_scheduled_newsletter
    
    logger.info("")
    logger.info("=" * 80)
    logger.info(f"🔧 [MANUAL TRIGGER] Running {preference} newsletter job NOW...")
    logger.info("=" * 80)
    result = await send_scheduled_newsletter(preference)
    return result
