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
    
    # Parallel fetch all categories at once
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
    
    Returns: (category, valid_articles, invalid_count, irrelevant_count)
    """
    from app.utils.data_validation import is_valid_article, sanitize_article, is_relevant_to_category
    from app.utils.date_parser import normalize_article_date
    
    try:
        logger.info("📌 Fetching %s...", category.upper())
        
        # Fetch from external APIs
        news_aggregator = NewsAggregator()
        
        # Concurrent fetch from Main Chain + Medium + Official Cloud
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
            if results[1]:
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
            # Step 1: Basic validation
            if not is_valid_article(article):
                invalid_count += 1
                continue
            
            # Step 2: Category relevance check
            if not is_relevant_to_category(article, category):
                irrelevant_count += 1
                continue
            
            # Step 3: Normalize date to UTC ISO-8601
            article = normalize_article_date(article)
            
            # Step 4: Sanitize and clean
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
                check_response = appwrite_db.tablesDB.list_rows(
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
                    response = appwrite_db.tablesDB.list_rows(
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
                            appwrite_db.tablesDB.delete_row(
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


def start_scheduler():
    """
    Initialize and start the background scheduler with all jobs
    """
    logger.info("")
    logger.info("═" * 80)
    logger.info("⏰ [SCHEDULER] Initializing background scheduler...")
    logger.info("═" * 80)
    
    # News Fetcher Job (Frequency: Every 1 hour)
    scheduler.add_job(
        fetch_all_news,
        trigger=IntervalTrigger(hours=1),
        id='fetch_all_news',
        name='News Fetcher (every 1 hour)',
        replace_existing=True
    )
    logger.info("")
    logger.info("✅ Job #1 Registered: 📰 News Fetcher")
    logger.info("   ⏱️  Schedule: Every 1 hour")
    logger.info("   📋 Task: Direct Fetch -> Deduplicate -> Store (Appwrite)")
    
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
    await send_scheduled_newsletter(preference)



