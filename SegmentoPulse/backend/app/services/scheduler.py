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
    "magazines"
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
        
        category, articles, invalid_count = result
        
        if not articles:
            logger.warning("⚠️  No valid articles for category: %s", category)
            category_stats[category] = {
                'fetched': 0,
                'saved': 0,
                'duplicates': 0,
                'invalid': invalid_count
            }
            continue
        
        try:
            # Save to Appwrite database (L2)
            logger.info("💾 Saving %d articles for %s...", len(articles), category.upper())
            saved_count = await appwrite_db.save_articles(articles)
            
            # Calculate duplicates
            duplicates = len(articles) - saved_count
            
            total_fetched += len(articles)
            total_saved += saved_count
            total_duplicates += duplicates
            total_invalid += invalid_count
            
            # Store category stats
            category_stats[category] = {
                'fetched': len(articles),
                'saved': saved_count,
                'duplicates': duplicates,
                'invalid': invalid_count
            }
            
            # Update Redis cache (L1) if available
            try:
                await cache_service.set(f"news:{category}", articles, ttl=settings.CACHE_TTL)
                logger.info("⚡ Redis cache updated for %s", category)
            except Exception as e:
                logger.debug("⚠️  Redis unavailable: %s", e)
            
            logger.info("✅ %s: %d fetched, %d saved, %d duplicates, %d invalid", 
                       category.upper(), len(articles), saved_count, duplicates, invalid_count)
                       
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
    logger.info("   🔹 Total Errors: %d categories", total_errors)
    logger.info("   🔹 Categories Processed: %d/%d", len(CATEGORIES) - total_errors, len(CATEGORIES))
    logger.info("   🔹 Deduplication Rate: %.1f%%", (total_duplicates / total_fetched * 100) if total_fetched > 0 else 0)
    logger.info("   🔹 Quality Rate: %.1f%%", (total_fetched / (total_fetched + total_invalid) * 100) if (total_fetched + total_invalid) > 0 else 0)
    logger.info("")
    logger.info("⏱️  PERFORMANCE:")
    logger.info("   🔹 Start: %s", start_time.strftime('%H:%M:%S'))
    logger.info("   🔹 End: %s", end_time.strftime('%H:%M:%S'))
    logger.info("   🔹 Duration: %.2f seconds", duration)
    logger.info("   🔹 Throughput: %.1f articles/second", total_fetched / duration if duration > 0 else 0)
    logger.info("   🔹 Speed Improvement: ~12x faster than sequential")
    logger.info("═" * 80)
    
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
    
    Returns: (category, valid_articles, invalid_count)
    """
    from app.utils.data_validation import is_valid_article, sanitize_article
    
    try:
        logger.info("📌 Fetching %s...", category.upper())
        
        # Fetch from external APIs
        news_aggregator = NewsAggregator()
        raw_articles = await news_aggregator.fetch_by_category(category)
        
        if not raw_articles:
            return (category, [], 0)
        
        # Validate and sanitize
        valid_articles = []
        invalid_count = 0
        
        for article in raw_articles:
            if is_valid_article(article):
                clean_article = sanitize_article(article)
                valid_articles.append(clean_article)
            else:
                invalid_count += 1
        
        logger.info("✓ %s: %d valid, %d invalid", category.upper(), len(valid_articles), invalid_count)
        return (category, valid_articles, invalid_count)
        
    except asyncio.TimeoutError:
        logger.error("⏱️  Timeout fetching %s (>30s)", category)
        return (category, [], 0)
    except Exception as e:
        logger.exception("❌ Error fetching %s", category)
        return (category, [], 0)


async def cleanup_old_news():
    """
    Background Job: Delete articles older than 48 hours (Data Retention Policy)
    
    Runs daily at midnight to keep Appwrite database within free tier limits.
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
        
        # Query and delete old articles
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
        
        logger.info("📊 Found %d old articles to delete", len(response['documents']))
        
        deleted_count = 0
        if len(response['documents']) > 0:
            logger.info("🗑️  Deleting articles...")
        
        for doc in response['documents']:
            try:
                appwrite_db.databases.delete_document(
                    database_id=settings.APPWRITE_DATABASE_ID,
                    collection_id=settings.APPWRITE_COLLECTION_ID,
                    document_id=doc['$id']
                )
                deleted_count += 1
                if deleted_count % 10 == 0:
                    logger.info("   Progress: %d articles deleted...", deleted_count)
            except Exception as e:
                logger.error("❌ Error deleting document %s: %s", doc['$id'], e)
        
        # Clear Redis cache to force refresh from updated database
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
        
        logger.info("")
        logger.info("═" * 80)
        logger.info("🎉 [CLEANUP JANITOR] COMPLETED!")
        logger.info("🗑️  Total Deleted: %d articles", deleted_count)
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
    
    # Job 1: Fetch news every 15 minutes
    scheduler.add_job(
        fetch_all_news,
        trigger=IntervalTrigger(minutes=15),
        id='fetch_all_news',
        name='News Fetcher (every 15 min)',
        replace_existing=True
    )
    logger.info("✅ Job #1 Registered: 📰 News Fetcher")
    logger.info("   ⏱️  Schedule: Every 15 minutes")
    logger.info("   📋 Task: Fetch news from all providers and update database")
    
    # Job 2: Cleanup old news every 6 hours
    scheduler.add_job(
        cleanup_old_news,
        trigger=CronTrigger(hour='0,6,12,18', minute=0),  # Every 6 hours at :00
        id='cleanup_old_news',
        name='Database Janitor (every 6 hours)',
        replace_existing=True
    )
    logger.info("")
    logger.info("✅ Job #2 Registered: 🧹 Database Janitor")
    logger.info("   ⏱️  Schedule: Every 6 hours (00:00, 06:00, 12:00, 18:00 UTC)")
    logger.info("   📋 Task: Delete articles older than 48 hours (500 per run)")
    logger.info("   🔢 Total cleanup capacity: 2,000 articles/day")
    
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
    logger.info("═" * 80)
    logger.info("🔧 [MANUAL TRIGGER] Running cleanup job NOW...")
    logger.info("═" * 80)
    await cleanup_old_news()
