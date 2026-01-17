"""
Background Scheduler Service - Phase 3
Automates news fetching and database cleanup using APScheduler
"""

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timedelta
import logging

from app.services.news_aggregator import NewsAggregator
from app.services.appwrite_db import get_appwrite_db
from app.services.cache_service import CacheService
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
    "business-intelligence",
    "business-analytics",
    "customer-data-platform",
    "data-centers",
    "cloud-computing",
    "magazines"
]


async def fetch_all_news():
    """
    Background Job: Fetch news for all categories and update Appwrite database
    
    Runs every 15 minutes to keep database fresh with latest articles.
    This ensures users always get fast responses from L2 cache (Appwrite).
    """
    logger.info("🔄 [Background Fetcher] Starting news fetch for all categories...")
    
    news_aggregator = NewsAggregator()
    appwrite_db = get_appwrite_db()
    cache_service = CacheService()
    
    total_fetched = 0
    total_saved = 0
    
    for category in CATEGORIES:
        try:
            logger.info(f"  Fetching {category}...")
            
            # Fetch from external APIs
            articles = await news_aggregator.fetch_by_category(category)
            
            if articles:
                # Save to Appwrite database (L2)
                saved_count = await appwrite_db.save_articles(articles)
                total_fetched += len(articles)
                total_saved += saved_count
                
                # Update Redis cache (L1) if available
                try:
                    await cache_service.set(f"news:{category}", articles, ttl=settings.CACHE_TTL)
                except Exception as e:
                    logger.debug(f"  Redis cache update skipped for {category}: {e}")
                
                logger.info(f"  ✓ {category}: {len(articles)} fetched, {saved_count} saved")
            else:
                logger.warning(f"  ✗ {category}: No articles available")
            
        except Exception as e:
            logger.error(f"  ✗ {category}: Error - {e}")
            continue
    
    logger.info(f"✅ [Background Fetcher] Complete! {total_fetched} articles fetched, {total_saved} new articles saved")


async def cleanup_old_news():
    """
    Background Job: Delete articles older than 48 hours (Data Retention Policy)
    
    Runs daily at midnight to keep Appwrite database within free tier limits.
    Only keeps the last 2 days of articles.
    """
    logger.info("🧹 [Janitor] Starting cleanup of old news articles...")
    
    appwrite_db = get_appwrite_db()
    
    if not appwrite_db.initialized:
        logger.warning("  Appwrite not initialized - skipping cleanup")
        return
    
    try:
        # Calculate cutoff date (48 hours ago)
        retention_hours = 48
        cutoff_date = datetime.now() - timedelta(hours=retention_hours)
        cutoff_iso = cutoff_date.isoformat()
        
        logger.info(f"  Retention policy: {retention_hours} hours")
        logger.info(f"  Cutoff date: {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Query and delete old articles
        from appwrite.query import Query
        
        response = appwrite_db.databases.list_documents(
            database_id=settings.APPWRITE_DATABASE_ID,
            collection_id=settings.APPWRITE_COLLECTION_ID,
            queries=[
                Query.less_than('published_at', cutoff_iso),
                Query.limit(100)  # Delete in batches of 100
            ]
        )
        
        deleted_count = 0
        for doc in response['documents']:
            try:
                appwrite_db.databases.delete_document(
                    database_id=settings.APPWRITE_DATABASE_ID,
                    collection_id=settings.APPWRITE_COLLECTION_ID,
                    document_id=doc['$id']
                )
                deleted_count += 1
            except Exception as e:
                logger.error(f"  Error deleting document {doc['$id']}: {e}")
        
        # Clear Redis cache to force refresh from updated database
        cache_service = CacheService()
        for category in CATEGORIES:
            try:
                await cache_service.delete(f"news:{category}")
            except Exception as e:
                logger.debug(f"  Cache clear skipped for {category}: {e}")
        
        logger.info(f"✅ [Janitor] Complete! Deleted {deleted_count} articles older than {retention_hours} hours")
        
        # If there are more old articles, schedule another cleanup soon
        if len(response['documents']) >= 100:
            logger.info(f"  More old articles detected - will clean up again in next run")
        
    except Exception as e:
        logger.error(f"✗ [Janitor] Cleanup failed: {e}")


def start_scheduler():
    """
    Initialize and start the background scheduler with all jobs
    """
    logger.info("⏰ Starting background scheduler...")
    
    # Job 1: Fetch news every 15 minutes
    scheduler.add_job(
        fetch_all_news,
        trigger=IntervalTrigger(minutes=15),
        id='fetch_all_news',
        name='News Fetcher (every 15 min)',
        replace_existing=True
    )
    logger.info("  ✓ Registered: News Fetcher (every 15 minutes)")
    
    # Job 2: Cleanup old news daily at midnight (00:00)
    scheduler.add_job(
        cleanup_old_news,
        trigger=CronTrigger(hour=0, minute=0),  # Daily at 00:00 UTC
        id='cleanup_old_news',
        name='Database Janitor (daily at midnight)',
        replace_existing=True
    )
    logger.info("  ✓ Registered: Database Janitor (daily at 00:00 UTC)")
    
    # Start the scheduler
    scheduler.start()
    logger.info("✅ Background scheduler started successfully!")


def shutdown_scheduler():
    """
    Gracefully shutdown the scheduler
    """
    logger.info("⏹️  Shutting down background scheduler...")
    scheduler.shutdown(wait=True)
    logger.info("✅ Background scheduler shut down successfully")


# Manual job triggers for testing (can be called from admin endpoints)
async def trigger_fetch_now():
    """Manually trigger news fetch (for testing)"""
    logger.info("🔧 [Manual Trigger] Running fetch job now...")
    await fetch_all_news()


async def trigger_cleanup_now():
    """Manually trigger cleanup (for testing)"""
    logger.info("🔧 [Manual Trigger] Running cleanup job now...")
    await cleanup_old_news()
