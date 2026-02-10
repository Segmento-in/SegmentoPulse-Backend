from fastapi import APIRouter, HTTPException
from typing import Dict, List
import asyncio
from app.services.news_aggregator import NewsAggregator
from app.services.cache_service import CacheService
from app.config import settings

router = APIRouter()

# All supported news categories
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

@router.post("/cache/warm")
async def warm_cache():
    """
    Cache Warming Endpoint - Phase 1 Optimization
    
    Proactively fetches news for all categories and populates Redis cache.
    This eliminates the "cold start" problem by pre-loading data before user requests.
    
    Usage:
        curl -X POST http://localhost:8000/api/admin/cache/warm
    
    Returns:
        - status: Success/failure message
        - categories_warmed: Number of categories successfully cached
        - categories_failed: Number of categories that failed
        - details: Per-category results with article counts
    """
    news_aggregator = NewsAggregator()
    cache_service = CacheService()
    
    results = {
        "successful": [],
        "failed": []
    }
    
    for category in CATEGORIES:
        try:
            print(f"[Cache Warming] Fetching {category}...")
            
            # Fetch articles from news aggregator (tries all providers with failover)
            articles = await news_aggregator.fetch_by_category(category)
            
            if articles:
                # Cache the articles with configured TTL (600 seconds)
                await cache_service.set(f"news:{category}", articles, ttl=settings.CACHE_TTL)
                
                results["successful"].append({
                    "category": category,
                    "article_count": len(articles),
                    "cached": True
                })
                print(f"✓ [Cache Warming] {category}: {len(articles)} articles cached")
            else:
                results["failed"].append({
                    "category": category,
                    "error": "No articles returned from providers"
                })
                print(f"✗ [Cache Warming] {category}: No articles available")
            
            # Rate limiting: Wait 1 second between API calls to avoid overwhelming providers
            await asyncio.sleep(1)
            
        except Exception as e:
            results["failed"].append({
                "category": category,
                "error": str(e)
            })
            print(f"✗ [Cache Warming] {category}: Error - {e}")
    
    # Prepare response summary
    categories_warmed = len(results["successful"])
    categories_failed = len(results["failed"])
    
    return {
        "status": "completed",
        "message": f"Cache warming completed: {categories_warmed} successful, {categories_failed} failed",
        "categories_warmed": categories_warmed,
        "categories_failed": categories_failed,
        "total_categories": len(CATEGORIES),
        "cache_ttl": settings.CACHE_TTL,
        "details": results
    }


@router.get("/cache/stats")
async def get_cache_stats():
    """
    Get cache statistics
    
    Returns information about:
        - Which categories are currently cached
        - Cache TTL configuration
        - Provider statistics
    """
    cache_service = CacheService()
    news_aggregator = NewsAggregator()
    
    cached_categories = []
    
    for category in CATEGORIES:
        cached_data = await cache_service.get(f"news:{category}")
        if cached_data:
            cached_categories.append({
                "category": category,
                "article_count": len(cached_data)
            })
    
    # Get provider statistics
    provider_stats = news_aggregator.get_stats()
    
    return {
        "cache_ttl": settings.CACHE_TTL,
        "total_categories": len(CATEGORIES),
        "cached_categories": len(cached_categories),
        "cache_details": cached_categories,
        "provider_stats": provider_stats
    }


@router.post("/cache/clear")
async def clear_cache():
    """
    Clear all cached news data
    
    Useful for testing or forcing a fresh data fetch.
    """
    cache_service = CacheService()
    
    cleared = 0
    for category in CATEGORIES:
        try:
            await cache_service.delete(f"news:{category}")
            cleared += 1
        except Exception as e:
            print(f"Error clearing cache for {category}: {e}")
    
    return {
        "status": "success",
        "message": f"Cleared {cleared} cached categories",
        "categories_cleared": cleared
    }


# ===========================================
# Database Management Endpoints (Phase 2)
# ===========================================

@router.get("/db/stats")
async def get_database_stats():
    """
    Get Appwrite database statistics (Phase 2)
    
    Returns:
        - Total article count
        - Articles per category  
        - Database connection status
        - Collection information
    """
    from app.services.appwrite_db import get_appwrite_db
    
    try:
        appwrite_db = get_appwrite_db()
        stats = await appwrite_db.get_stats()
        
        return {
            "success": True,
            **stats
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


@router.post("/db/cleanup")
async def cleanup_old_articles(days: int = 30):
    """
    Delete articles older than specified days from Appwrite database
    
    Args:
        days: Delete articles older than this many days (default: 30)
    
    Returns:
        Number of articles deleted
    """
    from app.services.appwrite_db import get_appwrite_db
    
    try:
        appwrite_db = get_appwrite_db()
        deleted_count = await appwrite_db.delete_old_articles(days)
        
        return {
            "success": True,
            "message": f"Deleted {deleted_count} articles older than {days} days",
            "deleted_count": deleted_count,
            "days_threshold": days
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


@router.post("/db/populate")
async def populate_database():
    """
    Populate Appwrite database by fetching fresh articles for all categories
    
    This is useful for:
    - Initial database setup
    - Refreshing all categories at once
    - Recovery after database cleanup
    """
    from app.services.appwrite_db import get_appwrite_db
    from app.services.news_aggregator import NewsAggregator
    import asyncio
    
    try:
        appwrite_db = get_appwrite_db()
        news_aggregator = NewsAggregator()
        
        results = {
            "successful": [],
            "failed": []
        }
        
        for category in CATEGORIES:
            try:
                print(f"[DB Populate] Fetching {category}...")
                
                # Fetch articles from external APIs
                articles = await news_aggregator.fetch_by_category(category)
                
                if articles:
                    # Save to Appwrite database
                    saved_count = await appwrite_db.save_articles(articles)
                    
                    results["successful"].append({
                        "category": category,
                        "fetched": len(articles),
                        "saved": saved_count
                    })
                    print(f"✓ [DB Populate] {category}: {saved_count} articles saved")
                else:
                    results["failed"].append({
                        "category": category,
                        "error": "No articles returned from providers"
                    })
                    print(f"✗ [DB Populate] {category}: No articles available")
                
                # Rate limiting: Wait 1 second between API calls
                await asyncio.sleep(1)
                
            except Exception as e:
                results["failed"].append({
                    "category": category,
                    "error": str(e)
                })
                print(f"✗ [DB Populate] {category}: Error - {e}")
        
        categories_populated = len(results["successful"])
        categories_failed = len(results["failed"])
        total_saved = sum(r["saved"] for r in results["successful"])
        
        return {
            "success": True,
            "message": f"Database populated: {categories_populated} categories, {total_saved} articles saved",
            "categories_populated": categories_populated,
            "categories_failed": categories_failed,
            "total_categories": len(CATEGORIES),
            "total_articles_saved": total_saved,
            "details": results
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


# ===========================================
# Background Scheduler Management (Phase 3)
# ===========================================

@router.post("/scheduler/fetch-now")
async def trigger_fetch_job():
    """
    Manually trigger the news fetch job (Phase 3)
    
    Useful for:
    - Testing the fetcher without waiting 15 minutes
    - Forcing an immediate database refresh
    - Verifying background worker functionality
    """
    from app.services.scheduler import trigger_fetch_now
    
    try:
        await trigger_fetch_now()
        
        return {
            "success": True,
            "message": "News fetch job triggered successfully",
            "note": "Check server logs for detailed progress"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


@router.post("/scheduler/cleanup-now")
async def trigger_cleanup_job():
    """
    Manually trigger the cleanup job (Phase 3)
    
    Deletes articles older than 48 hours.
    
    Useful for:
    - Testing the janitor without waiting 24 hours
    - Immediate cleanup of old data
    - Free tier space management
    """
    from app.services.scheduler import trigger_cleanup_now
    
    try:
        await trigger_cleanup_now()
        
        return {
            "success": True,
            "message": "Cleanup job triggered successfully",
            "note": "Check server logs for deletion count"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


@router.get("/scheduler/status")
async def get_scheduler_status():
    """
    Get background scheduler status and job information
    
    Returns:
        - Scheduler state (running/stopped)
        - List of registered jobs with next run times
    """
    from app.services.scheduler import scheduler
    
    try:
        jobs_info = []
        for job in scheduler.get_jobs():
            jobs_info.append({
                "id": job.id,
                "name": job.name,
                "next_run_time": str(job.next_run_time) if job.next_run_time else None,
                "trigger": str(job.trigger)
            })
        
        return {
            "success": True,
            "scheduler_running": scheduler.running,
            "total_jobs": len(jobs_info),
            "jobs": jobs_info
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


# Newsletter Admin Endpoints
@router.post("/newsletter/send-now")
async def send_newsletter_now(preference: str = "Weekly"):
    """
    Manually trigger newsletter for specific preference group
    
    Useful for testing before production deployment or sending ad-hoc newsletters.
    
    Args:
        preference: Newsletter preference (Morning/Afternoon/Evening/Weekly/Monthly)
    
    Returns:
        Send statistics and status
    """
    try:
        from app.services.scheduler import trigger_newsletter_now
        
        # Validate preference
        allowed_preferences = ["Morning", "Afternoon", "Evening", "Weekly", "Monthly"]
        if preference not in allowed_preferences:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid preference. Must be one of: {allowed_preferences}"
            )
        
        # Trigger newsletter
        result = await trigger_newsletter_now(preference)
        
        return {
            "success": True,
            "preference": preference,
            "timestamp": str(asyncio.get_event_loop().time()),
            **result
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to send newsletter: {str(e)}"
        )


@router.get("/subscribers/analytics")
async def get_subscriber_analytics():
    """
    Get subscriber distribution by preference from Appwrite
    
    Shows how many subscribers have chosen each newsletter timing.
    Useful for understanding user preferences and planning content strategy.
    
    Returns:
        Total active subscribers and breakdown by preference
    """
    try:
        from app.services.appwrite_db import get_appwrite_db
        
        appwrite_db = get_appwrite_db()
        
        if not appwrite_db.initialized:
            raise HTTPException(
                status_code=503,
                detail="Appwrite database not available"
            )
        
        all_subscribers = await appwrite_db.get_all_subscribers()
        
        # Calculate preference distribution
        preference_counts = {
            "Morning": 0,
            "Afternoon": 0,
            "Evening": 0,
            "Weekly": 0,
            "Monthly": 0
        }
        
        active_count = 0
        total_count = len(all_subscribers)
        
        for sub in all_subscribers:
            if sub.get('isActive', True):
                active_count += 1
                # Count each preference subscription
                if sub.get('sub_morning', False):
                    preference_counts['Morning'] += 1
                if sub.get('sub_afternoon', False):
                    preference_counts['Afternoon'] += 1
                if sub.get('sub_evening', False):
                    preference_counts['Evening'] += 1
                if sub.get('sub_weekly', False):
                    preference_counts['Weekly'] += 1
                if sub.get('sub_monthly', False):
                    preference_counts['Monthly'] += 1
        
        return {
            "total_subscribers": total_count,
            "active_subscribers": active_count,
            "inactive": total_count - active_count,
            "distribution_by_preference": preference_counts,
            "percentage_distribution": {
                pref: round((count / active_count * 100), 2) if active_count > 0 else 0
                for pref, count in preference_counts.items()
            }
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get analytics: {str(e)}"
        )


@router.get("/newsletter/preview/{preference}")
async def preview_newsletter_content(preference: str):
    """
    Preview newsletter content without sending emails
    
    Useful for testing and debugging content selection logic.
    Shows what articles would be included in the next newsletter.
    
    Args:
        preference: Newsletter preference to preview
    
    Returns:
        Article list and metadata
    """
    try:
        from app.services.newsletter_service import preview_newsletter_content as preview
        
        # Validate preference
        allowed_preferences = ["Morning", "Afternoon", "Evening", "Weekly", "Monthly"]
        if preference not in allowed_preferences:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid preference. Must be one of: {allowed_preferences}"
            )
        
        result = await preview(preference)
        
        return {
            "success": True,
            **result
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to preview content: {str(e)}"
        )


# ===========================================
# Bloom Filter Management (Integration Fix #1)
# ===========================================

@router.post("/bloom-filter/reset")
async def reset_bloom_filter():
    """
    Reset Scalable Bloom Filter - Integration Sync Mechanism
    
    **USE CASE**: After clearing the Appwrite database, call this endpoint
    to reset the Bloom Filter to match the empty database state.
    
    **WHY THIS IS NEEDED**: 
    In production (Hugging Face Spaces), the Bloom Filter persists on disk
    even when the database is cleared via the Appwrite dashboard. This causes
    100% duplicate detection because the filter "remembers" old URLs that no
    longer exist in the database.
    
    **INTEGRATION CONTRACT**:
    - When you clear Appwrite DB → Call this endpoint
    - Filter state syncs with database state
    - Fresh ingestion can proceed
    
    Returns:
        Status and statistics before/after reset
    """
    try:
        from app.services.deduplication import get_url_filter
        
        # Get the global filter instance
        url_filter = get_url_filter()
        
        # Capture stats before reset
        stats_before = url_filter.get_stats()
        
        # Reset the filter
        url_filter.reset()
        
        # Capture stats after reset
        stats_after = url_filter.get_stats()
        
        return {
            "success": True,
            "message": "Scalable Bloom Filter reset successfully",
            "operation": "Integration sync - Filter state now matches empty database",
            "stats_before_reset": {
                "total_checks": stats_before['total_checks'],
                "unique_urls_added": stats_before['unique_urls_added'],
                "duplicates_detected": stats_before['duplicates_detected'],
                "filter_buckets": stats_before['filter_buckets'],
                "estimated_capacity": stats_before['estimated_current_capacity']
            },
            "stats_after_reset": {
                "filter_buckets": stats_after['filter_buckets'],
                "last_reset": stats_after['last_reset']
            },
            "note": "Filter is now ready for fresh ingestion. Next fetch will save all articles."
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to reset Bloom Filter: {str(e)}"
        )


@router.get("/bloom-filter/stats")
async def get_bloom_filter_stats():
    """
    Get Scalable Bloom Filter statistics - Observability Endpoint
    
    Shows:
    - Total URLs processed
    - Duplicate detection rate
    - Filter bucket count (auto-scaling metric)
    - Memory usage estimate
    - Last persistence time
    
    **PRODUCTION DIAGNOSTIC**: Use this to verify filter state
    and detect saturation issues.
    
    Returns:
        Comprehensive filter statistics
    """
    try:
        from app.services.deduplication import get_url_filter
        
        url_filter = get_url_filter()
        stats = url_filter.get_stats()
        
        # Calculate additional metrics
        memory_usage = url_filter.get_estimated_memory_usage()
        
        return {
            "success": True,
            "filter_type": "ScalableBloomFilter (pybloom_live)",
            "persistence_enabled": True,
            "persistence_path": url_filter.persistence_path,
            "statistics": {
                "total_checks": stats['total_checks'],
                "unique_urls_added": stats['unique_urls_added'],
                "duplicates_detected": stats['duplicates_detected'],
                "duplicate_rate_percent": stats['duplicate_rate_percent'],
                "filter_buckets": stats['filter_buckets'],
                "initial_capacity": stats['initial_capacity'],
                "current_estimated_capacity": stats['estimated_current_capacity'],
                "error_rate": stats['filter_error_rate'],
                "is_scalable": stats['is_scalable'],
                "last_reset": stats['last_reset'],
                "last_save": stats['last_save']
            },
            "memory": {
                "estimated_usage": memory_usage,
                "note": "Scalable Bloom Filter auto-grows as needed"
            },
            "health": {
                "status": "healthy" if stats['duplicate_rate_percent'] < 95 else "warning",
                "warning": "100% duplicate rate detected - consider reset" if stats['duplicate_rate_percent'] >= 99.5 else None
            }
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get Bloom Filter stats: {str(e)}"
        )


@router.get("/bloom-filter/health")
async def bloom_filter_health_check():
    """
    Quick health check for Bloom Filter - Production Monitoring
    
    Returns:
    - Status: healthy/warning/critical
    - Reason for any issues
    - Recommended action
    
    **ALERTING**: Use this for automated monitoring.
    A "critical" status means ingestion is likely broken.
    """
    try:
        from app.services.deduplication import get_url_filter
        from app.services.appwrite_db import get_appwrite_db
        import os
        
        url_filter = get_url_filter()
        stats = url_filter.get_stats()
        appwrite_db = get_appwrite_db()
        
        # Check 1: Duplicate rate health
        duplicate_rate = stats['duplicate_rate_percent']
        
        # Check 2: Filter persistence file exists
        filter_file_exists = os.path.exists(url_filter.persistence_path)
        
        # Check 3: Database initialized
        db_initialized = appwrite_db.initialized
        
        # Determine health status
        issues = []
        status = "healthy"
        
        if duplicate_rate >= 99.5:
            status = "critical"
            issues.append({
                "type": "duplicate_saturation",
                "severity": "critical",
                "details": f"Duplicate rate: {duplicate_rate}% (expected < 95%)",
                "action": "Reset Bloom Filter via POST /admin/bloom-filter/reset"
            })
        elif duplicate_rate >= 90:
            status = "warning"
            issues.append({
                "type": "high_duplicates",
                "severity": "warning",
                "details": f"Duplicate rate: {duplicate_rate}% (expected < 90%)",
                "action": "Monitor ingestion logs for validation issues"
            })
        
        if not filter_file_exists:
            issues.append({
                "type": "missing_persistence",
                "severity": "info",
                "details": "Filter persistence file not found (filter will create on first save)",
                "action": "No action needed - this is normal on first run"
            })
        
        if not db_initialized:
            status = "critical"
            issues.append({
                "type": "database_disconnected",
                "severity": "critical",
                "details": "Appwrite database not initialized",
                "action": "Check Appwrite credentials in environment variables"
            })
        
        return {
            "status": status,
            "timestamp": stats['last_reset'],
            "checks_performed": {
                "duplicate_rate": duplicate_rate,
                "filter_persisted": filter_file_exists,
                "database_initialized": db_initialized
            },
            "issues": issues if issues else [],
            "recommendation": issues[0]["action"] if issues else "System healthy - all checks passed"
        }
    
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "recommendation": "Check server logs for detailed error information"
        }

