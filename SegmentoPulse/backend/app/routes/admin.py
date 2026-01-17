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
    "magazines"
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
