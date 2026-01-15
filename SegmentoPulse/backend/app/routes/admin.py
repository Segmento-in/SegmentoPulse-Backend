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
