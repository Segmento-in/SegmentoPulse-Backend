from fastapi import APIRouter, HTTPException
from app.models import NewsResponse, ErrorResponse
from app.services.news_aggregator import NewsAggregator
from app.services.cache_service import CacheService
from app.services.appwrite_db import get_appwrite_db

router = APIRouter()
news_aggregator = NewsAggregator()
cache_service = CacheService()
appwrite_db = get_appwrite_db()

@router.get("/{category}", response_model=NewsResponse)
async def get_news_by_category(
    category: str,
    limit: int = 20,  # ← Pagination: items per page
    page: int = 1     # ← Pagination: page number (1-indexed)
):
    """
    Get news articles by category with multi-layer caching and pagination (Phase 4)
    
    **THE GOLDEN RULE: Users NEVER wait for external APIs**
    - Users only read from database (Appwrite)
    - Background workers populate the database every 15 minutes
    - If database is empty, return empty state (workers will fill it soon)
    
    **Pagination:**
    - limit: Number of articles per page (default: 20, max: 100)
    - page: Page number starting from 1 (default: 1)
    - Example: page=1, limit=20 returns articles 1-20
    - Example: page=2, limit=20 returns articles 21-40
    
    Caching Strategy:
    - L1 Cache: Redis (if available) - 600s TTL, ~5ms response
    - L2 Cache: Appwrite Database - persistent, 10-50ms response  
    - NO L3: External APIs are ONLY called by background workers
    
    Categories:
    - ai: Artificial Intelligence
    - data-security: Data Security
    - data-governance: Data Governance
    - data-privacy: Data Privacy
    - data-engineering: Data Engineering
    - data-management: Data Management
    - business-intelligence: Business Intelligence
    - business-analytics: Business Analytics
    - customer-data-platform: Customer Data Platform
    - data-centers: Data Centers
    - cloud-computing: Cloud Computing
    - magazines: Tech Magazines
    """
    try:
        # Validate and cap pagination parameters
        limit = min(limit, 100)  # Max 100 items per page
        page = max(page, 1)  # Minimum page 1
        offset = (page - 1) * limit  # Calculate offset
        
        # L1: Check Redis cache (fastest path - ~5ms)
        # Note: Cache key now includes pagination params
        cache_key = f"news:{category}:p{page}:l{limit}"
        cached_data = await cache_service.get(cache_key)
        if cached_data:
            return NewsResponse(
                success=True,
                category=category,
                count=len(cached_data),
                articles=cached_data,
                cached=True,
                source="redis"
            )
        
        # L2: Check Appwrite database (fast persistent storage - ~50ms)
        db_articles = await appwrite_db.get_articles(category, limit=limit, offset=offset)
        
        if db_articles:
            # Cache the database results in Redis for next request
            await cache_service.set(cache_key, db_articles)
            
            return NewsResponse(
                success=True,
                category=category,
                count=len(db_articles),
                articles=db_articles,
                cached=True,
                source="appwrite"
            )
        
        # Database is empty - return empty state
        # Background workers will populate the database every 15 minutes
        return NewsResponse(
            success=True,
            category=category,
            count=0,
            articles=[],
            cached=False,
            source="empty",
            message="News data is being fetched by background workers. Please check back in a few minutes."
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/rss/{provider}")
async def get_rss_feed(provider: str):
    """
    Get RSS feed from cloud providers
    
    Providers: aws, gcp, azure, ibm, oracle, digitalocean
    """
    try:
        # Check cache
        cached_data = await cache_service.get(f"rss:{provider}")
        if cached_data:
            return NewsResponse(
                success=True,
                category=f"cloud-{provider}",
                count=len(cached_data),
                articles=cached_data,
                cached=True,
                source="redis"
            )
        
        # Fetch RSS
        articles = await news_aggregator.fetch_rss(provider)
        
        # Cache
        await cache_service.set(f"rss:{provider}", articles)
        
        return NewsResponse(
            success=True,
            category=f"cloud-{provider}",
            count=len(articles),
            articles=articles,
            cached=False,
            source="api"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/system/stats")
async def get_provider_stats():
    """
    Get statistics about news provider usage and health
    
    Returns information about:
    - Total requests
    - Provider usage counts
    - Failover counts
    - Available providers
    - Provider status and rate limits
    """
    try:
        stats = news_aggregator.get_stats()
        return {
            "success": True,
            **stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
