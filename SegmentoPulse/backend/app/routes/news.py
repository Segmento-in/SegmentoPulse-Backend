from fastapi import APIRouter, HTTPException
from app.models import NewsResponse, ErrorResponse
from app.services.news_aggregator import NewsAggregator
from app.services.cache_service import CacheService

router = APIRouter()
news_aggregator = NewsAggregator()
cache_service = CacheService()

@router.get("/{category}", response_model=NewsResponse)
async def get_news_by_category(category: str):
    """
    Get news articles by category
    
    Categories:
    - ai: Artificial Intelligence
    - data-security: Data Security
    - data-governance: Data Governance
    - data-privacy: Data Privacy
    - data-engineering: Data Engineering
    - business-intelligence: Business Intelligence
    - business-analytics: Business Analytics
    - customer-data-platform: Customer Data Platform
    - data-centers: Data Centers
    - cloud-computing: Cloud Computing
    - magazines: Tech Magazines
    """
    try:
        # Check cache first
        cached_data = await cache_service.get(f"news:{category}")
        if cached_data:
            return NewsResponse(
                success=True,
                category=category,
                count=len(cached_data),
                articles=cached_data,
                cached=True
            )
        
        # Fetch fresh data
        articles = await news_aggregator.fetch_by_category(category)
        
        # Cache the results
        await cache_service.set(f"news:{category}", articles)
        
        return NewsResponse(
            success=True,
            category=category,
            count=len(articles),
            articles=articles,
            cached=False
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
                cached=True
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
            cached=False
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
