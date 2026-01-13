from fastapi import APIRouter, HTTPException, Query
from app.models import SearchResponse
from app.services.news_aggregator import NewsAggregator
from app.services.cache_service import CacheService

router = APIRouter()
news_aggregator = NewsAggregator()
cache_service = CacheService()

@router.get("/", response_model=SearchResponse)
async def search_news(q: str = Query(..., min_length=2, description="Search query")):
    """
    Search news articles by keyword
    """
    try:
        # Check cache
        cache_key = f"search:{q.lower()}"
        cached_data = await cache_service.get(cache_key)
        if cached_data:
            return SearchResponse(
                success=True,
                query=q,
                count=len(cached_data),
                articles=cached_data
            )
        
        # Search articles
        articles = await news_aggregator.search(q)
        
        # Cache results
        await cache_service.set(cache_key, articles, ttl=300)  # 5 min cache for searches
        
        return SearchResponse(
            success=True,
            query=q,
            count=len(articles),
            articles=articles
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
