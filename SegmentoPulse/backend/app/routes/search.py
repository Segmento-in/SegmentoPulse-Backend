from fastapi import APIRouter, HTTPException, Query
from app.models import SearchResponse
from app.services.news_aggregator import NewsAggregator
from app.services.cache_service import CacheService
import logging

logger = logging.getLogger(__name__)

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
        
        # Strategy: Hybrid Search (Semantic -> Keyword)
        
        # 1. Semantic Search (Agentic RAG)
        # We try to get "smart" results first
        from app.services.vector_store import vector_store as _vector_store
        semantic_articles = _vector_store.search_articles(q, limit=10)
        
        # 2. Keyword Search (Aggregator Fallback/Augmentation)
        # We always fetch some keyword results too, to ensure we don't miss exact matches
        # or if Vector DB is empty/cold.
        keyword_articles = await news_aggregator.search(q)
        
        # 3. Merge Strategies
        # Create a dict by URL to deduplicate
        merged_map = {}
        
        # Stats for observability
        semantic_count = 0
        keyword_count = len(keyword_articles)
        
        # Add Semantic results first (Higher priority?) 
        # Actually, let's prioritize them but ensure unique valid URLs
        for art in semantic_articles:
            if art.get('url'):
                merged_map[art['url']] = art
                semantic_count += 1
                
        # Add Keyword results (Only if not already present)
        for art in keyword_articles:
            if art.get('url') and art['url'] not in merged_map:
                merged_map[art['url']] = art
                
        # Convert back to list
        final_articles = list(merged_map.values())
        
        # Observability: Log Search Performance
        import time
        end_time = time.time()
        # We assume start_time could be added at top of function, but for now we just log counts
        logger.info("🔎 [Search] Query: '%s' | Total: %d", q, len(final_articles))
        logger.info("   -> 🧠 Vector Matches: %d", semantic_count)
        logger.info("   -> 🔑 Keyword Matches: %d", keyword_count)
        logger.info("   -> 🔗 Valid Merged: %d", len(final_articles))
        
        # If we have NO results, that's fine
        
        # Cache results
        await cache_service.set(cache_key, final_articles, ttl=300)
        
        return SearchResponse(
            success=True,
            query=q,
            count=len(final_articles),
            articles=final_articles
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
