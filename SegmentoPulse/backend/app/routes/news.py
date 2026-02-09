from fastapi import APIRouter, HTTPException
from app.models import NewsResponse, ErrorResponse
from app.services.news_aggregator import NewsAggregator
from app.services.upstash_cache import get_upstash_cache  # New Upstash cache
from app.services.appwrite_db import get_appwrite_db
import logging

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()
news_aggregator = NewsAggregator()
upstash_cache = get_upstash_cache()  # Upstash REST API cache
appwrite_db = get_appwrite_db()

@router.get("/{category}", response_model=NewsResponse)
async def get_news_by_category(
    category: str,
    limit: int = 20,    # Items per page
    cursor: str = None,  # Cursor for pagination (replaces page number)
    page: int = 1       # Fallback for legacy frontend (offset pagination)
):
    """
    Get news articles by category with cursor pagination and stale-while-revalidate
    
    **ADVANCED OPTIMIZATIONS:**
    - Cursor-based pagination: O(1) performance at any page (no offset trap)
    - Stale-while-revalidate: Prevents thundering herd on cache expiration
    
    **THE GOLDEN RULE: Users NEVER wait for external APIs**
    - Users only read from database (Appwrite)
    - Background workers populate the database every 15 minutes
    
    **Cursor Pagination:**
    - No more page numbers! Use cursor for next page
    - Request: GET /api/news/ai?limit=20
    - Response includes: articles + next_cursor
    - Next request: GET /api/news/ai?limit=20&cursor=<next_cursor>
    
    **Fallback Support:**
    - If `page` is provided without `cursor`, we fallback to OFFSET pagination.
    - This allows legacy frontend code to work without refactoring.
    
    Categories: ai, data-security, cloud-computing, etc.
    """
    try:
        from app.utils.cursor_pagination import CursorPagination
        from appwrite.query import Query
        
        # Validate limit
        limit = min(limit, 100)  # Max 100 items per page
        
        # Build cache key (Include page/cursor)
        if cursor:
            cache_key = f"news_v3:{category}:cursor:{cursor}:l{limit}"
        else:
            cache_key = f"news_v3:{category}:page:{page}:l{limit}"
        
        # Try Upstash cache first (5 min TTL)
        if upstash_cache.enabled:
            cached_data = upstash_cache.get(cache_key)
            if cached_data:
                return NewsResponse(
                    success=True,
                    category=category,
                    count=len(cached_data.get('articles', [])),
                    articles=cached_data.get('articles', []),
                    cached=True,
                    source="upstash"
                )
        
        # Cache miss - fetch from database
        queries = []
        
        # DECISION: OFFSET vs CURSOR
        if not cursor and page > 1:
            # Fallback: Offset Pagination (Legacy Support)
            logger.info(f"🔄 [PAGINATION] Using OFFSET fallback for page {page}")
            offset = (page - 1) * limit
            
            # Note: Appwrite only supports offset up to 5000 items
            if offset > 5000:
                raise HTTPException(status_code=400, detail="Offset limit reached (5000). Use cursor pagination.")
                
            queries = [
                Query.equal('category', category),
                Query.order_desc('published_at'),
                Query.limit(limit),
                Query.offset(offset)
            ]
        else:
            # Default: Cursor Pagination (Preferred)
            # Pass category to build_query_filters so it adds Query.equal('category', ...)
            queries = CursorPagination.build_query_filters(cursor, category)
            queries.append(Query.limit(limit + 1))  # Fetch one extra to check if more exist
        
        # ROUTING: Explicitly pass category to ensure correct collection is selected
        articles = await appwrite_db.get_articles_with_queries(queries, category=category)
        
        # Handle "one extra" logic ONLY for Cursor Pagination
        has_more = False
        next_cursor = None
        
        if not cursor and page > 1:
             # Offset mode: We don't fetch extra item, checking has_more is harder without total count
             # Typically assume has_more if len(articles) == limit
             has_more = len(articles) == limit
        else:
            # Cursor mode
            has_more = len(articles) > limit
            if has_more:
                articles = articles[:limit]  # Remove the extra one
            
            # Generate next cursor from last article
            if has_more and articles:
                last_article = articles[-1]
                next_cursor = CursorPagination.encode_cursor(
                    last_article.get('publishedAt') or last_article.get('published_at'),
                    last_article.get('$id')
                )
        
        response_data = NewsResponse(
            success=True,
            category=category,
            count=len(articles),
            articles=articles,
            cached=False,
            source="appwrite"
        )
        
        # Cache the result (5 min TTL)
        if upstash_cache.enabled:
            upstash_cache.set(
                cache_key,
                {"articles": articles, "has_more": has_more, "next_cursor": next_cursor},
                ttl=300  # 5 minutes
            )
        
        return response_data
        
    except Exception as e:
        import traceback
        traceback.print_exc() 
        logger.error(f"Error fetching news: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/rss/{provider}")
async def get_rss_feed(provider: str):
    """
    Get RSS feed from cloud providers
    
    Providers: aws, gcp, azure, ibm, oracle, digitalocean
    """
    try:
        # Check Upstash cache
        cache_key = f"rss:{provider}"
        if upstash_cache.enabled:
            cached_data = upstash_cache.get(cache_key)
            if cached_data:
                return NewsResponse(
                    success=True,
                    category=f"cloud-{provider}",
                    count=len(cached_data),
                    articles=cached_data,
                    cached=True,
                    source="upstash"
                )
        
        # Fetch RSS
        articles = await news_aggregator.fetch_rss(provider)
        
        # Cache in Upstash (10 min TTL for RSS feeds)
        if upstash_cache.enabled:
            upstash_cache.set(cache_key, articles, ttl=600)
        
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
