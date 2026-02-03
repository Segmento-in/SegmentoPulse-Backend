"""
Advanced Search Routes - V2 Hybrid Search
==========================================
Implements intelligent hybrid search with:
- Semantic vector search (ChromaDB)
- Time decay ranking
- Engagement-aware boosting
- Redis semantic caching (5min TTL)
- Metadata filtering (category, cloud provider, status)
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
from pydantic import BaseModel
import hashlib
import time
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


# Response models
class SearchResultV2(BaseModel):
    id: str
    title: str
    description: str
    url: str
    source: str
    published_at: str
    image: str
    category: str
    tags: Optional[str] = ""
    is_cloud_news: Optional[bool] = False
    cloud_provider: Optional[str] = ""
    likes: int = 0
    views: int = 0
    relevance_score: float
    time_decay: float
    final_score: float
    hours_old: float


class HybridSearchResponse(BaseModel):
    success: bool
    query: str
    count: int
    cache_hit: bool
    processing_time_ms: float
    results: List[SearchResultV2]
    filters_applied: dict


@router.get("/v2", response_model=HybridSearchResponse)
async def hybrid_search_v2(
    q: str = Query(..., min_length=2, description="Search query"),
    category: Optional[str] = Query(None, description="Filter by category"),
    cloud_provider: Optional[str] = Query(None, description="Filter by cloud provider (aws, azure, gcp, etc.)"),
    limit: int = Query(20, ge=1, le=100, description="Maximum results"),
    max_hours: Optional[int] = Query(None, ge=1, le=168, description="Filter articles within N hours"),
    decay_factor: float = Query(0.1, ge=0.0, le=1.0, description="Time decay strength")
):
    """
    V2 Hybrid Search Endpoint
    
    Features:
    - Semantic vector search using ChromaDB
    - Time decay ranking (fresher = better)
    - Engagement boosting (likes/views)
    - Redis semantic caching (5min TTL)
    - Category/cloud provider filtering
    - Fail-open Redis (continues without cache if Redis down)
    
    Performance Target: <200ms average
    """
    start_time = time.time()
    cache_hit = False
    
    try:
        # ================================================================
        # Step 1: Semantic Caching (Redis)
        # ================================================================
        from app.services.cache_service import CacheService
        cache_service = CacheService()
        
        # Create cache key from query + filters
        filter_str = f"{category}_{cloud_provider}_{limit}_{max_hours}_{decay_factor}"
        cache_key_raw = f"search:v2:{q.lower()}:{filter_str}"
        cache_key = hashlib.md5(cache_key_raw.encode()).hexdigest()
        
        # Try cache (fail-open pattern)
        try:
            cached_data = await cache_service.get(cache_key)
            if cached_data:
                cache_hit = True
                processing_time = (time.time() - start_time) * 1000
                logger.info(f"⚡ [SearchV2] Cache HIT for query: '{q}' ({processing_time:.1f}ms)")
                
                return HybridSearchResponse(
                    success=True,
                    query=q,
                    count=len(cached_data.get('results', [])),
                    cache_hit=True,
                    processing_time_ms=round(processing_time, 2),
                    results=cached_data.get('results', []),
                    filters_applied=cached_data.get('filters_applied', {})
                )
        except Exception as cache_error:
            # Fail open - continue without cache
            logger.warning(f"⚠️  [SearchV2] Redis unavailable, proceeding without cache: {cache_error}")
        
        # ================================================================
        # Step 2: Vector Search with Metadata Filtering
        # ================================================================
        from app.services.vector_store import vector_store
        
        # Ensure vector store is initialized
        if not vector_store._initialized:
            vector_store._initialize()
        
        if not vector_store._initialized or not vector_store.collection:
            raise HTTPException(status_code=503, detail="Vector store not available")
        
        # Build ChromaDB where filter
        where_filter = {}
        
        # Category filter
        if category:
            where_filter["category"] = category
        
        # Cloud provider filter
        if cloud_provider:
            where_filter["cloud_provider"] = cloud_provider.lower()
            where_filter["is_cloud_news"] = True
        
        # Generate query embedding
        query_embedding = vector_store.embedder.encode(q).tolist()
        
        # Query ChromaDB with filters
        # Fetch more results initially for better re-ranking
        initial_limit = min(limit * 3, 50)
        
        search_kwargs = {
            "query_embeddings": [query_embedding],
            "n_results": initial_limit
        }
        
        if where_filter:
            search_kwargs["where"] = where_filter
        
        chroma_results = vector_store.collection.query(**search_kwargs)
        
        # ================================================================
        # Step 3: Parse ChromaDB Results
        # ================================================================
        if not chroma_results['ids'] or not chroma_results['ids'][0]:
            # No results found
            processing_time = (time.time() - start_time) * 1000
            empty_response = {
                'results': [],
                'filters_applied': {
                    'category': category,
                    'cloud_provider': cloud_provider,
                    'max_hours': max_hours
                }
            }
            
            # Cache empty results too (prevent repeated searches)
            try:
                await cache_service.set(cache_key, empty_response, ttl=300)
            except Exception:
                pass
            
            return HybridSearchResponse(
                success=True,
                query=q,
                count=0,
                cache_hit=False,
                processing_time_ms=round(processing_time, 2),
                results=[],
                filters_applied=empty_response['filters_applied']
            )
        
        # Parse results
        ids = chroma_results['ids'][0]
        metadatas = chroma_results['metadatas'][0]
        distances = chroma_results['distances'][0]
        
        raw_results = []
        for i, doc_id in enumerate(ids):
            raw_results.append({
                'id': doc_id,
                'metadata': metadatas[i],
                'distance': distances[i]
            })
        
        # ================================================================
        # Step 4: Apply Time Decay Ranking
        # ================================================================
        from app.utils.ranking import apply_time_decay, apply_engagement_boost, filter_by_recency
        
        # Time decay
        ranked_results = apply_time_decay(raw_results, decay_factor=decay_factor)
        
        # Engagement boost
        ranked_results = apply_engagement_boost(ranked_results, boost_factor=0.05)
        
        # Recency filter (if specified)
        if max_hours:
            ranked_results = filter_by_recency(ranked_results, max_hours=max_hours)
        
        # Limit results
        ranked_results = ranked_results[:limit]
        
        # ================================================================
        # Step 5: Format Response
        # ================================================================
        formatted_results = []
        for result in ranked_results:
            meta = result['metadata']
            
            formatted_results.append(SearchResultV2(
                id=result['id'],
                title=meta.get('title', 'Untitled'),
                description=meta.get('description', ''),
                url=meta.get('url', '#'),
                source=meta.get('source', 'Segmento AI'),
                published_at=meta.get('published_at', ''),
                image=meta.get('image', ''),
                category=meta.get('category', 'General'),
                tags=meta.get('tags', ''),
                is_cloud_news=meta.get('is_cloud_news', False),
                cloud_provider=meta.get('cloud_provider', ''),
                likes=meta.get('likes', 0),
                views=meta.get('views', 0),
                relevance_score=result.get('_relevance_score', 0.0),
                time_decay=result.get('_time_decay', 1.0),
                final_score=result.get('_final_score', 0.0),
                hours_old=result.get('_hours_old', 0.0)
            ))
        
        # ================================================================
        # Step 6: Cache Results (300s = 5min TTL)
        # ================================================================
        filters_applied = {
            'category': category,
            'cloud_provider': cloud_provider,
            'max_hours': max_hours,
            'decay_factor': decay_factor
        }
        
        cache_data = {
            'results': [r.dict() for r in formatted_results],
            'filters_applied': filters_applied
        }
        
        try:
            await cache_service.set(cache_key, cache_data, ttl=300)
        except Exception as cache_error:
            logger.warning(f"⚠️  [SearchV2] Failed to cache results: {cache_error}")
        
        # ================================================================
        # Response
        # ================================================================
        processing_time = (time.time() - start_time) * 1000
        
        logger.info(f"🔎 [SearchV2] Query: '{q}' | Results: {len(formatted_results)} | Time: {processing_time:.1f}ms")
        logger.info(f"   → Filters: category={category}, cloud={cloud_provider}, hours={max_hours}")
        
        return HybridSearchResponse(
            success=True,
            query=q,
            count=len(formatted_results),
            cache_hit=False,
            processing_time_ms=round(processing_time, 2),
            results=formatted_results,
            filters_applied=filters_applied
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"❌ [SearchV2] Search failed: {e}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")
