"""
Engagement API Endpoints
Handles article likes, views tracking, and trending articles
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import Optional
from app.services.appwrite_db import get_appwrite_db
from app.config import settings
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/articles/{article_id}/like")
async def like_article(article_id: str):
    """
    Increment like count for an article.
    
    Phase 3: Engagement tracking for article popularity.
    
    Args:
        article_id: Document ID from Appwrite
        
    Returns:
        Updated likes count
    """
    try:
        appwrite_db = get_appwrite_db()
        
        # Try regular articles collection first
        collection_id = settings.APPWRITE_COLLECTION_ID
        
        try:
            doc = appwrite_db.databases.get_document(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=collection_id,
                document_id=article_id
            )
        except:
            # Try cloud articles collection
            if settings.APPWRITE_CLOUD_COLLECTION_ID:
                collection_id = settings.APPWRITE_CLOUD_COLLECTION_ID
                doc = appwrite_db.databases.get_document(
                    database_id=settings.APPWRITE_DATABASE_ID,
                    collection_id=collection_id,
                    document_id=article_id
                )
            else:
                raise HTTPException(status_code=404, detail="Article not found")
        
        # Increment likes
        current_likes = doc.get('likes', 0)
        new_likes = current_likes + 1
        
        # Update document
        appwrite_db.databases.update_document(
            database_id=settings.APPWRITE_DATABASE_ID,
            collection_id=collection_id,
            document_id=article_id,
            data={"likes": new_likes}
        )
        
        logger.info(f"❤️  Article {article_id[:8]}... liked (total: {new_likes})")
        
        return {
            "article_id": article_id,
            "likes": new_likes,
            "success": True
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error liking article {article_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/articles/{article_id}/dislike")
async def dislike_article(article_id: str):
    """
    Increment dislike count for an article.
    
    Phase 3: Engagement tracking for article feedback.
    
    Args:
        article_id: Document ID from Appwrite
        
    Returns:
        Updated dislikes count
    """
    try:
        appwrite_db = get_appwrite_db()
        
        # Try regular articles collection first
        collection_id = settings.APPWRITE_COLLECTION_ID
        
        try:
            doc = appwrite_db.databases.get_document(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=collection_id,
                document_id=article_id
            )
        except:
            # Try cloud articles collection
            if settings.APPWRITE_CLOUD_COLLECTION_ID:
                collection_id = settings.APPWRITE_CLOUD_COLLECTION_ID
                doc = appwrite_db.databases.get_document(
                    database_id=settings.APPWRITE_DATABASE_ID,
                    collection_id=collection_id,
                    document_id=article_id
                )
            else:
                raise HTTPException(status_code=404, detail="Article not found")
        
        # Increment dislikes
        current_dislikes = doc.get('dislikes', 0)
        new_dislikes = current_dislikes + 1
        
        # Update document
        appwrite_db.databases.update_document(
            database_id=settings.APPWRITE_DATABASE_ID,
            collection_id=collection_id,
            document_id=article_id,
            data={"dislikes": new_dislikes}
        )
        
        logger.info(f"👎 Article {article_id[:8]}... disliked (total: {new_dislikes})")
        
        return {
            "article_id": article_id,
            "dislikes": new_dislikes,
            "success": True
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error disliking article {article_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/articles/{article_id}/view")
async def track_view(article_id: str):
    """
    Increment view count for an article.
    
    Phase 3: Track article views for analytics.
    
    Args:
        article_id: Document ID from Appwrite
        
    Returns:
        Updated views count
    """
    try:
        appwrite_db = get_appwrite_db()
        
        # Try regular articles collection first
        collection_id = settings.APPWRITE_COLLECTION_ID
        
        try:
            doc = appwrite_db.databases.get_document(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=collection_id,
                document_id=article_id
            )
        except:
            # Try cloud articles collection
            if settings.APPWRITE_CLOUD_COLLECTION_ID:
                collection_id = settings.APPWRITE_CLOUD_COLLECTION_ID
                doc = appwrite_db.databases.get_document(
                    database_id=settings.APPWRITE_DATABASE_ID,
                    collection_id=collection_id,
                    document_id=article_id
                )
            else:
                raise HTTPException(status_code=404, detail="Article not found")
        
        # Increment views
        current_views = doc.get('views', 0)
        new_views = current_views + 1
        
        # Update document
        appwrite_db.databases.update_document(
            database_id=settings.APPWRITE_DATABASE_ID,
            collection_id=collection_id,
            document_id=article_id,
            data={"views": new_views}
        )
        
        # Log only every 10 views to avoid spam
        if new_views % 10 == 0:
            logger.info(f"👁️  Article {article_id[:8]}... reached {new_views} views")
        
        return {
            "article_id": article_id,
            "views": new_views,
            "success": True
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error tracking view for {article_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/articles/trending")
async def get_trending_articles(
    hours: int = 24,
    limit: int = 10,
    cloud_only: bool = False
):
    """
    Get trending articles based on views and likes.
    
    Phase 3: Discover popular content.
    
    Args:
        hours: Time window for trending (default: 24 hours)
        limit: Number of articles to return (default: 10)
        cloud_only: Only return cloud articles (default: False)
        
    Returns:
        List of trending articles sorted by engagement
    """
    try:
        from appwrite.query import Query
        
        appwrite_db = get_appwrite_db()
        cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()
        
        # Determine collection
        if cloud_only and settings.APPWRITE_CLOUD_COLLECTION_ID:
            collection_id = settings.APPWRITE_CLOUD_COLLECTION_ID
        else:
            collection_id = settings.APPWRITE_COLLECTION_ID
        
        # Query articles, sorted by views (descending)
        response = appwrite_db.databases.list_documents(
            database_id=settings.APPWRITE_DATABASE_ID,
            collection_id=collection_id,
            queries=[
                Query.greater_than('publishedAt', cutoff),
                Query.order_desc('views'),
                Query.limit(limit)
            ]
        )
        
        articles = response['documents']
        
        # Calculate engagement score (views + likes * 5 - dislikes * 3)
        # Likes are weighted higher, dislikes have negative impact
        for article in articles:
            views = article.get('views', 0)
            likes = article.get('likes', 0)
            dislikes = article.get('dislikes', 0)
            article['engagement_score'] = views + (likes * 5) - (dislikes * 3)
        
        # Sort by engagement score
        articles.sort(key=lambda x: x.get('engagement_score', 0), reverse=True)
        
        logger.info(f"🔥 Trending: {len(articles)} articles in last {hours}h")
        
        return {
            "articles": articles[:limit],
            "timeframe_hours": hours,
            "cloud_only": cloud_only,
            "total_count": len(articles)
        }
        
    except Exception as e:
        logger.error(f"Error getting trending articles: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/articles/popular-cloud")
async def get_popular_cloud_articles(provider: Optional[str] = None, limit: int = 10):
    """
    Get popular cloud articles, optionally filtered by provider.
    
    Phase 3: Cloud-specific trending.
    
    Args:
        provider: Cloud provider (aws, azure, gcp, etc.) or None for all
        limit: Number of articles (default: 10)
        
    Returns:
        Popular cloud articles
    """
    try:
        from appwrite.query import Query
        
        if not settings.APPWRITE_CLOUD_COLLECTION_ID:
            raise HTTPException(status_code=404, detail="Cloud collection not configured")
        
        appwrite_db = get_appwrite_db()
        
        queries = [
            Query.order_desc('views'),
            Query.limit(limit)
        ]
        
        # Filter by provider if specified
        if provider:
            queries.insert(0, Query.equal('provider', provider))
        
        response = appwrite_db.databases.list_documents(
            database_id=settings.APPWRITE_DATABASE_ID,
            collection_id=settings.APPWRITE_CLOUD_COLLECTION_ID,
            queries=queries
        )
        
        articles = response['documents']
        
        logger.info(f"☁️  Popular cloud articles: {len(articles)} (provider={provider or 'all'})")
        
        return {
            "articles": articles,
            "provider": provider,
            "total_count": len(articles)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting popular cloud articles: {e}")
        raise HTTPException(status_code=500, detail=str(e))
