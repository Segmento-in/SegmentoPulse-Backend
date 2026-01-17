"""
Appwrite Database Service - Phase 2
Provides persistent storage for news articles with fast querying capability.
"""

try:
    from appwrite.client import Client
    from appwrite.services.databases import Databases
    from appwrite.query import Query
    from appwrite.exception import AppwriteException
    APPWRITE_AVAILABLE = True
except ImportError:
    APPWRITE_AVAILABLE = False
    print("Appwrite SDK not available - database features disabled")

from typing import List, Optional, Dict
from datetime import datetime, timedelta
import hashlib
from app.models import Article
from app.config import settings


class AppwriteDatabase:
    """Appwrite Database service for persistent article storage (L2 cache)"""
    
    def __init__(self):
        self.initialized = False
        self.client = None
        self.databases = None
        
        if APPWRITE_AVAILABLE and settings.APPWRITE_PROJECT_ID:
            self._initialize()
    
    def _initialize(self):
        """Initialize Appwrite client and database connection"""
        if not APPWRITE_AVAILABLE:
            return
        
        try:
            # Check if required config is present
            if not settings.APPWRITE_PROJECT_ID or not settings.APPWRITE_API_KEY:
                print("Appwrite credentials not configured - database features disabled")
                self.initialized = False
                return
            
            # Initialize Appwrite client
            self.client = Client()
            self.client.set_endpoint(settings.APPWRITE_ENDPOINT)
            self.client.set_project(settings.APPWRITE_PROJECT_ID)
            self.client.set_key(settings.APPWRITE_API_KEY)
            
            # Initialize databases service
            self.databases = Databases(self.client)
            
            self.initialized = True
            print(f"✓ Appwrite database initialized successfully")
            print(f"  Database: {settings.APPWRITE_DATABASE_ID}")
            print(f"  Collection: {settings.APPWRITE_COLLECTION_ID}")
            
        except Exception as e:
            print(f"✗ Appwrite initialization error: {e}")
            self.initialized = False
    
    def _generate_url_hash(self, url: str) -> str:
        """
        Generate unique hash from article URL for use as document ID
        This prevents duplicate articles in the database
        """
        return hashlib.sha256(url.encode()).hexdigest()[:16]
    
    async def get_articles(self, category: str, limit: int = 20) -> List[Dict]:
        """
        Get articles by category from Appwrite database (L2 cache)
        
        Args:
            category: News category (e.g., 'ai', 'data-security')
            limit: Maximum number of articles to return
        
        Returns:
            List of article dictionaries, sorted by published_at DESC
        """
        if not self.initialized:
            return []
        
        try:
            # Query articles by category, sorted by published date
            response = self.databases.list_documents(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=settings.APPWRITE_COLLECTION_ID,
                queries=[
                    Query.equal('category', [category]),  # SDK v5+ requires array format
                    Query.order_desc('published_at'),
                    Query.limit(limit)
                ]
            )
            
            # Convert Appwrite documents to Article dictionaries
            articles = []
            for doc in response['documents']:
                try:
                    article = {
                        'title': doc.get('title'),
                        'description': doc.get('description', ''),
                        'url': doc.get('url'),
                        'image': doc.get('image_url', ''),
                        'publishedAt': doc.get('published_at'),
                        'source': doc.get('source', ''),
                        'category': doc.get('category')
                    }
                    articles.append(article)
                except Exception as e:
                    print(f"Error parsing Appwrite document: {e}")
                    continue
            
            if articles:
                print(f"✓ Retrieved {len(articles)} articles for '{category}' from Appwrite (L2 cache)")
            
            return articles
            
        except AppwriteException as e:
            print(f"Appwrite query error for category '{category}': {e}")
            return []
        except Exception as e:
            print(f"Unexpected error querying Appwrite: {e}")
            return []
    
    async def save_articles(self, articles: List[Article]) -> int:
        """
        Save articles to Appwrite database with duplicate prevention
        
        Args:
            articles: List of Article objects to save
        
        Returns:
            Number of articles successfully saved (excluding duplicates)
        """
        if not self.initialized:
            return 0
        
        if not articles:
            return 0
        
        saved_count = 0
        skipped_count = 0
        
        for article in articles:
            try:
                # Generate unique document ID from URL hash
                url_hash = self._generate_url_hash(str(article.url))
                
                # Prepare document data
                document_data = {
                    'title': article.title[:500],  # Limit to attribute size
                    'description': article.description[:2000] if article.description else '',
                    'url': str(article.url)[:2048],
                    'image_url': article.image[:2048] if article.image else '',
                    'published_at': article.publishedAt.isoformat() if isinstance(article.publishedAt, datetime) else article.publishedAt,
                    'source': article.source[:200] if article.source else '',
                    'category': article.category[:100],
                    'fetched_at': datetime.now().isoformat(),
                    'url_hash': url_hash
                }
                
                # Try to create document (will fail if duplicate exists)
                try:
                    self.databases.create_document(
                        database_id=settings.APPWRITE_DATABASE_ID,
                        collection_id=settings.APPWRITE_COLLECTION_ID,
                        document_id=url_hash,  # Use hash as ID for duplicate prevention
                        data=document_data
                    )
                    saved_count += 1
                    
                except AppwriteException as e:
                    # Document with this ID already exists (duplicate)
                    if 'document_already_exists' in str(e).lower() or 'unique' in str(e).lower():
                        skipped_count += 1
                    else:
                        print(f"Error saving article '{article.title[:50]}...': {e}")
                
            except Exception as e:
                print(f"Unexpected error saving article: {e}")
                continue
        
        if saved_count > 0:
            print(f"✓ Saved {saved_count} new articles to Appwrite")
        if skipped_count > 0:
            print(f"  Skipped {skipped_count} duplicate articles")
        
        return saved_count
    
    async def delete_old_articles(self, days: int = 30) -> int:
        """
        Delete articles older than specified days
        
        Args:
            days: Delete articles older than this many days
        
        Returns:
            Number of articles deleted
        """
        if not self.initialized:
            return 0
        
        try:
            cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
            
            # Query old articles
            response = self.databases.list_documents(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=settings.APPWRITE_COLLECTION_ID,
                queries=[
                    Query.less_than('fetched_at', cutoff_date),
                    Query.limit(100)  # Delete in batches
                ]
            )
            
            deleted_count = 0
            for doc in response['documents']:
                try:
                    self.databases.delete_document(
                        database_id=settings.APPWRITE_DATABASE_ID,
                        collection_id=settings.APPWRITE_COLLECTION_ID,
                        document_id=doc['$id']
                    )
                    deleted_count += 1
                except Exception as e:
                    print(f"Error deleting document {doc['$id']}: {e}")
            
            if deleted_count > 0:
                print(f"✓ Deleted {deleted_count} articles older than {days} days")
            
            return deleted_count
            
        except Exception as e:
            print(f"Error deleting old articles: {e}")
            return 0
    
    async def get_stats(self) -> Dict:
        """
        Get database statistics
        
        Returns:
            Dictionary with database stats (total articles, by category, etc.)
        """
        if not self.initialized:
            return {"error": "Appwrite not initialized"}
        
        try:
            # Get total count
            total_response = self.databases.list_documents(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=settings.APPWRITE_COLLECTION_ID,
                queries=[Query.limit(1)]
            )
            total_articles = total_response['total']
            
            # Get counts by category
            categories = [
                "ai", "data-security", "data-governance", "data-privacy",
                "data-engineering", "business-intelligence", "business-analytics",
                "customer-data-platform", "data-centers", "cloud-computing", "magazines"
            ]
            
            articles_by_category = {}
            for category in categories:
                response = self.databases.list_documents(
                    database_id=settings.APPWRITE_DATABASE_ID,
                    collection_id=settings.APPWRITE_COLLECTION_ID,
                    queries=[
                        Query.equal('category', [category]),  # SDK v5+ requires array format
                        Query.limit(1)
                    ]
                )
                articles_by_category[category] = response['total']
            
            return {
                "total_articles": total_articles,
                "articles_by_category": articles_by_category,
                "database_id": settings.APPWRITE_DATABASE_ID,
                "collection_id": settings.APPWRITE_COLLECTION_ID,
                "initialized": self.initialized
            }
            
        except Exception as e:
            print(f"Error getting database stats: {e}")
            return {"error": str(e)}


# Singleton instance
_appwrite_db = None

def get_appwrite_db() -> AppwriteDatabase:
    """Get or create Appwrite database singleton instance"""
    global _appwrite_db
    if _appwrite_db is None:
        _appwrite_db = AppwriteDatabase()
    return _appwrite_db
