"""
Appwrite Database Service - Phase 2
Provides persistent storage for news articles with fast querying capability.
"""

# Suppress Appwrite SDK v4.1.0 deprecation warnings
# NOTE: list_documents() is deprecated but new API (tablesDB.list_rows) requires SDK v6+
# We're using v4.1.0 for stability, suppress warnings until we upgrade
import warnings
import warnings
warnings.filterwarnings('ignore', category=DeprecationWarning, module='appwrite')
warnings.filterwarnings('ignore', message='Call to deprecated function') # Catch-all for Appwrite logs

try:
    from appwrite.client import Client
    from appwrite.services.databases import Databases
    from appwrite.services.storage import Storage
    from appwrite.query import Query
    from appwrite.exception import AppwriteException
    APPWRITE_AVAILABLE = True
except ImportError:
    APPWRITE_AVAILABLE = False
    print("Appwrite SDK not available - database features disabled")

from typing import List, Optional, Dict
from datetime import datetime, timedelta
import hashlib
import asyncio # For parallel writes
from app.models import Article
from app.config import settings
import logging

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AppwriteDatabase:
    """Appwrite Database service for persistent article storage (L2 cache)"""
    
    def __init__(self):
        self.initialized = False
        self.client = None
        self.databases = None
        self.storage = None
        
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
            
            # Initialize storage service
            self.storage = Storage(self.client)
            
            self.initialized = True
            print("")
            self.initialized = True
            print("")
            print("-" * 80)
            print("[Appwrite] Database initialized successfully!")
            print(f"Database ID: {settings.APPWRITE_DATABASE_ID}")
            print(f"Collection ID: {settings.APPWRITE_COLLECTION_ID}")
            print("-" * 80)
            print("-" * 80)
            print("")
            print("")
            
            # Future-Proofing Wrapper (Migration Phase)
            # Wraps legacy 'documents' API into new 'tables' nomenclature
            class TablesDBWrapper:
                def __init__(self, db_service):
                    self.db = db_service
                
                def create_row(self, *args, **kwargs):
                    return self.db.create_document(*args, **kwargs)
                    
                def get_row(self, *args, **kwargs):
                    return self.db.get_document(*args, **kwargs)
                
                def list_rows(self, *args, **kwargs):
                    return self.db.list_documents(*args, **kwargs)

                def delete_row(self, *args, **kwargs):
                     # Mapping delete_document -> delete_row if needed
                    return self.db.delete_document(*args, **kwargs)

                def update_row(self, *args, **kwargs):
                    return self.db.update_document(*args, **kwargs)
            
            self.tablesDB = TablesDBWrapper(self.databases)
            
        except Exception as e:
            print("")
        except Exception as e:
            print("")
            print("!" * 80)
            print("[Appwrite] Initialization FAILED!")
            print(f"[ERROR] Error: {e}")
            print("[INFO] Please check your Appwrite credentials in .env file")
            print("!" * 80)
            print("")
            print("")
            self.initialized = False
            
    def get_collection_id(self, category: str) -> str:
        """
        Phase 4: Strict Routing Algorithm (Vertical Architecture)
        """
        # Normalize
        if not category or not category.strip():
            logger.warning("[ROUTING] Empty category, defaulting to News Articles")
            return settings.APPWRITE_COLLECTION_ID
            
        cat = category.lower().strip()
        
        # 1. AI Vertical
        if cat == 'ai':
            return settings.APPWRITE_AI_COLLECTION_ID
            
        # 2. Cloud Vertical (All providers)
        if cat.startswith('cloud-'):
            return settings.APPWRITE_CLOUD_COLLECTION_ID
            
        # 3. Data Vertical (Security, Governance, etc.)
        if cat.startswith('data-') or cat.startswith('business-') or cat == 'customer-data-platform':
            return settings.APPWRITE_DATA_COLLECTION_ID
            
        # 4. Magazines
        if cat == 'magazines':
            return settings.APPWRITE_MAGAZINE_COLLECTION_ID
            
        # 5. Medium
        if cat == 'medium-article':
            return settings.APPWRITE_MEDIUM_COLLECTION_ID
            
        # Default / Fallback
        logger.warning(f"[ROUTING] Unmatched category '{cat}', defaulting to News Articles")
        return settings.APPWRITE_COLLECTION_ID

    
    def _generate_url_hash(self, url: str) -> str:
        """
        Generate a unique hash for an article URL
        
        **INTEGRATION UPDATE**: Matches Schema Size 64
        Uses SHA-256 hash of the RAW URL.
        
        Returns:
            64-character hex hash
        """
        import hashlib
        # Generate SHA-256 hash from RAW URL (no canonicalization for ID)
        hash_bytes = hashlib.sha256(url.encode('utf-8')).hexdigest()
        # Return FULL 64 characters (matches DB Schema)
        return hash_bytes
    
    async def get_articles(self, category: str, limit: int = 20, offset: int = 0) -> List[Dict]:
        """
        Get articles by category with pagination and projection (FAANG-Level)
        """
        if not self.initialized:
            return []
        
        try:
            # Determine collection based on category
            target_collection_id = self.get_collection_id(category)

            # FAANG Optimization: Projection - fetch only what UI needs!
            select_fields = [
                '$id',
                'title',
                'url',
                'image_url',
                'published_at',
                'source',
                'category',
                'url_hash'
            ]
            
            # Query with projection
            response = self.tablesDB.list_rows(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=target_collection_id,
                queries=[
                    Query.equal('category', category),
                    Query.order_desc('published_at'),  # Uses index!
                    Query.limit(limit),
                    Query.offset(offset)
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
                        'image_url': doc.get('image_url', ''),
                        'publishedAt': doc.get('published_at'),
                        'published_at': doc.get('published_at'), # Standard schema field
                        'source': doc.get('source', ''),
                        'category': doc.get('category'),
                        'likes': doc.get('likes', 0),
                        'dislikes': doc.get('dislike', 0),
                        'views': doc.get('views', 0)
                    }
                    articles.append(article)
                except Exception as e:
                    print(f"Error parsing Appwrite document: {e}")
                    continue
            
            if articles:
                print(f"[SUCCESS] Retrieved {len(articles)} articles for '{category}' (Collection: {target_collection_id})")
            
            return articles
            
        except AppwriteException as e:
            print(f"Appwrite query error for category '{category}': {e}")
            return []
    
    async def get_articles_with_queries(self, queries: List, category: str = None) -> List[Dict]:
        """
        Get articles with custom query filters (for cursor pagination)
        
        Args:
            queries: List of Appwrite Query objects
            category: Optional category for explicit routing (Recommended)
        """
        if not self.initialized:
            return []
        
        try:
            # Phase 4 Routing: Determine Collection ID
            target_collection_id = settings.APPWRITE_COLLECTION_ID
            
            if category:
                # 1. Explicit Routing (Robust)
                target_collection_id = self.get_collection_id(category)
                logger.info(f"🔍 [ROUTING] Category='{category}' -> Collection='{target_collection_id}'")
            else:
                # 2. Fallback: Extract category from queries (Brittle)
                # Parse query list for 'category' to route to correct table
                for q in queries:
                    q_str = str(q)
                    if 'category' in q_str:
                        import re
                        # Regex for JSON-like string: {"attribute":"category","values":["ai"]}
                        # Logic: Look for "category" attribute, then find the value inside ["..."]
                        match = re.search(r'category.*?"values":\["([^"]+)"\]', q_str)
                        if not match:
                             # Try simpler regex (just in case string format differs)
                             match = re.search(r'category.*?"([^"]+)"', q_str)
                        
                        if match:
                            category_val = match.group(1)
                            target_collection_id = self.get_collection_id(category_val)
                            logger.info(f"🔍 [ROUTING-FALLBACK] Extracted='{category_val}' -> Collection='{target_collection_id}'")
                            break

            logger.info(f"🚀 [QUERY] Executing query on Collection: {target_collection_id}")
            
            response = self.tablesDB.list_rows(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=target_collection_id,
                queries=queries
            )
            
            # Convert to article dictionaries
            articles = []
            for doc in response['documents']:
                try:
                    article = {
                        '$id': doc.get('$id'),
                        'title': doc.get('title'),
                        'description': doc.get('description', ''),
                        'url': doc.get('url'),
                        'image_url': doc.get('image_url', ''),
                        'publishedAt': doc.get('published_at'),
                        'published_at': doc.get('published_at'),
                        'source': doc.get('source', ''),
                        'category': doc.get('category'),
                        'likes': doc.get('likes', 0),
                        'dislikes': doc.get('dislike', 0),
                        'views': doc.get('views', 0)
                    }
                    articles.append(article)
                except Exception as e:
                    continue
            
            return articles
            
        except Exception as e:
            print(f"Query error: {e}")
            return []
    
    async def save_articles(self, articles: List) -> int:
        """
        Save articles to Appwrite database with TRUE parallel writes
        """
        logger = logging.getLogger(__name__)
        
        if not self.initialized:
            return (0, 0, 0, [])
        
        if not articles:
            return (0, 0, 0, [])

        # Initialize URL Filter
        try:
            from app.services.deduplication import get_url_filter
            url_filter = get_url_filter()
        except ImportError:
            logger.warning("[Appwrite] Deduplication service not found, skipping local bloom filter check")
            url_filter = None
        
        async def save_single_article(article: dict) -> tuple:
            try:
                # Handle both dict and object types
                url = str(article.get('url', '')) if isinstance(article, dict) else str(article.url)
                if not url:
                    return ('error', None)
                
                # 1. BLOOM FILTER CHECK (Local De-duplication)
                if url_filter and not url_filter.check_and_add(url):
                    # Only return duplicate if it was actually caught by the filter
                    # This saves an API call to Appwrite
                    return ('duplicate', None)

                # Generate unique document ID (Must be <= 36 chars)
                # Use raw SHA-256 for url_hash attribute (64 chars)
                url_hash_full = self._generate_url_hash(url)
                # Truncate for Document ID (32 chars)
                doc_id = url_hash_full[:32]
                
                # Helper to get field from dict or object
                def get_field(obj, field, default=''):
                    if isinstance(obj, dict):
                        return obj.get(field, default)
                    return getattr(obj, field, default)
                
                # Route to correct collection
                category_val = str(get_field(article, 'category', ''))
                target_collection_id = self.get_collection_id(category_val)

                # Prepare document data - STRICT SCHEMA MAPPING (New Schema Enforcement)
                # Notes: 
                # 1. 'image_url' is the standard (replacing legacy 'image')
                # 2. 'published_at' is the standard (replacing legacy 'publishedAt' camelCase)
                
                # Helper to get published date safely
                pub_date = get_field(article, 'published_at') or get_field(article, 'publishedAt')
                if isinstance(pub_date, datetime):
                    pub_date_str = pub_date.isoformat()
                else:
                    pub_date_str = str(pub_date or datetime.now().isoformat())

                document_data = {
                    'title': str(get_field(article, 'title', ''))[:500],
                    'description': str(get_field(article, 'description', ''))[:2000],
                    'url': url[:2048],
                    'image_url': str(get_field(article, 'image_url') or get_field(article, 'image', ''))[:2048] or None,
                    'published_at': pub_date_str,
                    'source': str(get_field(article, 'source', ''))[:200],
                    'category': str(get_field(article, 'category', ''))[:100],
                    'fetched_at': datetime.now().isoformat(),
                    'url_hash': url_hash_full, # 64 chars
                    'slug': str(get_field(article, 'slug', ''))[:200] if get_field(article, 'slug', '') else None,
                    'quality_score': int(get_field(article, 'quality_score', 50)),
                    # ENGAGEMENT METRICS
                    'likes': 0,
                    'dislike': 0, 
                    'views': 0,
                    'audio_url': get_field(article, 'audio_url', None) # Initialize audio_url
                }
                
                # Cloud Collection Specifics (Legacy Schema requirements)
                if target_collection_id == settings.APPWRITE_CLOUD_COLLECTION_ID:
                    document_data['provider'] = document_data['source']
                    document_data['is_official'] = False # Default to False
                    
                    # FIX: Cloud collection uses legacy 'image' attribute, not 'image_url'
                    if 'image_url' in document_data:
                        document_data['image'] = document_data.pop('image_url')
                    
                    # FIX: Cloud collection uses legacy 'publishedAt' attribute, not 'published_at'
                    # Based on logs, other collections accept 'published_at' (snake_case)
                    # But Cloud might strictly require 'publishedAt' (camelCase)
                    if 'published_at' in document_data:
                         document_data['publishedAt'] = document_data.pop('published_at')

                # Try to create document
                self.tablesDB.create_row(
                    database_id=settings.APPWRITE_DATABASE_ID,
                    collection_id=target_collection_id,
                    document_id=doc_id, # Truncated ID
                    data=document_data
                )
                
                return ('success', document_data)
                
            except AppwriteException as e:
                # Document already exists (duplicate detected by Appwrite)
                if 'document_already_exists' in str(e).lower() or 'unique' in str(e).lower():
                    return ('duplicate', None)
                else:
                    logger.error(f"❌ Appwrite Write Error: {str(e)} | URL: {url[:50]}...")
                    return ('error', str(e))
                    
            except Exception as e:
                logger.error(f"❌ General Error: {str(e)} | URL: {url[:50]}...", exc_info=True)
                return ('error', str(e))
        
        # PARALLEL WRITES: Create tasks for all articles
        save_tasks = [save_single_article(article) for article in articles]
        
        # Execute all writes concurrently!
        results = await asyncio.gather(*save_tasks, return_exceptions=True)
        
        # Count results
        saved_count = 0
        saved_documents = []
        duplicate_count = 0
        error_count = 0
        
        for result in results:
            if isinstance(result, Exception):
                error_count += 1
                continue
                
            status, data = result
            if status == 'success':
                saved_count += 1
                saved_documents.append(data)
            elif status == 'duplicate':
                duplicate_count += 1
            else:  # error
                error_count += 1
        
        if saved_count > 0 or duplicate_count > 0 or error_count > 0:
            logger.info(f"[WRITE] Parallel write: {saved_count} saved, {duplicate_count} duplicates, {error_count} errors")
        
        return saved_count, duplicate_count, error_count, saved_documents
    
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
            response = self.tablesDB.list_rows(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=settings.APPWRITE_COLLECTION_ID,
                queries=[
                    Query.less_than('fetched_at', cutoff_date),
                    Query.limit(500)  # Increased from 100 to 500 for better throughput
                ]
            )
            
            deleted_count = 0
            for doc in response['documents']:
                try:
                    self.tablesDB.delete_row(
                        database_id=settings.APPWRITE_DATABASE_ID,
                        collection_id=settings.APPWRITE_COLLECTION_ID,
                        document_id=doc['$id']
                    )
                    deleted_count += 1
                except Exception as e:
                    print(f"Error deleting document {doc['$id']}: {e}")
            
            if deleted_count > 0:
                print(f"[CLEANUP] Deleted {deleted_count} articles older than {days} days")
            else:
                print(f"[CLEANUP] No old articles to delete")
            
            return deleted_count
            
        except Exception as e:
            print(f"Error deleting old articles: {e}")
            return 0
    
    # ------------------------------------------------------------------
    # SUBSCRIBER MANAGEMENT (Migration Phase 2)
    # ------------------------------------------------------------------

    async def create_subscriber(self, email: str, name: str, preferences: Dict[str, bool], token: str) -> bool:
        """
        Create a new subscriber in Appwrite (Dual-Write)
        Uses Boolean Flags schema: sub_morning, sub_afternoon, etc.
        """
        if not self.initialized:
            return False
            
        try:
            # Prepare document data
            data = {
                "email": email,
                "name": name,
                "token": token,
                "isActive": True,
                # Map dict preferences to individual boolean columns
                "sub_morning": preferences.get("Morning", False),
                "sub_afternoon": preferences.get("Afternoon", False),
                "sub_evening": preferences.get("Evening", False),
                "sub_weekly": preferences.get("Weekly", False),
                "sub_monthly": preferences.get("Monthly", False)
            }
            
            # Use email hash or sanitized email as ID to prevent duplicates
            # doc_id = hashlib.md5(email.encode()).hexdigest() 
            # Appwrite requires unique ID. 'email' attribute is unique, but let's use 'unique()' or hash.
            # Using MD5 of email ensures idempotent writes (same email = same ID)
            doc_id = hashlib.md5(email.lower().encode()).hexdigest()

            self.tablesDB.create_row(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=settings.APPWRITE_SUBSCRIBERS_COLLECTION_ID,
                document_id=doc_id,
                data=data
            )
            logger.info(f"✅ [Appwrite] Subscriber created: {email}")
            return True

        except AppwriteException as e:
            if 'document_already_exists' in str(e).lower() or 'unique' in str(e).lower():
                # If exists, we should try to update it? Or just return True?
                # For dual-write safety, let's update it to ensure sync
                logger.info(f"ℹ️ [Appwrite] Subscriber exists, updating: {email}")
                return await self.update_subscriber(email, preferences)
            
            logger.error(f"❌ [Appwrite] Error creating subscriber: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ [Appwrite] Unexpected error creating subscriber: {e}")
            return False

    async def get_subscriber(self, email: str) -> Optional[Dict]:
        """Get subscriber by email"""
        if not self.initialized:
            return None
            
        try:
            documents = self.tablesDB.list_rows(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=settings.APPWRITE_SUBSCRIBERS_COLLECTION_ID,
                queries=[Query.equal("email", email)]
            )
            
            if documents['total'] > 0:
                return documents['documents'][0]
            return None
            
        except Exception as e:
            logger.error(f"❌ [Appwrite] Error getting subscriber: {e}")
            return None

    async def update_subscriber(self, email: str, preferences: Dict[str, bool]) -> bool:
        """Update subscriber preferences"""
        if not self.initialized:
            return False
            
        try:
            # 1. Find document ID by email
            subscriber = await self.get_subscriber(email)
            if not subscriber:
                return False
            
            doc_id = subscriber['$id']
            
            # 2. Prepare update data
            data = {}
            if "Morning" in preferences: data["sub_morning"] = preferences["Morning"]
            if "Afternoon" in preferences: data["sub_afternoon"] = preferences["Afternoon"]
            if "Evening" in preferences: data["sub_evening"] = preferences["Evening"]
            if "Weekly" in preferences: data["sub_weekly"] = preferences["Weekly"]
            if "Monthly" in preferences: data["sub_monthly"] = preferences["Monthly"]
            
            # Note: tablesDB wrapper now has update_row
            self.tablesDB.update_row(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=settings.APPWRITE_SUBSCRIBERS_COLLECTION_ID,
                document_id=doc_id,
                data=data
            )
            logger.info(f"✅ [Appwrite] Subscriber updated: {email}")
            return True
            
        except Exception as e:
            logger.error(f"❌ [Appwrite] Error updating subscriber: {e}")
            return False

    async def get_subscriber_by_token(self, token: str) -> Optional[Dict]:
        """Get subscriber by unsubscribe token"""
        try:
            documents = self.tablesDB.list_rows(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=settings.APPWRITE_SUBSCRIBERS_COLLECTION_ID,
                queries=[Query.equal("token", token)]
            )
            
            if documents['total'] > 0:
                return documents['documents'][0]
            return None
            
        except Exception as e:
            logger.error(f"❌ [Appwrite] Error finding subscriber by token: {e}")
            return None

    async def update_article_audio(self, collection_id: str, document_id: str, audio_url: str) -> bool:
        """Update article with audio URL"""
        if not self.initialized:
            return False
            
        try:
            self.tablesDB.update_row(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=collection_id,
                document_id=document_id,
                data={'audio_url': audio_url}
            )
            return True
        except Exception as e:
            logger.error(f"❌ [Appwrite] Error updating article audio: {e}")
            return False

    async def update_subscription_status(self, email: str, preference: str, is_active: bool) -> bool:
        """
        Update specific subscription preference (Granular Unsubscribe)
        """
        if not self.initialized:
            return False
            
        try:
            subscriber = await self.get_subscriber(email)
            if not subscriber:
                return False
            
            # Map preference name to column name
            field_map = {
                "Morning": "sub_morning",
                "Afternoon": "sub_afternoon",
                "Evening": "sub_evening",
                "Weekly": "sub_weekly",
                "Monthly": "sub_monthly"
            }
            
            field = field_map.get(preference)
            if not field:
                logger.error(f"Invalid preference: {preference}")
                return False
                
            data = {field: is_active}
            
            self.tablesDB.update_row(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=settings.APPWRITE_SUBSCRIBERS_COLLECTION_ID,
                document_id=subscriber['$id'],
                data=data
            )
            logger.info(f"✅ [Appwrite] Updated {preference} for {email} to {is_active}")
            return True
            
        except Exception as e:
            logger.error(f"❌ [Appwrite] Error updating subscription status: {e}")
            return False

    async def update_subscriber_status(self, email: str, subscribed: bool) -> bool:
        """
        Update global subscription status (Global Unsubscribe)
        """
        if not self.initialized:
            return False
            
        try:
            subscriber = await self.get_subscriber(email)
            if not subscriber:
                return False
            
            data = {"isActive": subscribed}
            
            self.tablesDB.update_row(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=settings.APPWRITE_SUBSCRIBERS_COLLECTION_ID,
                document_id=subscriber['$id'],
                data=data
            )
            logger.info(f"✅ [Appwrite] Global status for {email} set to {subscribed}")
            return True
            
        except Exception as e:
            logger.error(f"❌ [Appwrite] Error updating global subscriber status: {e}")
            return False

    async def update_last_sent(self, email: str) -> bool:
        """
        Update lastSentAt timestamp for a subscriber
        """
        if not self.initialized:
            return False
            
        try:
            subscriber = await self.get_subscriber(email)
            if not subscriber:
                return False
            
            from datetime import datetime
            import pytz
            
            # Store in UTC ISO format
            utc_now = datetime.now(pytz.UTC).isoformat()
            
            self.tablesDB.update_row(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=settings.APPWRITE_SUBSCRIBERS_COLLECTION_ID,
                document_id=subscriber['$id'],
                data={'lastSentAt': utc_now}
            )
            # logger.debug(f"✅ [Appwrite] Updated lastSentAt for {email}")
            return True
            
        except Exception as e:
            logger.error(f"❌ [Appwrite] Error updating lastSentAt: {e}")
            return False

    async def get_subscribers_by_preference(self, preference: str) -> List[Dict]:
        """
        Get all subscribers filtered by newsletter preference
        Directly from Appwrite (Source of Truth)
        """
        if not self.initialized:
            return []
            
        try:
            # Map preference name to column name
            field_map = {
                "Morning": "sub_morning",
                "Afternoon": "sub_afternoon",
                "Evening": "sub_evening",
                "Weekly": "sub_weekly",
                "Monthly": "sub_monthly"
            }
            
            field = field_map.get(preference)
            
            # Default fallback for safety (or if preference is invalid)
            if not field:
                logger.warning(f"⚠️ [Appwrite] Unknown preference '{preference}', defaulting to Weekly")
                field = "sub_weekly"
                
            logger.info(f"🔍 [Appwrite] Fetching subscribers for {preference} ({field})...")
            
            # Query Logic:
            # 1. Must be globally active (isActive=true)
            # 2. Must be subscribed to specific preference (sub_X=true)
            
            documents = self.tablesDB.list_rows(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=settings.APPWRITE_SUBSCRIBERS_COLLECTION_ID,
                queries=[
                    Query.equal("isActive", True),
                    Query.equal(field, True),
                    Query.limit(1000) # Safety limit
                ]
            )
            
            subs = documents['documents']
            logger.info(f"✅ [Appwrite] Found {len(subs)} subscribers for {preference}")
            return subs
            
        except Exception as e:
            logger.error(f"❌ [Appwrite] Error getting subscribers by preference: {e}")
            return []

    async def update_article_audio(self, collection_id: str, document_id: str, audio_url: str) -> bool:
        """Update article with audio URL"""
        if not self.initialized:
            return False
            
        try:
            self.tablesDB.update_row(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=collection_id,
                document_id=document_id,
                data={'audio_url': audio_url}
            )
            return True
        except Exception as e:
            logger.error(f"❌ [Appwrite] Error updating article audio: {e}")
            return False

    async def get_database_stats(self) -> Dict:
        """
        Get database statistics
        
        Returns:
            Dictionary with database stats (total articles, by category, etc.)
        """
        if not self.initialized:
            return {"error": "Appwrite not initialized"}
        
        try:
            # Get total count
            total_response = self.tablesDB.list_rows(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=settings.APPWRITE_COLLECTION_ID,
                queries=[Query.limit(1)]
            )
            total_articles = total_response['total']
            
            # Get counts by category
            categories = [
                "ai", "data-security", "data-governance", "data-privacy",
                "data-engineering", "data-management", "business-intelligence", 
                "business-analytics", "customer-data-platform", "data-centers", 
                "cloud-computing", "magazines"
            ]
            
            articles_by_category = {}
            for category in categories:
                response = self.tablesDB.list_rows(
                    database_id=settings.APPWRITE_DATABASE_ID,
                    collection_id=settings.APPWRITE_COLLECTION_ID,
                    queries=[
                        Query.equal('category', category),  # SDK v4.x uses string value
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
