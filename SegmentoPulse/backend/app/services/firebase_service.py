try:
    import firebase_admin
    from firebase_admin import credentials, db
    FIREBASE_AVAILABLE = True
except ImportError:
    FIREBASE_AVAILABLE = False
    print("Firebase not available - analytics disabled")

from typing import Optional
import base64
from app.config import settings

class FirebaseService:
    """Firebase Realtime Database service for analytics (optional)"""
    
    def __init__(self):
        self.initialized = False
        self.db_ref = None
        if FIREBASE_AVAILABLE:
            self._initialize()
    
    def _initialize(self):
        """Initialize Firebase Admin SDK"""
        if not FIREBASE_AVAILABLE:
            return
        
        try:
            if not firebase_admin._apps:
                cred = credentials.Certificate(settings.FIREBASE_CREDENTIALS_PATH if hasattr(settings, 'FIREBASE_CREDENTIALS_PATH') else './firebase-credentials.json')
                firebase_admin.initialize_app(cred, {
                    'databaseURL': settings.FIREBASE_DATABASE_URL if hasattr(settings, 'FIREBASE_DATABASE_URL') else ''
                })
            
            self.db_ref = db.reference('pulse/article_views')
            self.initialized = True
        except Exception as e:
            print(f"Firebase initialization error: {e}")
            self.initialized = False
    
    def _get_article_id(self, article_url: str) -> str:
        """Generate article ID from URL"""
        # Base64 encode and sanitize
        encoded = base64.b64encode(article_url.encode()).decode()
        # Remove non-alphanumeric characters and limit length
        sanitized = ''.join(c for c in encoded if c.isalnum())[:100]
        return sanitized
    
    async def increment_view(self, article_url: str) -> int:
        """Increment view count for an article"""
        if not self.initialized:
            return 0
        
        try:
            article_id = self._get_article_id(article_url)
            article_ref = self.db_ref.child(article_id)
            
            # Get current data
            current_data = article_ref.get()
            
            if current_data:
                # Increment existing count
                new_count = current_data.get('viewCount', 0) + 1
                article_ref.update({
                    'viewCount': new_count,
                    'url': article_url,
                    'lastUpdated': {'.sv': 'timestamp'}
                })
                return new_count
            else:
                # Create new entry
                article_ref.set({
                    'url': article_url,
                    'viewCount': 1,
                    'lastUpdated': {'.sv': 'timestamp'}
                })
                return 1
        except Exception as e:
            print(f"Error incrementing view: {e}")
            return 0
    
    async def get_view_count(self, article_url: str) -> int:
        """Get view count for an article"""
        if not self.initialized:
            return 0
        
        try:
            article_id = self._get_article_id(article_url)
            article_ref = self.db_ref.child(article_id)
            data = article_ref.get()
            
            if data:
                return data.get('viewCount', 0)
            return 0
        except Exception as e:
            print(f"Error getting view count: {e}")
            return 0
    
    # Subscriber Management Methods
    
    def add_subscriber(self, email: str, subscriber_data: dict) -> bool:
        """Add a new subscriber to database"""
        if not self.initialized:
            return False
        
        try:
            # Use email hash as key for privacy
            import hashlib
            email_hash = hashlib.sha256(email.encode()).hexdigest()[:16]
            
            subscribers_ref = db.reference('pulse/subscribers')
            subscriber_ref = subscribers_ref.child(email_hash)
            
            subscriber_ref.set(subscriber_data)
            return True
        except Exception as e:
            print(f"Error adding subscriber: {e}")
            return False
    
    def get_subscriber(self, email: str) -> Optional[dict]:
        """Get subscriber by email"""
        if not self.initialized:
            return None
        
        try:
            import hashlib
            email_hash = hashlib.sha256(email.encode()).hexdigest()[:16]
            
            subscribers_ref = db.reference('pulse/subscribers')
            subscriber_ref = subscribers_ref.child(email_hash)
            
            return subscriber_ref.get()
        except Exception as e:
            print(f"Error getting subscriber: {e}")
            return None
    
    def get_subscriber_by_token(self, token: str) -> Optional[dict]:
        """Get subscriber by unsubscribe token"""
        if not self.initialized:
            return None
        
        try:
            subscribers_ref = db.reference('pulse/subscribers')
            all_subscribers = subscribers_ref.get()
            
            if not all_subscribers:
                return None
            
            # Search for subscriber with matching token
            for subscriber_id, subscriber_data in all_subscribers.items():
                if subscriber_data.get('token') == token:
                    return subscriber_data
            
            return None
        except Exception as e:
            print(f"Error getting subscriber by token: {e}")
            return None
    
    def update_subscriber_status(self, email: str, subscribed: bool) -> bool:
        """Update subscriber subscription status"""
        if not self.initialized:
            return False
        
        try:
            import hashlib
            from datetime import datetime
            
            email_hash = hashlib.sha256(email.encode()).hexdigest()[:16]
            
            subscribers_ref = db.reference('pulse/subscribers')
            subscriber_ref = subscribers_ref.child(email_hash)
            
            subscriber_ref.update({
                'subscribed': subscribed,
                'unsubscribedAt': datetime.now().isoformat() if not subscribed else None
            })
            
            return True
        except Exception as e:
            print(f"Error updating subscriber status: {e}")
            return False
    
    def get_all_subscribers(self) -> list:
        """Get all subscribers from database"""
        if not self.initialized:
            return []
        
        try:
            subscribers_ref = db.reference('pulse/subscribers')
            all_subscribers = subscribers_ref.get()
            
            if not all_subscribers:
                return []
            
            # Convert to list
            return list(all_subscribers.values())
        except Exception as e:
            print(f"Error getting all subscribers: {e}")
            return []

