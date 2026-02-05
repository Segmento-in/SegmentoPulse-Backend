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
            # Priority 1: Try initializing from Environment Variable (JSON String)
            # This is common for Hugging Face Spaces / Cloud deployments
            if hasattr(settings, 'FIREBASE_CREDENTIALS') and settings.FIREBASE_CREDENTIALS:
                try:
                    import json
                    cred_dict = json.loads(settings.FIREBASE_CREDENTIALS)
                    cred = credentials.Certificate(cred_dict)
                    
                    # check if already initialized to prevent error
                    if not firebase_admin._apps:
                        firebase_admin.initialize_app(cred, {
                            'databaseURL': settings.FIREBASE_DATABASE_URL if hasattr(settings, 'FIREBASE_DATABASE_URL') else ''
                        })
                        
                    self.db_ref = db.reference('pulse/article_views')
                    self.initialized = True
                    print("Firebase initialized successfully from Environment Variable")
                    return
                except Exception as json_err:
                    print(f"Error initializing from FIREBASE_CREDENTIALS: {json_err}")
                    # Fallthrough to file check if this fails

            # Priority 2: Check if credentials file exists
            import os
            creds_path = settings.FIREBASE_CREDENTIALS_PATH if hasattr(settings, 'FIREBASE_CREDENTIALS_PATH') else './firebase-credentials.json'
            
            if not os.path.exists(creds_path):
                print(f"Firebase credentials file not found at '{creds_path}' - Firebase features disabled")
                self.initialized = False
                return
            
            if not firebase_admin._apps:
                cred = credentials.Certificate(creds_path)
                firebase_admin.initialize_app(cred, {
                    'databaseURL': settings.FIREBASE_DATABASE_URL if hasattr(settings, 'FIREBASE_DATABASE_URL') else ''
                })
            
            self.db_ref = db.reference('pulse/article_views')
            self.initialized = True
            print("Firebase initialized successfully")
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
        """
        Add or Update subscriber in database
        Now supports merging 'subscriptions' for multi-preference support
        """
        if not self.initialized:
            return False
        
        try:
            # Use email hash as key for privacy
            import hashlib
            email_hash = hashlib.sha256(email.encode()).hexdigest()[:16]
            
            subscribers_ref = db.reference('pulse/subscribers')
            subscriber_ref = subscribers_ref.child(email_hash)
            
            # Check if subscriber already exists
            existing_data = subscriber_ref.get()
            
            if existing_data:
                # MERGE LOGIC: Keep existing subscriptions, add new one
                new_pref = subscriber_data.get('preference')
                
                # Initialize subscriptions dict if missing (migration)
                existing_subs = existing_data.get('subscriptions', {})
                
                # If legacy 'preference' exists, map it to subscriptions
                if not existing_subs and existing_data.get('preference'):
                    existing_subs[existing_data.get('preference')] = True
                
                # Add the new preference
                if new_pref:
                    existing_subs[new_pref] = True
                
                # Update the data
                updates = {
                    'subscriptions': existing_subs,
                    'subscribed': True, # Re-activate if was unsubscribed
                    'unsubscribedAt': None,
                    'lastUpdated': datetime.now().isoformat()
                }
                # Update specific fields without overwriting everything
                subscriber_ref.update(updates)
                return True
            else:
                # NEW SUBSCRIBER
                # Convert single 'preference' to 'subscriptions' map
                pref = subscriber_data.get('preference', 'Weekly')
                subscriber_data['subscriptions'] = {pref: True}
                
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
        """Get all subscribers from database with migration support"""
        if not self.initialized:
            return []
        
        try:
            subscribers_ref = db.reference('pulse/subscribers')
            all_subscribers = subscribers_ref.get()
            
            if not all_subscribers:
                return []
            
            # Convert to list and add default preference for legacy subscribers
            subscribers = []
            for subscriber_data in all_subscribers.values():
                # Migration: Add default 'Weekly' preference for existing subscribers
                if 'preference' not in subscriber_data:
                    subscriber_data['preference'] = 'Weekly'
                subscribers.append(subscriber_data)
            
            return subscribers
        except Exception as e:
            print(f"Error getting all subscribers: {e}")
            return []
    
    def get_subscribers_by_preference(self, preference: str) -> list:
        """
        Get all subscribers filtered by newsletter preference
        Supports NEW 'subscriptions' map and LEGACY 'preference' string
        """
        if not self.initialized:
            return []
        
        try:
            subscribers_ref = db.reference('pulse/subscribers')
            
            # NOTE: Firebase RTDB only supports sorting/filtering on ONE key.
            # We can't efficiently query "subscriptions/Morning == True" AND "subscriptions/Afternoon == True" effectively without multiple indexes.
            # Given the dataset size (< 10k), fetching all and filtering in memory is acceptable for now.
            # Later we can add `.indexOn: ["subscriptions/Morning", "subscriptions/Afternoon"]` to rules.
            
            # Fetch all subscribers (simpler and safer for mixed schema)
            all_subscribers = subscribers_ref.get()
            
            if not all_subscribers:
                return []
            
            matched_subscribers = []
            
            for sub_id, data in all_subscribers.items():
                if not data.get('subscribed', True):
                    continue
                
                # Check NEW Schema: subscriptions = {'Morning': True, 'Weekly': False}
                subscriptions = data.get('subscriptions', {})
                if subscriptions and isinstance(subscriptions, dict):
                    if subscriptions.get(preference, False):
                        matched_subscribers.append(data)
                        continue
                
                # Check LEGACY Schema: preference = "Morning"
                # Only check if subscriptions dict was missing or empty
                if not subscriptions and data.get('preference') == preference:
                     matched_subscribers.append(data)
            
            # FAIRNESS FIX: Sort by lastSentAt
            matched_subscribers.sort(
                key=lambda x: x.get('lastSentAt', '1970-01-01T00:00:00Z')
            )
            
            return matched_subscribers
            
        except Exception as e:
            print(f"Error getting subscribers by preference: {e}")
            return []
    
    def update_preference(self, email: str, preference: str) -> bool:
        """Update subscriber's newsletter preference"""
        if not self.initialized:
            return False
        
        try:
            import hashlib
            email_hash = hashlib.sha256(email.encode()).hexdigest()[:16]
            
            subscribers_ref = db.reference('pulse/subscribers')
            subscriber_ref = subscribers_ref.child(email_hash)
            
            subscriber_ref.update({'preference': preference})
            return True
        except Exception as e:
            print(f"Error updating preference: {e}")
            return False
    
            # Store in UTC format
            utc_now = datetime.now(timezone.utc).isoformat()
            subscriber_ref.update({'lastSentAt': utc_now})
            
            return True
        except Exception as e:
            print(f"Error updating last sent timestamp: {e}")
            return False

    def update_subscription_status(self, email: str, preference: str, is_active: bool) -> bool:
        """
        Update a SPECIFIC subscription (Granular Unsubscribe)
        e.g. Set 'Morning' to False, but keep 'Weekly' True
        """
        if not self.initialized:
            return False

        try:
            import hashlib
            email_hash = hashlib.sha256(email.encode()).hexdigest()[:16]
            subscribers_ref = db.reference('pulse/subscribers')
            subscriber_ref = subscribers_ref.child(email_hash)
            
            data = subscriber_ref.get()
            if not data:
                return False
                
            subscriptions = data.get('subscriptions', {})
            
            # Migration check: if no subscriptions dict, populate from legacy 'preference'
            if not subscriptions and data.get('preference'):
                subscriptions[data.get('preference')] = True
            
            # Update the specific preference
            subscriptions[preference] = is_active
            
            updates = {'subscriptions': subscriptions}
            
            # Check if ALL subscriptions are now false
            any_active = any(subscriptions.values())
            if not any_active:
                updates['subscribed'] = False
                updates['unsubscribedAt'] = datetime.now().isoformat()
            else:
                 # Ensure global subscribed is True if at least one is active
                updates['subscribed'] = True
                updates['unsubscribedAt'] = None
                
            subscriber_ref.update(updates)
            return True
            
        except Exception as e:
            print(f"Error updating subscription status: {e}")
            return False


# Singleton instance
_firebase_service = None

def get_firebase_service() -> FirebaseService:
    """Get or create Firebase service singleton instance"""
    global _firebase_service
    if _firebase_service is None:
        _firebase_service = FirebaseService()
    return _firebase_service

