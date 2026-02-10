import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.models import Article
from app.services.appwrite_db import AppwriteDatabase
from app.config import settings

# MOCK SETTINGS FOR TEST
# We need to simulate that we are saving to the CLOUD collection
settings.APPWRITE_CLOUD_COLLECTION_ID = "mock_cloud_collection_id"
settings.APPWRITE_DATABASE_ID = "mock_db_id"

def test_cloud_schema_mapping():
    print("🧪 Testing Cloud Collection Schema Mapping...")
    
    # 1. Create a dummy AppwriteDatabase instance
    # We will mock the tablesDB.create_row method to intercept the data
    db = AppwriteDatabase()
    
    # Mock the internal tablesDB.create_row to just print the data
    class MockDB:
        def create_row(self, database_id, collection_id, document_id, data):
            print(f"\n📝 INTERCEPTED WRITE to {collection_id}:")
            print(f"   IDs: {document_id}")
            print(f"   Keys: {list(data.keys())}")
            
            # CHECK 1: 'image' should exist, 'image_url' should NOT
            if 'image' in data and 'image_url' not in data:
                print("   ✅ PASS: 'image' used instead of 'image_url'")
            else:
                 print(f"   ❌ FAIL: Keys are wrong! {list(data.keys())}")
            
            # CHECK 2: 'publishedAt' should exist, 'published_at' should NOT
            if 'publishedAt' in data and 'published_at' not in data:
                 print("   ✅ PASS: 'publishedAt' used instead of 'published_at'")
            else:
                 print(f"   ❌ FAIL: Date keys are wrong! {list(data.keys())}")
                 
            return {'$id': document_id}
            
    db.tablesDB = MockDB()
    db.initialized = True # Force init for test
    
    # 2. Create a test article (Cloud Category)
    # This category 'cloud-aws' triggers the get_collection_id -> cloud collection logic
    article = {
        'title': 'Test Cloud Article',
        'url': 'https://aws.amazon.com/test',
        'image_url': 'https://aws.amazon.com/image.jpg', # New schema key
        'published_at': datetime.now(), # New schema key
        'source': 'AWS Blog',
        'category': 'cloud-aws' # CRITICAL: This routes to Cloud Collection
    }
    
    print(f"\n📋 Input Article Keys: {list(article.keys())}")
    
    # 3. Import asyncio and run save_articles
    import asyncio
    
    # Patch get_collection_id to return our mock ID
    original_get_collection_id = db.get_collection_id
    db.get_collection_id = lambda cat: settings.APPWRITE_CLOUD_COLLECTION_ID
    
    try:
        asyncio.run(db.save_articles([article]))
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Restore (good practice, though script ends)
        db.get_collection_id = original_get_collection_id

if __name__ == "__main__":
    test_cloud_schema_mapping()
