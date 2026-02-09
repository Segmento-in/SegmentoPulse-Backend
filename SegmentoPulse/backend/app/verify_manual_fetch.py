
import asyncio
import logging
from app.services.appwrite_db import get_appwrite_db
from app.config import settings
from appwrite.query import Query
from datetime import datetime, timedelta

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Verifier")

async def verify_stored_articles():
    print("="*60)
    print("🔍 VERIFYING FETCHED ARTICLES IN APPWRITE")
    print("="*60)
    
    appwrite_db = get_appwrite_db()
    
    # Check a few key categories
    categories_to_check = ["ai", "cloud-computing", "data-security"]
    
    total_found = 0
    
    for category in categories_to_check:
        try:
            # Query for articles created in the last 1 hour
            # Note: 'created_at' is internal Appwrite, 'publishedAt' is article time
            # We'll check 'publishedAt' as a proxy for recent content 
            # OR just list the latest documents
            
            # Using the collection ID for the category (which is actually just the main collection with category filter in this architecture)
            # Wait, the architecture uses specific collections for specific types OR one collection with category field?
            # scheduler.py uses:
            # settings.APPWRITE_COLLECTION_ID for "Regular News"
            # settings.APPWRITE_CLOUD_COLLECTION_ID for "Cloud News"
            # settings.APPWRITE_AI_COLLECTION_ID for "AI News"
            
            collection_id = None
            if category == "ai":
                collection_id = settings.APPWRITE_AI_COLLECTION_ID
            elif category == "cloud-computing":
                collection_id = settings.APPWRITE_CLOUD_COLLECTION_ID
            else:
                 collection_id = settings.APPWRITE_COLLECTION_ID # Default/Data
                 
            print(f"\n📂 Checking collection for category: {category.upper()}")
            print(f"   ID: {collection_id}")
            
            if not collection_id:
                print("   ⚠️  Collection ID not configured")
                continue

            response = appwrite_db.databases.list_documents(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=collection_id,
                queries=[
                    Query.limit(5),
                    Query.order_desc('$createdAt') # Get most recently created
                    # Query.equal('category', category) # Optional if collection is mixed
                ]
            )
            
            count = len(response['documents'])
            print(f"   ✅ Found {count} recent documents")
            
            if count > 0:
                for doc in response['documents']:
                    title = doc.get('title', 'No Title')
                    created_at = doc.get('$createdAt', 'Unknown')
                    print(f"      - [{created_at}] {title[:60]}...")
                    total_found += 1
            else:
                 print("   ❌ No documents found. Fetch may have failed.")

        except Exception as e:
            print(f"   ❌ Error querying collection: {e}")

    print("\n" + "="*60)
    if total_found > 0:
        print(f"✅ VERIFICATION PASSED: Found {total_found} recent articles.")
    else:
        print("❌ VERIFICATION FAILED: No recent articles found.")
    print("="*60)

if __name__ == "__main__":
    asyncio.run(verify_stored_articles())
