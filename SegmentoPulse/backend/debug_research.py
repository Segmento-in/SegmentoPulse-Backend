
import asyncio
from app.services.appwrite_db import AppwriteDatabase
from app.config import settings

async def debug_research():
    print("🔬 Debugging Research Papers Collection...")
    print(f"Collection ID: {settings.APPWRITE_RESEARCH_COLLECTION_ID}")
    
    db = AppwriteDatabase()
    
    # Check if initialized
    if not db.initialized:
        print("❌ DB Not Initialized")
        return

    # Try to fetch ALL docs (no queries)
    try:
        print("1. Fetching raw list (no queries)...")
        response = await db.tablesDB.list_rows(
            database_id=settings.APPWRITE_DATABASE_ID,
            collection_id=settings.APPWRITE_RESEARCH_COLLECTION_ID
        )
        print(f"   Total: {response.get('total')}")
        documents = response.get('documents', [])
        print(f"   Docs Returned: {len(documents)}")
        
        if documents:
            doc = documents[0]
            print("\n2. Sample Document Structure:")
            print(f"   $id: {doc.get('$id')}")
            print(f"   title: {doc.get('title')}")
            print(f"   published_at: {doc.get('published_at')}")
            print(f"   category: {doc.get('category')}")
            print(f"   summary: {str(doc.get('summary'))[:50]}...")
            print("-" * 40)
            print(f"   Full Keys: {list(doc.keys())}")
            
    except Exception as e:
        print(f"❌ Error fetching raw list: {e}")

    # Try to fetch with actual query used in app
    try:
        print("\n3. Fetching with 'get_articles('research')'...")
        articles = await db.get_articles('research')
        print(f"   Articles returned: {len(articles)}")
    except Exception as e:
        print(f"❌ Error in get_articles: {e}")

if __name__ == "__main__":
    asyncio.run(debug_research())
