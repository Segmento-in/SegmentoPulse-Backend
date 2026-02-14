import asyncio
from app.services.appwrite_db import get_appwrite_db
from app.config import settings

async def verify_research():
    db = get_appwrite_db()
    if not db.initialized:
        print("DB not initialized")
        return

    collection_id = settings.APPWRITE_RESEARCH_COLLECTION_ID
    print(f"Research Collection ID: {collection_id}")

    try:
        # List documents
        docs = await db.tablesDB.list_rows(
            database_id=settings.APPWRITE_DATABASE_ID,
            collection_id=collection_id,
            queries=[]
        )
        print(f"Total Documents: {docs['total']}")
        if docs['documents']:
            print("Sample Document:", docs['documents'][0])
        else:
            print("No documents found.")
            
        # Test get_articles logic
        articles = await db.get_articles('research', limit=5)
        print(f"get_articles('research') returned: {len(articles)}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(verify_research())
