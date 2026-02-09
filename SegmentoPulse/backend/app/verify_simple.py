
import asyncio
import sys
from app.services.appwrite_db import get_appwrite_db
from app.config import settings
from appwrite.query import Query

async def verify():
    print("STARTING VERIFICATION", flush=True)
    try:
        appwrite_db = get_appwrite_db()
        print(f"DB Initialized: {appwrite_db.initialized}", flush=True)
        
        # Check AI news
        print(f"Checking AI News collection: {settings.APPWRITE_AI_COLLECTION_ID}", flush=True)
        response = appwrite_db.databases.list_documents(
            database_id=settings.APPWRITE_DATABASE_ID,
            collection_id=settings.APPWRITE_AI_COLLECTION_ID,
            queries=[Query.limit(5), Query.order_desc('$createdAt')]
        )
        print(f"Found {len(response['documents'])} docs", flush=True)
        for doc in response['documents']:
             print(f"- {doc.get('title', 'No Title')[:50]}...", flush=True)
             
    except Exception as e:
        print(f"ERROR: {e}", flush=True)
    print("DONE", flush=True)

if __name__ == "__main__":
    asyncio.run(verify())
