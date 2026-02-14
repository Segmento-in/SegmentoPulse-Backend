import asyncio
import warnings
warnings.filterwarnings('ignore')

from app.services.appwrite_db import get_appwrite_db
from app.config import settings
from appwrite.query import Query

async def inspect():
    db = get_appwrite_db()
    if not db.initialized:
        print("DB not initialized")
        return

    print("Fetching one document from Dedicated Magazine Collection...")
    try:
        res = await db.tablesDB.list_rows(
            database_id=settings.APPWRITE_DATABASE_ID,
            collection_id=settings.APPWRITE_MAGAZINE_COLLECTION_ID,
            queries=[Query.limit(1)]
        )
        if res['documents']:
            doc = res['documents'][0]
            print(f"CATEGORY_VALUE: '{doc.get('category')}'")
            print(f"PUBLISHED_AT: '{doc.get('published_at')}'")
        else:
            print("No documents found.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(inspect())
