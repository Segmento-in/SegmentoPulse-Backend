import asyncio
import warnings
# Filter warnings
warnings.filterwarnings('ignore')

from app.services.appwrite_db import get_appwrite_db
from app.config import settings
from appwrite.query import Query

async def check_counts():
    db = get_appwrite_db()
    if not db.initialized:
        print("DB not initialized")
        return

    print("\n" + "="*50)
    print("DEBUG: MAGAZINE COLLECTION COUNTS")
    print("="*50)

    try:
        main_mag = await db.tablesDB.list_rows(
            database_id=settings.APPWRITE_DATABASE_ID,
            collection_id=settings.APPWRITE_COLLECTION_ID,
            queries=[Query.equal('category', 'magazines'), Query.limit(1)]
        )
        ded_mag = await db.tablesDB.list_rows(
            database_id=settings.APPWRITE_DATABASE_ID,
            collection_id=settings.APPWRITE_MAGAZINE_COLLECTION_ID,
            queries=[Query.limit(1)]
        )
        print(f"Magazines (Main Collection):      {main_mag['total']}")
        print(f"Magazines (Dedicated Collection): {ded_mag['total']}")
    except Exception as e:
        print(f"Error checking magazines: {e}")

    print("="*50 + "\n")

if __name__ == "__main__":
    asyncio.run(check_counts())
