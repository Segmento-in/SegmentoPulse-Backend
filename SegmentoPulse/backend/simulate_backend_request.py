import asyncio
import warnings
warnings.filterwarnings('ignore')

from app.services.appwrite_db import get_appwrite_db
from app.config import settings
from appwrite.query import Query

async def simulate():
    db = get_appwrite_db()
    
    category = 'magazines'
    queries = [
        Query.equal('category', category),
        Query.order_desc('published_at'),
        Query.limit(21)
    ]
    
    # Needs to match what news.py receives/sends
    
    print(f"Simulating request for category: {category}")
    
    # 1. Check strict routing
    target_id = db.get_collection_id(category)
    print(f"Target Collection ID: {target_id} (Expected for strict: {settings.APPWRITE_MAGAZINE_COLLECTION_ID})")
    
    if target_id == settings.APPWRITE_MAGAZINE_COLLECTION_ID:
        print("Strict routing IS active (if you reverted it in code)")
    else:
        print("Strict routing IS NOT active (using main)")

    # 2. Execute Query
    try:
        articles = await db.get_articles_with_queries(queries, category=category)
        print(f"COUNT_ARTICLES: {len(articles)}")
    except Exception as e:
        print(f"ERROR: {e}")

'''
NOTE: You need to revert `appwrite_db.py` change 
BEFORE running this to test strict routing!
Currently it should use MAIN collection and likely return 0.
'''

if __name__ == "__main__":
    asyncio.run(simulate())
