import asyncio
import os
import sys

# Add the backend directory to sys.path
sys.path.append(os.getcwd())

from app.services.appwrite_db import get_appwrite_db, _safe_get
from app.config import settings

async def verify_migration():
    print("[VERIFY] Starting Appwrite Migration Verification...")
    
    db = get_appwrite_db()
    
    if not db.initialized:
        print("Appwrite not initialized.")
        return

    print(f"[VERIFY] Appwrite Initialized (Project: {settings.APPWRITE_PROJECT_ID})")
    print(f"📡 [VERIFY] Endpoint: {settings.APPWRITE_ENDPOINT}")

    # Test 1: Get Database Stats
    print("\n--- Test 1: Database Stats ---")
    stats = await db.get_database_stats()
    if "error" in stats:
        print(f"[VERIFY] get_database_stats failed: {stats['error']}")
    else:
        print(f"[VERIFY] Total Articles: {stats.get('total_articles')}")
        for cat, count in stats.get('articles_by_category', {}).items():
            if count > 0:
                print(f"   - {cat}: {count} articles")

    # Test 2: List Articles (Modern list_rows)
    print("\n--- Test 2: List Articles (AI) ---")
    articles = await db.get_articles("ai", limit=5)
    print(f"✅ [VERIFY] Found {len(articles)} AI articles")
    for i, art in enumerate(articles, 1):
        print(f"   [{i}] {_safe_get(art, 'title')} ({_safe_get(art, 'source')})")

    # Test 3: List Articles (Cloud)
    print("\n--- Test 3: List Articles (Cloud) ---")
    articles = await db.get_articles("cloud-aws", limit=5)
    print(f"✅ [VERIFY] Found {len(articles)} Cloud articles")
    for i, art in enumerate(articles, 1):
        print(f"   [{i}] {_safe_get(art, 'title')} ({_safe_get(art, 'source')})")

    # Test 4: Generic list_rows Helper
    print("\n--- Test 4: Generic list_rows Helper (Subscribers) ---")
    sub_response = await db.list_rows(
        table_id=settings.APPWRITE_SUBSCRIBERS_COLLECTION_ID,
        queries=[]
    )
    total_subs = sub_response.get('total', 0)
    print("[VERIFY] Total Subscribers: " + str(total_subs))

    # Test 5: get_articles_with_queries (The API Route)
    print("\n--- Test 5: get_articles_with_queries (API Style) ---")
    from appwrite.query import Query
    api_queries = [
        Query.equal('category', 'ai'),
        Query.order_desc('published_at'),
        Query.limit(5)
    ]
    api_articles = await db.get_articles_with_queries(api_queries, category="ai")
    print(f"✅ [VERIFY] Found {len(api_articles)} AI articles via API logic")
    
    if not api_articles:
         print("WARNING: API logic returned 0 articles, while get_articles returned " + str(len(articles)))
         # Try without the sorting index
         print("Retrying without sorting...")
         api_queries_no_sort = [Query.equal('category', 'ai'), Query.limit(5)]
         api_articles_no_sort = await db.get_articles_with_queries(api_queries_no_sort, category="ai")
         print(f"[VERIFY] Found {len(api_articles_no_sort)} articles WITHOUT sort")

    print("\nVerification Script Completed Successfully!")

if __name__ == "__main__":
    asyncio.run(verify_migration())
