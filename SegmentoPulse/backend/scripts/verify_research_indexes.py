
import os
import sys
import asyncio
from dotenv import load_dotenv
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query

load_dotenv()

APPWRITE_ENDPOINT = os.getenv("APPWRITE_ENDPOINT")
APPWRITE_PROJECT_ID = os.getenv("APPWRITE_PROJECT_ID")
APPWRITE_API_KEY = os.getenv("APPWRITE_API_KEY")
APPWRITE_DATABASE_ID = os.getenv("APPWRITE_DATABASE_ID")
RESEARCH_COLLECTION_ID = os.getenv("APPWRITE_RESEARCH_COLLECTION_ID")

client = Client()
client.set_endpoint(APPWRITE_ENDPOINT)
client.set_project(APPWRITE_PROJECT_ID)
client.set_key(APPWRITE_API_KEY)

databases = Databases(client)

async def probe_index(attr, query_type):
    print(f"🕵️ Probing index for '{attr}'...")
    try:
        # We don't need async here actually, the python SDK is sync unless we use the async client, 
        # but my environment context shows async usage in services. 
        # The standard appwrite python SDK is synchronous (CHECK: 'appwrite' package).
        # Wait, the code I viewed in `research_aggregator.py` imports `arxiv` but uses `app.services.appwrite_db` handling stuff.
        # Standard `appwrite` package on PyPI is synchronous.
        # But `appwrite_db.py` wraps it? No, checking `appwrite_db.py`: `from appwrite.services.databases import Databases`.
        # Wait, step 768 `appwrite_db.py` has `await self.tablesDB.list_rows`.
        # If the user is using `appwrite` python package < 4.0 it might be sync, or if they operate it differently.
        # BUT, to be safe and simple, I will use the SYNC calls here as it's a script.
        
        # Testing Sort
        q = [Query.order_desc(attr), Query.limit(1)]
        databases.list_documents(
            database_id=APPWRITE_DATABASE_ID,
            collection_id=RESEARCH_COLLECTION_ID,
            queries=q
        )
        print(f"   ✅ Index '{attr}' appears ACTIVE (Sort query worked).")
        return True
    except Exception as e:
        if "Index not found" in str(e) or "missing index" in str(e).lower():
            print(f"   ❌ Index '{attr}' is MISSING or INVALID.")
            print(f"      Error: {e}")
            return False
        else:
            print(f"   ⚠️  Query failed (Unrelated?): {e}")
            return False

def verify():
    print(f"🧪 Verifying Indexes for {RESEARCH_COLLECTION_ID}")
    
    # 1. Check 'published_at' (Sortable)
    probe_index("published_at", "order_desc")
    
    # 2. Check 'category' (Filterable/Sortable)
    # To check filter index, we try to filter.
    print(f"🕵️ Probing index for 'category' (Filter)...")
    try:
        databases.list_documents(
             database_id=APPWRITE_DATABASE_ID,
             collection_id=RESEARCH_COLLECTION_ID,
             queries=[Query.equal("category", "research-ai"), Query.limit(1)]
        )
        print(f"   ✅ Index 'category' appears ACTIVE (Filter query worked).")
    except Exception as e:
        print(f"   ❌ Index 'category' is MISSING/INVALID: {e}")

    # 3. Check 'paper_id' (Unique)
    # Hard to test unique without creating, but we can try to filter by it if it's a key.
    # Usually unique indexes also allow filtering.
    print(f"🕵️ Probing index for 'paper_id' (Filter)...")
    try:
        databases.list_documents(
             database_id=APPWRITE_DATABASE_ID,
             collection_id=RESEARCH_COLLECTION_ID,
             queries=[Query.equal("paper_id", "test"), Query.limit(1)]
        )
        print(f"   ✅ Index 'paper_id' appears ACTIVE (Filter query worked).")
    except Exception as e:
        print(f"   ❌ Index 'paper_id' is MISSING/INVALID: {e}")

if __name__ == "__main__":
    verify()
