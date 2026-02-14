
import asyncio
import os
import sys
import httpx
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from app.config import settings
from app.services.appwrite_db import get_appwrite_db
from app.services.research_aggregator import ResearchAggregator

load_dotenv()

async def test_flow():
    print("🧪 Starting End-to-End Research Verification...")
    print(f"   Target Collection: {settings.APPWRITE_RESEARCH_COLLECTION_ID}")
    
    # 1. Simulate Ingestion (Fetch 1 paper from ArXiv)
    print("\n1. Testing Ingestion (Dry Run logic)...")
    agg = ResearchAggregator()
    
    # We'll mock the fetch to avoid hitting ArXiv rate limits or waiting
    # We just want to test the _save_paper logic with the new schema.
    
    test_paper = {
        "paper_id": "test.12345",
        "title": "Test Paper for Verification",
        "summary": "This is a test summary for verification purposes.",
        "authors": ["Test Author"],
        "published_at": "2023-10-27T00:00:00+00:00",
        "pdf_url": "http://arxiv.org/pdf/test.12345",
        "category": "research-ai",
        "original_category": "cs.AI",
        "sub_category": "Artificial Intelligence", # Friendly name
        "source": "arXiv"
    }
    
    print(f"   Attempting to save test paper: {test_paper['paper_id']}")
    saved = await agg._save_paper(test_paper)
    
    if saved:
        print("   ✅ Ingestion Successful (Paper Saved/Created).")
    else:
        print("   ⚠️  Ingestion Skipped (Likely Duplicate or Error).")

    # 2. Verify Data in DB
    print("\n2. Verifying Data in Appwrite...")
    appwrite = get_appwrite_db()
    try:
        # We need to find the document ID of the paper we just saved (or existing one)
        # We can query by paper_id
        from appwrite.query import Query
        response = await appwrite.tablesDB.list_rows(
            database_id=settings.APPWRITE_DATABASE_ID,
            collection_id=settings.APPWRITE_RESEARCH_COLLECTION_ID,
            queries=[Query.equal("paper_id", "test.12345")]
        )
        
        if response['total'] == 0:
            print("   ❌ Error: Paper not found in DB after ingestion.")
            return

        doc = response['documents'][0]
        doc_id = doc['$id']
        print(f"   ✅ Paper found! Doc ID: {doc_id}")
        print(f"   Current Stats -> Likes: {doc.get('likes')}, Views: {doc.get('views')}")
        
    except Exception as e:
        print(f"   ❌ Error verifying data: {e}")
        return

    # 3. Test Engagement (Like)
    # This simulates what the Frontend does (POST /api/engagement/articles/{id}/like)
    # We will use the internal router function logic or just call DB update manually to verify schema.
    # Calling DB manually is better to verify the *schema* accepts updates.
    
    print("\n3. Testing Stats Update (Simulation)...")
    try:
        current_likes = doc.get('likes', 0) or 0
        new_likes = current_likes + 1
        
        updated_doc = await appwrite.tablesDB.update_row(
            database_id=settings.APPWRITE_DATABASE_ID,
            collection_id=settings.APPWRITE_RESEARCH_COLLECTION_ID,
            document_id=doc_id,
            data={"likes": new_likes}
        )
        
        if updated_doc['likes'] == new_likes:
            print(f"   ✅ Stats Update Successful! Likes: {current_likes} -> {updated_doc['likes']}")
        else:
            print(f"   ❌ Stats Update Mismatch: Expected {new_likes}, got {updated_doc['likes']}")

    except Exception as e:
        print(f"   ❌ Error updating stats: {e}")
        
    print("\n🎉 Verification Complete!")

if __name__ == "__main__":
    asyncio.run(test_flow())
