"""
Clear All Articles from Appwrite Collection
============================================

Deletes all documents from the articles collection in Appwrite.

IMPORTANT: After running this script, you MUST reset the Bloom Filter
to sync the filter state with the empty database:

    curl -X POST http://localhost:8000/api/admin/bloom-filter/reset

Or in production:
    curl -X POST https://YOUR_SPACE_A_URL/api/admin/bloom-filter/reset

Usage:
    python clear_articles.py
"""

import asyncio
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.getcwd())

from dotenv import load_dotenv
load_dotenv()

from app.services.appwrite_db import get_appwrite_db
from app.config import settings


async def clear_all_articles():
    """Delete all articles from Appwrite collection"""
    print("=" * 70)
    print("🗑️  CLEAR ALL ARTICLES - Appwrite Collection")
    print("=" * 70)
    print()
    
    # Confirmation prompt
    print("⚠️  WARNING: This will DELETE ALL ARTICLES from:")
    print(f"   Database: {settings.APPWRITE_DATABASE_ID}")
    print(f"   Collection: {settings.APPWRITE_COLLECTION_ID}")
    print()
    
    confirm = input("Type 'DELETE ALL' to proceed: ")
    if confirm != "DELETE ALL":
        print("\n❌ Aborted - Collection NOT cleared")
        return
    
    print("\n🔄 Starting deletion process...\n")
    
    try:
        # Initialize Appwrite DB
        db = get_appwrite_db()
        
        if not db.initialized:
            print("❌ ERROR: Appwrite database not initialized")
            print("   Check your .env configuration")
            return
        
        # Get all documents (in batches)
        from appwrite.query import Query
        
        total_deleted = 0
        batch_size = 100
        
        while True:
            # Fetch a batch of documents
            response = db.databases.list_documents(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=settings.APPWRITE_COLLECTION_ID,
                queries=[Query.limit(batch_size)]
            )
            
            documents = response.get('documents', [])
            
            if not documents:
                # No more documents to delete
                break
            
            print(f"📦 Processing batch of {len(documents)} documents...")
            
            # Delete each document in the batch
            for doc in documents:
                try:
                    db.databases.delete_document(
                        database_id=settings.APPWRITE_DATABASE_ID,
                        collection_id=settings.APPWRITE_COLLECTION_ID,
                        document_id=doc['$id']
                    )
                    total_deleted += 1
                    
                    # Progress indicator
                    if total_deleted % 50 == 0:
                        print(f"   ✓ Deleted {total_deleted} articles...")
                
                except Exception as e:
                    print(f"   ⚠️  Failed to delete {doc['$id']}: {e}")
                    continue
        
        print()
        print("=" * 70)
        print(f"✅ SUCCESS: Deleted {total_deleted} articles")
        print("=" * 70)
        print()
        
        # CRITICAL REMINDER
        print("🚨 NEXT STEP REQUIRED - Bloom Filter Sync")
        print("-" * 70)
        print("The Scalable Bloom Filter still has old URLs in memory.")
        print("You MUST reset it to match the empty database state:")
        print()
        print("  1. If running locally:")
        print("     curl -X POST http://localhost:8000/api/admin/bloom-filter/reset")
        print()
        print("  2. If in production (Hugging Face):")
        print("     curl -X POST https://YOUR_SPACE_A_URL/api/admin/bloom-filter/reset")
        print()
        print("Without this step, the system will show 100% duplicates on next fetch!")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(clear_all_articles())
