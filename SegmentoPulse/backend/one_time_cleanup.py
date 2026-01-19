"""
ONE-TIME BULK CLEANUP SCRIPT
==============================

This script will delete ALL articles older than 48 hours from Appwrite.
Use this to clear the 27k article backlog.

WARNING: This will delete articles. Make sure you want to do this!

Run: python one_time_cleanup.py
"""

import asyncio
import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.appwrite_db import get_appwrite_db
from app.config import settings
from appwrite.query import Query


async def bulk_cleanup():
    """Delete ALL old articles (older than 48 hours)"""
    
    print("=" * 80)
    print("🧹 ONE-TIME BULK CLEANUP - Deleting ALL old articles")
    print("=" * 80)
    print("")
    
    appwrite_db = get_appwrite_db()
    
    if not appwrite_db.initialized:
        print("❌ ERROR: Appwrite not initialized!")
        print("Check your .env file has APPWRITE_PROJECT_ID and APPWRITE_API_KEY")
        return
    
    # Calculate cutoff (48 hours ago)
    retention_hours = 48
    cutoff_date = datetime.now() - timedelta(hours=retention_hours)
    cutoff_iso = cutoff_date.isoformat()
    
    print(f"📅 Cutoff Date: {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🗑️  Deleting all articles published before this date...")
    print("")
    
    total_deleted = 0
    batch_number = 0
    
    while True:
        batch_number += 1
        print(f"🔄 Batch #{batch_number}: Querying for old articles...")
        
        try:
            # Query old articles (100 at a time due to Appwrite limits)
            response = appwrite_db.databases.list_documents(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=settings.APPWRITE_COLLECTION_ID,
                queries=[
                    Query.less_than('published_at', cutoff_iso),
                    Query.limit(100)  # Appwrite max
                ]
            )
            
            docs = response['documents']
            
            if len(docs) == 0:
                print("✅ No more old articles found!")
                break
            
            print(f"   Found {len(docs)} articles in this batch")
            print(f"   Deleting...")
            
            # Delete all documents in this batch
            batch_deleted = 0
            for doc in docs:
                try:
                    appwrite_db.databases.delete_document(
                        database_id=settings.APPWRITE_DATABASE_ID,
                        collection_id=settings.APPWRITE_COLLECTION_ID,
                        document_id=doc['$id']
                    )
                    batch_deleted += 1
                except Exception as e:
                    print(f"   ⚠️  Failed to delete {doc['$id']}: {e}")
            
            total_deleted += batch_deleted
            print(f"   ✅ Deleted {batch_deleted} articles")
            print(f"   📊 Total deleted so far: {total_deleted}")
            print("")
            
            # Small delay to avoid rate limiting
            await asyncio.sleep(0.5)
            
        except Exception as e:
            print(f"❌ Error in batch {batch_number}: {e}")
            break
    
    print("=" * 80)
    print(f"🎉 CLEANUP COMPLETE!")
    print(f"🗑️  Total Articles Deleted: {total_deleted}")
    print(f"📦 Batch Operations: {batch_number}")
    print("=" * 80)
    print("")
    print("💡 Next Steps:")
    print("   1. Restart your backend server to start the scheduler")
    print("   2. Scheduler will now delete old articles daily at midnight UTC")
    print("   3. Each daily cleanup will delete up to 100 old articles")
    print("")


if __name__ == "__main__":
    print("")
    print("⚠️  WARNING: This will delete articles older than 48 hours!")
    print("")
    response = input("Are you sure you want to continue? (yes/no): ")
    
    if response.lower() == "yes":
        asyncio.run(bulk_cleanup())
    else:
        print("❌ Cleanup cancelled.")
