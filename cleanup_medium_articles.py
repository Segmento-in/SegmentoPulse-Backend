import asyncio
import sys
import os
from appwrite.query import Query

# Add backend directory to path
sys.path.append(os.path.join(os.getcwd(), 'SegmentoPulse', 'backend'))

# Load env vars explicitly BEFORE imports
from dotenv import load_dotenv
load_dotenv(os.path.join(os.getcwd(), 'SegmentoPulse', 'backend', '.env'))

# Fix Pydantic parsing error for CORS_ORIGINS if missing/empty
if not os.environ.get("CORS_ORIGINS") or os.environ.get("CORS_ORIGINS") == "":
    os.environ["CORS_ORIGINS"] = '["*"]'

from app.services.appwrite_db import get_appwrite_db
from app.config import settings

async def cleanup():
    print("Initializing Database connection...")
    
    # Debug settings
    print(f"Env Path: {os.path.join(os.getcwd(), 'SegmentoPulse', 'backend', '.env')}")
    print(f"APPWRITE_PROJECT_ID: {settings.APPWRITE_PROJECT_ID}")
    
    db_service = get_appwrite_db()
    
    if not db_service.initialized:
        print("Database not initialized. Check credentials.")
        return

    print("Fetching articles with source='Medium'...")
    
    current_offset = 0
    updated_count = 0
    total_processed = 0
    
    while True:
        # Appwrite pagination limit is usually 100 max for listDocument?? 
        # Actually it can be higher but let's stick to 100.
        try:
            response = db_service.databases.list_documents(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=settings.APPWRITE_COLLECTION_ID,
                queries=[
                    Query.equal('source', 'Medium'),
                    Query.limit(100),
                    Query.offset(current_offset)
                ]
            )
            
            documents = response['documents']
            total_in_db = response['total']
            
            if not documents:
                break
                
            print(f"Processing batch: {len(documents)} items (Offset: {current_offset}, Total found: {total_in_db})")
            
            batch_updates = 0
            for doc in documents:
                if doc.get('category') != 'medium-article':
                    try:
                        print(f"Updating: '{doc.get('title')[:30]}...' (Category: {doc.get('category')} -> medium-article)")
                        db_service.databases.update_document(
                            database_id=settings.APPWRITE_DATABASE_ID,
                            collection_id=settings.APPWRITE_COLLECTION_ID,
                            document_id=doc['$id'],
                            data={'category': 'medium-article'}
                        )
                        updated_count += 1
                        batch_updates += 1
                    except Exception as e:
                        print(f"Error updating doc {doc['$id']}: {e}")
            
            total_processed += len(documents)
            
            # Since we iterate through all, and we are updating them, the query for source='Medium' still returns them.
            # So standard pagination (offset) is correct.
            current_offset += 100
            
            if total_processed >= total_in_db:
                break
                
        except Exception as e:
            print(f"Error fetching documents: {e}")
            break
            
    print("-" * 50)
    print(f"Cleanup Complete!")
    print(f"Total Medium Articles Scanned: {total_processed}")
    print(f"Total Updated to 'medium-article': {updated_count}")

if __name__ == "__main__":
    asyncio.run(cleanup())
