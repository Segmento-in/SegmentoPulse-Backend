import asyncio
import os
import sys

# Add parent directory to path to import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings
from app.services.appwrite_db import get_appwrite_db

async def add_text_summary_attribute():
    """
    Adds the 'text_summary' attribute to all article collections.
    """
    print("="*60)
    print("📜 Adding 'text_summary' attribute to Appwrite Collections")
    print("="*60)
    
    appwrite = get_appwrite_db()
    
    if not appwrite.initialized:
        print("❌ Appwrite not initialized. Check credentials.")
        return

    # List of collections to update
    collections = [
        settings.APPWRITE_COLLECTION_ID,         # Articles
        settings.APPWRITE_CLOUD_COLLECTION_ID,   # Cloud Articles
        settings.APPWRITE_AI_COLLECTION_ID,      # AI
        settings.APPWRITE_DATA_COLLECTION_ID,    # Data
        settings.APPWRITE_MAGAZINE_COLLECTION_ID,# Magazine
        settings.APPWRITE_MEDIUM_COLLECTION_ID   # Medium
    ]
    
    for col_id in collections:
        if not col_id or col_id == "change_me":
            continue
            
        print(f"Checking collection: {col_id}...")
        
        try:
            # Create text_summary attribute (String, 5000 chars, not required)
            try:
                appwrite.databases.create_string_attribute(
                    database_id=settings.APPWRITE_DATABASE_ID,
                    collection_id=col_id,
                    key="text_summary",
                    size=5000,
                    required=False,
                    default=None
                )
                print(f"✅ Added 'text_summary' to {col_id}")
            except Exception as e:
                if "Attribute already exists" in str(e) or "409" in str(e):
                     print(f"⚠️  Attribute 'text_summary' already exists in {col_id}")
                else:
                    print(f"❌ Failed to add attribute to {col_id}: {e}")

        except Exception as e:
            print(f"❌ Error processing collection {col_id}: {e}")

    print("\n🎉 Schema update complete!")

if __name__ == "__main__":
    asyncio.run(add_text_summary_attribute())
