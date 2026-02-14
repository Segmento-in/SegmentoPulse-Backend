import os
import sys
import warnings
from dotenv import load_dotenv
from appwrite.client import Client
from appwrite.services.databases import Databases

# Suppress deprecation warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Load environment variables
load_dotenv()

# Configuration
APPWRITE_ENDPOINT = os.getenv("APPWRITE_ENDPOINT", "https://cloud.appwrite.io/v1")
APPWRITE_PROJECT_ID = os.getenv("APPWRITE_PROJECT_ID")
APPWRITE_API_KEY = os.getenv("APPWRITE_API_KEY")
APPWRITE_DATABASE_ID = os.getenv("APPWRITE_DATABASE_ID")
RESEARCH_COLLECTION_ID = os.getenv("APPWRITE_RESEARCH_COLLECTION_ID", "69845c19002c864d4d3f")

if not all([APPWRITE_ENDPOINT, APPWRITE_PROJECT_ID, APPWRITE_API_KEY, APPWRITE_DATABASE_ID]):
    print("❌ Missing environment variables. Please check .env file.")
    sys.exit(1)

# Initialize Appwrite Client
client = Client()
client.set_endpoint(APPWRITE_ENDPOINT)
client.set_project(APPWRITE_PROJECT_ID)
client.set_key(APPWRITE_API_KEY)

databases = Databases(client)

def init_research_schema():
    print(f"🔬 Initializing Schema for Collection: {RESEARCH_COLLECTION_ID}")
    
    # 1. Verify Collection Exists
    try:
        databases.get_collection(APPWRITE_DATABASE_ID, RESEARCH_COLLECTION_ID)
        print("✅ Collection exists.")
    except Exception as e:
        print(f"❌ Collection not found: {e}")
        return

    # 2. Define Required Attributes
    required_attributes = [
        {"key": "paper_id", "type": "string", "size": 255, "required": True},
        {"key": "title", "type": "string", "size": 500, "required": True},
        {"key": "summary", "type": "string", "size": 5000, "required": False}, # Abstract
        {"key": "authors", "type": "string", "size": 5000, "required": False},
        {"key": "published_at", "type": "datetime", "required": True},
        {"key": "pdf_url", "type": "url", "required": True},
        {"key": "category", "type": "string", "size": 255, "required": True}, # Internal ID (research-ai)
        {"key": "sub_category", "type": "string", "size": 255, "required": False}, # New strict sub-category
        {"key": "original_category", "type": "string", "size": 255, "required": True}, # ArXiv ID (cs.AI)
        {"key": "likes", "type": "integer", "required": False, "default": 0},
        {"key": "dislike", "type": "integer", "required": False, "default": 0}, # Note: 'dislike' singular to match news
        {"key": "views", "type": "integer", "required": False, "default": 0},
    ]

    # 3. Check and Create Attributes
    import time
    
    try:
        attrs = databases.list_attributes(APPWRITE_DATABASE_ID, RESEARCH_COLLECTION_ID)
        existing_attributes = {attr['key']: attr for attr in attrs['attributes']}
        existing_keys = list(existing_attributes.keys())
        print(f"Existing Attributes: {existing_keys}")
        
        for attr in required_attributes:
            key = attr['key']
            if key not in existing_keys:
                print(f"⚙️ Creating attribute: {key}...")
                try:
                    if attr['type'] == "string":
                        databases.create_string_attribute(
                            database_id=APPWRITE_DATABASE_ID, 
                            collection_id=RESEARCH_COLLECTION_ID, 
                            key=key, 
                            size=attr['size'], 
                            required=attr['required'], 
                            default=attr.get('default')
                        )
                    elif attr['type'] == "datetime":
                        databases.create_datetime_attribute(
                            database_id=APPWRITE_DATABASE_ID, 
                            collection_id=RESEARCH_COLLECTION_ID, 
                            key=key, 
                            required=attr['required'], 
                            default=attr.get('default')
                        )
                    elif attr['type'] == "url":
                        databases.create_url_attribute(
                            database_id=APPWRITE_DATABASE_ID, 
                            collection_id=RESEARCH_COLLECTION_ID, 
                            key=key, 
                            required=attr['required'], 
                            default=attr.get('default')
                        )
                    elif attr['type'] == "integer":
                        databases.create_integer_attribute(
                            database_id=APPWRITE_DATABASE_ID, 
                            collection_id=RESEARCH_COLLECTION_ID, 
                            key=key, 
                            required=attr['required'], 
                            min=0, 
                            max=None, 
                            default=attr.get('default')
                        )
                    print(f"   ✅ Request sent for: {key}")
                except Exception as attr_error:
                    print(f"   ❌ Failed to create {key}: {attr_error}")
            else:
                print(f"   🔹 Exists: {key}")
        
        # 3.5 WAIT FOR ATTRIBUTES TO BE AVAILABLE
        print("\n⏳ Waiting for attributes to become 'available'...")
        pending_attrs = [attr['key'] for attr in required_attributes]
        max_retries = 30 # 30 * 2 = 60 seconds
        
        for key in pending_attrs:
            for attempt in range(max_retries):
                try:
                    # Generic get_attribute doesn't exist mainly, need typed check or list loop
                    # robust way: list all and check specific
                    curr_attrs = databases.list_attributes(APPWRITE_DATABASE_ID, RESEARCH_COLLECTION_ID)['attributes']
                    target = next((a for a in curr_attrs if a['key'] == key), None)
                    
                    if target and target['status'] == 'available':
                        print(f"   ✅ {key} is available.")
                        break
                    elif target and target['status'] == 'failed':
                        print(f"   ❌ {key} creation FAILED in Appwrite.")
                        break
                    else:
                         if attempt % 5 == 0:
                            print(f"   ... waiting for {key} (attempt {attempt+1}/{max_retries})")
                         time.sleep(2)
                except Exception as e:
                    print(f"Error checking status for {key}: {e}")
                    time.sleep(2)
            else:
                 print(f"   ⚠️ Timeout waiting for {key} to be available.")

        # 4. Create Index on paper_id (if not exists)
        # Only try to create index if paper_id is available
        print("\n⚙️ Checking/Creating index on paper_id...")
        try:
            databases.create_index(
                database_id=APPWRITE_DATABASE_ID,
                collection_id=RESEARCH_COLLECTION_ID,
                key="unique_paper_id",
                type="unique",
                attributes=["paper_id"]
            )
            print("   ✅ Index created.")
        except Exception as e:
            # If error contains "already exists" or 409, it's fine
            if "already exists" in str(e) or "409" in str(e):
                print("   🔹 Index already exists.")
            elif "attribute not found" in str(e).lower() or "processing" in str(e).lower():
                 print(f"   ❌ Index creation failed: Attributes still processing or missing. ({e})")
            else:
                print(f"   ⚠️  Could not create index: {e}")

    except Exception as e:
        print(f"❌ Error during schema initialization: {e}")

if __name__ == "__main__":
    init_research_schema()
