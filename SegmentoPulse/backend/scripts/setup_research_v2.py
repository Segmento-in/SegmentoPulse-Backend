
import os
import sys
import time
import warnings
from dotenv import load_dotenv
from appwrite.client import Client
from appwrite.services.databases import Databases

# Suppress warnings
warnings.filterwarnings("ignore")

# Load environment variables
load_dotenv()

APPWRITE_ENDPOINT = os.getenv("APPWRITE_ENDPOINT")
APPWRITE_PROJECT_ID = os.getenv("APPWRITE_PROJECT_ID")
APPWRITE_API_KEY = os.getenv("APPWRITE_API_KEY")
APPWRITE_DATABASE_ID = os.getenv("APPWRITE_DATABASE_ID")

if not all([APPWRITE_ENDPOINT, APPWRITE_PROJECT_ID, APPWRITE_API_KEY, APPWRITE_DATABASE_ID]):
    print("❌ Missing environment variables. Check .env file.")
    sys.exit(1)

client = Client()
client.set_endpoint(APPWRITE_ENDPOINT)
client.set_project(APPWRITE_PROJECT_ID)
client.set_key(APPWRITE_API_KEY)

databases = Databases(client)

def wait_for_attribute(collection_id, key):
    """Polls Appwrite until the attribute status is 'available'."""
    print(f"   ⏳ Waiting for attribute '{key}' to be available...", end="", flush=True)
    retries = 0
    max_retries = 60 # 2 minutes
    
    while retries < max_retries:
        try:
            # Check attribute status by listing or getting
            # The SDK doesn't have 'get_attribute' easily for all types, so we list
            response = databases.list_attributes(APPWRITE_DATABASE_ID, collection_id)
            attrs = response.get('attributes', [])
            
            target = next((a for a in attrs if a['key'] == key), None)
            
            if target:
                if target['status'] == 'available':
                    print(" ✅ Ready.")
                    return True
                elif target['status'] == 'failed':
                    print(" ❌ Failed.")
                    return False
            
            time.sleep(2)
            print(".", end="", flush=True)
            retries += 1
        except Exception as e:
            print(f" Error: {e}")
            time.sleep(2)
            retries += 1
            
    print(" ❌ Timeout.")
    return False

def wait_for_index(collection_id, key):
    """Polls Appwrite until the index status is 'available'."""
    print(f"   ⏳ Waiting for index '{key}' to be available...", end="", flush=True)
    retries = 0
    max_retries = 60 
    
    while retries < max_retries:
        try:
            response = databases.list_indexes(APPWRITE_DATABASE_ID, collection_id)
            indexes = response.get('indexes', [])
            
            target = next((i for i in indexes if i['key'] == key), None)
            
            if target:
                if target['status'] == 'available':
                    print(" ✅ Ready.")
                    return True
                elif target['status'] == 'failed':
                    print(" ❌ Failed.")
                    return False
            
            time.sleep(2)
            print(".", end="", flush=True)
            retries += 1
        except Exception as e:
            print(f" Error: {e}")
            time.sleep(2)
            retries += 1

    print(" ❌ Timeout.")
    return False

def setup_v2(collection_id):
    print(f"🚀 Starting Robust Schema Setup for: {collection_id}")
    
    # Define Attributes
    # (func, key, size/opts, required, default, array)
    # Simplified structure for the loop
    
    # 1. Attributes
    print("\n📦 Creating Attributes (Synchronous Mode)...")
    
    try:
        # paper_id
        print(" -> paper_id")
        try:
            databases.create_string_attribute(APPWRITE_DATABASE_ID, collection_id, "paper_id", 100, True)
        except Exception as e:
             if "already exists" not in str(e): print(f"Error: {e}")
        wait_for_attribute(collection_id, "paper_id")

        # title
        print(" -> title")
        try:
            databases.create_string_attribute(APPWRITE_DATABASE_ID, collection_id, "title", 500, True)
        except Exception as e:
             if "already exists" not in str(e): print(f"Error: {e}")
        wait_for_attribute(collection_id, "title")

        # summary
        print(" -> summary")
        try:
            databases.create_string_attribute(APPWRITE_DATABASE_ID, collection_id, "summary", 5000, True)
        except Exception as e:
             if "already exists" not in str(e): print(f"Error: {e}")
        wait_for_attribute(collection_id, "summary")

        # authors
        print(" -> authors")
        try:
            databases.create_string_attribute(APPWRITE_DATABASE_ID, collection_id, "authors", 5000, False)
        except Exception as e:
             if "already exists" not in str(e): print(f"Error: {e}")
        wait_for_attribute(collection_id, "authors")

        # published_at
        print(" -> published_at")
        try:
            databases.create_datetime_attribute(APPWRITE_DATABASE_ID, collection_id, "published_at", True)
        except Exception as e:
             if "already exists" not in str(e): print(f"Error: {e}")
        wait_for_attribute(collection_id, "published_at")

        # pdf_url
        print(" -> pdf_url")
        try:
            databases.create_url_attribute(APPWRITE_DATABASE_ID, collection_id, "pdf_url", True)
        except Exception as e:
             if "already exists" not in str(e): print(f"Error: {e}")
        wait_for_attribute(collection_id, "pdf_url")

        # category
        print(" -> category")
        try:
            databases.create_string_attribute(APPWRITE_DATABASE_ID, collection_id, "category", 50, True)
        except Exception as e:
             if "already exists" not in str(e): print(f"Error: {e}")
        wait_for_attribute(collection_id, "category")
        
        # Stats
        for stat in ["likes", "dislikes", "views"]:
            print(f" -> {stat}")
            try:
                databases.create_integer_attribute(APPWRITE_DATABASE_ID, collection_id, stat, False, 0, 2147483647, 0)
            except Exception as e:
                if "already exists" not in str(e): print(f"Error: {e}")
            wait_for_attribute(collection_id, stat)

    except Exception as e:
        print(f"❌ Critical Error during attribute creation: {e}")
        return

    # 2. Indexes
    print("\n🗂️ Creating Indexes (Now that attributes are ready)...")
    
    # unique_paper_id
    print(" -> unique_paper_id")
    try:
        databases.create_index(APPWRITE_DATABASE_ID, collection_id, "unique_paper_id", "unique", ["paper_id"], ["ASC"])
    except Exception as e:
        if "already exists" not in str(e): print(f"Error: {e}")
    wait_for_index(collection_id, "unique_paper_id")

    # idx_published_at
    print(" -> idx_published_at")
    try:
        databases.create_index(APPWRITE_DATABASE_ID, collection_id, "idx_published_at", "key", ["published_at"], ["DESC"])
    except Exception as e:
        if "already exists" not in str(e): print(f"Error: {e}")
    wait_for_index(collection_id, "idx_published_at")

    # idx_category
    print(" -> idx_category")
    try:
        databases.create_index(APPWRITE_DATABASE_ID, collection_id, "idx_category", "key", ["category"], ["ASC"])
    except Exception as e:
        if "already exists" not in str(e): print(f"Error: {e}")
    wait_for_index(collection_id, "idx_category")

    print("\n✅ Verification Complete: All attributes and indexes are AVAILABLE.")
    print("   You may now run the ingestion script.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/setup_research_v2.py <NEW_COLLECTION_ID>")
        sys.exit(1)
    
    col_id = sys.argv[1]
    setup_v2(col_id)
