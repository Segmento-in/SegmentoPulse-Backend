
import os
import sys
import time
from dotenv import load_dotenv
from appwrite.client import Client
from appwrite.services.databases import Databases

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

def wait_for_attribute(collection_id, key):
    print(f"   ⏳ Waiting for attribute '{key}'...", end="", flush=True)
    for _ in range(30):
        try:
            response = databases.list_attributes(APPWRITE_DATABASE_ID, collection_id)
            attrs = response.get('attributes', [])
            target = next((a for a in attrs if a['key'] == key), None)
            if target and target['status'] == 'available':
                print(" ✅ Ready.")
                return True
            time.sleep(2)
            print(".", end="", flush=True)
        except Exception:
            time.sleep(2)
    print(" ❌ Timeout.")
    return False

def add_missing():
    print(f"🔧 Adding missing attributes to {RESEARCH_COLLECTION_ID}...")
    
    # 1. original_category
    print(" -> original_category")
    try:
        databases.create_string_attribute(APPWRITE_DATABASE_ID, RESEARCH_COLLECTION_ID, "original_category", 50, False)
    except Exception as e:
        if "already exists" not in str(e): print(f"Error: {e}")
    wait_for_attribute(RESEARCH_COLLECTION_ID, "original_category")

    # 2. sub_category
    print(" -> sub_category")
    try:
        databases.create_string_attribute(APPWRITE_DATABASE_ID, RESEARCH_COLLECTION_ID, "sub_category", 100, False)
    except Exception as e:
        if "already exists" not in str(e): print(f"Error: {e}")
    wait_for_attribute(RESEARCH_COLLECTION_ID, "sub_category")

    # 3. source
    print(" -> source")
    try:
        databases.create_string_attribute(APPWRITE_DATABASE_ID, RESEARCH_COLLECTION_ID, "source", 50, False)
    except Exception as e:
        if "already exists" not in str(e): print(f"Error: {e}")
    wait_for_attribute(RESEARCH_COLLECTION_ID, "source")

    print("\n✅ Schema Update Complete.")

if __name__ == "__main__":
    add_missing()
