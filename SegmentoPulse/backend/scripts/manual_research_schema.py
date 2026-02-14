
import os
import sys
import warnings
from dotenv import load_dotenv
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.id import ID

# Suppress warnings
warnings.filterwarnings("ignore")

# Load environment variables
load_dotenv()

APPWRITE_ENDPOINT = os.getenv("APPWRITE_ENDPOINT")
APPWRITE_PROJECT_ID = os.getenv("APPWRITE_PROJECT_ID")
APPWRITE_API_KEY = os.getenv("APPWRITE_API_KEY")
APPWRITE_DATABASE_ID = os.getenv("APPWRITE_DATABASE_ID")
RESEARCH_COLLECTION_ID = os.getenv("APPWRITE_RESEARCH_COLLECTION_ID")

if not all([APPWRITE_ENDPOINT, APPWRITE_PROJECT_ID, APPWRITE_API_KEY, APPWRITE_DATABASE_ID, RESEARCH_COLLECTION_ID]):
    print("❌ Missing environment variables. Check .env file.")
    sys.exit(1)

client = Client()
client.set_endpoint(APPWRITE_ENDPOINT)
client.set_project(APPWRITE_PROJECT_ID)
client.set_key(APPWRITE_API_KEY)

databases = Databases(client)

def create_attribute_safe(func, *args, **kwargs):
    try:
        func(*args, **kwargs)
        print(f"   ✅ Created attribute/index.")
    except Exception as e:
        if "already exists" in str(e):
            print(f"   🔹 Already exists.")
        else:
            print(f"   ❌ Error: {e}")

def manual_setup():
    print(f"🔬 Manual Schema Setup for Collection: {RESEARCH_COLLECTION_ID}")
    
    # 1. Attributes
    print("\n1. creating Attributes...")
    
    print(" - paper_id (string, 100 char, required)")
    create_attribute_safe(databases.create_string_attribute, APPWRITE_DATABASE_ID, RESEARCH_COLLECTION_ID, "paper_id", 100, True)
    
    print(" - title (string, 500 char, required)")
    create_attribute_safe(databases.create_string_attribute, APPWRITE_DATABASE_ID, RESEARCH_COLLECTION_ID, "title", 500, True)
    
    print(" - summary (string, 5000 char, required)")
    create_attribute_safe(databases.create_string_attribute, APPWRITE_DATABASE_ID, RESEARCH_COLLECTION_ID, "summary", 5000, True)
    
    print(" - authors (string, 5000 char, optional - storing as JSON string or comma separated)")
    create_attribute_safe(databases.create_string_attribute, APPWRITE_DATABASE_ID, RESEARCH_COLLECTION_ID, "authors", 5000, False)
    
    print(" - published_at (datetime, required)")
    create_attribute_safe(databases.create_datetime_attribute, APPWRITE_DATABASE_ID, RESEARCH_COLLECTION_ID, "published_at", True)
    
    print(" - pdf_url (url, required - using string 2000)")
    create_attribute_safe(databases.create_url_attribute, APPWRITE_DATABASE_ID, RESEARCH_COLLECTION_ID, "pdf_url", True)
    
    print(" - category (string, 50 char, required)")
    create_attribute_safe(databases.create_string_attribute, APPWRITE_DATABASE_ID, RESEARCH_COLLECTION_ID, "category", 50, True)
    
    # Engagement stats
    print(" - likes (integer, min 0, default 0)")
    create_attribute_safe(databases.create_integer_attribute, APPWRITE_DATABASE_ID, RESEARCH_COLLECTION_ID, "likes", False, 0, 2147483647, 0)

    print(" - dislikes (integer, min 0, default 0)")
    create_attribute_safe(databases.create_integer_attribute, APPWRITE_DATABASE_ID, RESEARCH_COLLECTION_ID, "dislikes", False, 0, 2147483647, 0)
    
    print(" - views (integer, min 0, default 0)")
    create_attribute_safe(databases.create_integer_attribute, APPWRITE_DATABASE_ID, RESEARCH_COLLECTION_ID, "views", False, 0, 2147483647, 0)

    # 2. Indexes
    print("\n2. Creating Indexes...")
    
    print(" - paper_id (unique)")
    create_attribute_safe(
        databases.create_index, 
        APPWRITE_DATABASE_ID, 
        RESEARCH_COLLECTION_ID, 
        "unique_paper_id", 
        "unique", 
        ["paper_id"],
        ["ASC"]
    )
    
    print(" - published_at (key)")
    create_attribute_safe(
        databases.create_index, 
        APPWRITE_DATABASE_ID, 
        RESEARCH_COLLECTION_ID, 
        "idx_published_at", 
        "key", 
        ["published_at"],
        ["DESC"]
    )

    print(" - category (key)")
    create_attribute_safe(
        databases.create_index, 
        APPWRITE_DATABASE_ID, 
        RESEARCH_COLLECTION_ID, 
        "idx_category", 
        "key", 
        ["category"],
        ["ASC"]
    )

    print("\n✅ Setup commands sent. Attribute creation is ASYNC in Appwrite.")
    print("   Wait ~30-60 seconds before running ingestion.")

if __name__ == "__main__":
    manual_setup()
