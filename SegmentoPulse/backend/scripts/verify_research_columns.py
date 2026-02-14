
import os
import sys
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

def list_attributes():
    print(f"📊 Checking Attributes for Collection: {RESEARCH_COLLECTION_ID}")
    try:
        # Fetch list of attributes
        # Note: Appwrite returns a list of attribute objects
        # We need to handle pagination if there are many, but usually < 25
        response = databases.list_attributes(
            database_id=APPWRITE_DATABASE_ID,
            collection_id=RESEARCH_COLLECTION_ID
        )
        
        attributes = response.get('attributes', [])
        print(f"   Found {len(attributes)} attributes.")
        
        found_attrs = set()
        for attr in attributes:
            key = attr.get('key')
            status = attr.get('status')
            typ = attr.get('type')
            print(f"   - {key} ({typ}) [Status: {status}]")
            found_attrs.add(key)
            
        # Check against expected
        expected = ['paper_id', 'title', 'summary', 'authors', 'published_at', 'pdf_url', 'category', 'likes', 'dislikes', 'views']
        missing = [x for x in expected if x not in found_attrs]
        
        if missing:
            print(f"\n❌ MISSING Attributes: {missing}")
        else:
            print(f"\n✅ All expected attributes exist.")
            
    except Exception as e:
        print(f"❌ Error fetching attributes: {e}")

if __name__ == "__main__":
    list_attributes()
