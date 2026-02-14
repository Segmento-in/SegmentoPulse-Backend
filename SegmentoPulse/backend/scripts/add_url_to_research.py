
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

def add_url_and_backfill():
    print(f"🔧 Adding 'url' attribute to {RESEARCH_COLLECTION_ID}...")
    
    # 1. Create Attribute
    try:
        # url (string, 2000 chars, required=False)
        databases.create_url_attribute(APPWRITE_DATABASE_ID, RESEARCH_COLLECTION_ID, "url", False)
    except Exception as e:
        if "already exists" not in str(e): print(f"Error creating attribute: {e}")
    
    if wait_for_attribute(RESEARCH_COLLECTION_ID, "url"):
        print("✅ Attribute 'url' created.")
        
        # 2. Backfill existing documents
        print("🔄 Backfilling 'url' from 'pdf_url'...")
        try:
            # 1. List all documents (cursor pagination if many, but assuming few for now)
            has_more = True
            cursor = None
            total_updated = 0
            
            while has_more:
                from appwrite.query import Query
                q = [Query.limit(100)]
                if cursor:
                    q.append(Query.cursor_after(cursor))

                response = databases.list_documents(
                    database_id=APPWRITE_DATABASE_ID,
                    collection_id=RESEARCH_COLLECTION_ID,
                    queries=q
                )
                
                docs = response.get('documents', [])
                if not docs:
                    break
                    
                for doc in docs:
                    if not doc.get('url') and doc.get('pdf_url'):
                        try:
                            databases.update_document(
                                database_id=APPWRITE_DATABASE_ID,
                                collection_id=RESEARCH_COLLECTION_ID,
                                document_id=doc['$id'],
                                data={'url': doc['pdf_url']}
                            )
                            total_updated += 1
                            print(f"   updated {doc['$id']}")
                        except Exception as e:
                            print(f"   failed to update {doc['$id']}: {e}")
                
                cursor = docs[-1]['$id']
                if len(docs) < 100:
                    has_more = False
            
            print(f"✅ Backfill complete. Updated {total_updated} documents.")
            
        except Exception as e:
            print(f"❌ Backfill failed: {e}")

if __name__ == "__main__":
    add_url_and_backfill()
