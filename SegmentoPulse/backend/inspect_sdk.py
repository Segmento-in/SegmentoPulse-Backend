from appwrite.client import Client
from appwrite.services.databases import Databases
import inspect

def inspect_sdk():
    client = Client()
    db = Databases(client)
    print("Methods in Databases service:")
    methods = [m for m in dir(db) if not m.startswith('_')]
    for m in methods:
        print(f" - {m}")
        
    if 'list_documents' in dir(db):
        print("\nlist_documents exists.")
    if 'list_rows' in dir(db):
        print("\nlist_rows exists.")
    if 'get_document' in dir(db):
        print("\nget_document exists.")
    if 'get_row' in dir(db):
        print("\nget_row exists.")

if __name__ == "__main__":
    inspect_sdk()
