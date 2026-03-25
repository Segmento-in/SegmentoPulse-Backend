from appwrite.client import Client
from appwrite.services.tables_db import TablesDB
import inspect

def inspect_tables_db():
    client = Client()
    tdb = TablesDB(client)
    print("Methods in TablesDB service:")
    methods = [m for m in dir(tdb) if not m.startswith('_')]
    for m in methods:
        print(f" - {m}")
        
    if 'list_rows' in dir(tdb):
        print("\nlist_rows exists.")
    if 'get_row' in dir(tdb):
        print("\nget_row exists.")

if __name__ == "__main__":
    inspect_tables_db()
