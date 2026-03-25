from appwrite.services.tables_db import TablesDB
import inspect

def inspect_methods():
    tdb = TablesDB(None)
    
    print("Signatures:")
    try:
        print(f"list_rows: {inspect.signature(tdb.list_rows)}")
    except:
        print("Could not get signature for list_rows")
        
    try:
        print(f"get_row: {inspect.signature(tdb.get_row)}")
    except:
        print("Could not get signature for get_row")

    try:
        print(f"create_row: {inspect.signature(tdb.create_row)}")
    except:
        print("Could not get signature for create_row")

if __name__ == "__main__":
    inspect_methods()
