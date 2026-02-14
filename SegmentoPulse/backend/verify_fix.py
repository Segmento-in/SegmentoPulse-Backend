from app.services.appwrite_db import AppwriteDatabase
from app.config import settings

def verify_routing():
    db = AppwriteDatabase()
    
    # 1. Verify Magazines Routing
    mag_id = db.get_collection_id('magazines')
    expected_mag_id = settings.APPWRITE_COLLECTION_ID
    
    print(f"Magazines Collection ID: {mag_id}")
    print(f"Expected (Main):         {expected_mag_id}")
    
    if mag_id == expected_mag_id:
        print("✅ Magazines routing CORRECT (Using Main DB)")
    else:
        print("❌ Magazines routing INCORRECT (Still using dedicated DB)")
        
    print("-" * 30)

    # 2. Verify Medium Routing
    med_id = db.get_collection_id('medium-article')
    expected_med_id = settings.APPWRITE_MEDIUM_COLLECTION_ID
    
    print(f"Medium Collection ID:    {med_id}")
    print(f"Expected (Dedicated):    {expected_med_id}")
    
    if med_id == expected_med_id:
        print("✅ Medium routing CORRECT (Using Dedicated DB)")
    else:
        print("❌ Medium routing INCORRECT")

if __name__ == "__main__":
    verify_routing()
