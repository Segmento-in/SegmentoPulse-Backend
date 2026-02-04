"""
Quick test to verify article saving is working after fix
"""

import asyncio
import sys
import os

sys.path.append(os.getcwd())

async def test_save_fix():
    print("="  * 60)
    print("Testing Article Save Fix")
    print("=" * 60)
    
    from dotenv import load_dotenv
    load_dotenv()
    
    from app.services.appwrite_db import get_appwrite_db
    from datetime import datetime
    
    db = get_appwrite_db()
    
    print(f"\n1. Database initialized: {db.initialized}")
    
   # Test with sample article
    test_article = {
        'title': f'TEST - Save Fix Verification {datetime.now()}',
        'description': 'Testing if save_articles now returns tuple correctly',
        'url': f'https://test.com/fix-test-{datetime.now().timestamp()}',
        'image': '',
        'publishedAt': datetime.now().isoformat(),
        'source': 'TEST',
        'category': 'cloud-computing'
    }
    
    print("\n2. Attempting to save test article...")
    
    try:
        result = await db.save_articles([test_article])
        print(f"   Result type: {type(result)}")
        print(f"   Result value: {result}")
        
        if isinstance(result, tuple) and len(result) == 2:
            saved_count, saved_docs = result
            print(f"   ✅ Returns tuple correctly!")
            print(f"   Saved count: {saved_count}")
            print(f"   Saved docs: {len(saved_docs)}")
        else:
            print(f"   ❌ Still returns wrong type!")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    # Now fetch from cloud-computing to verify
    print("\n3. Fetching cloud-computing articles...")
    
    articles = await db.get_articles('cloud-computing', limit=5)
    print(f"   Found {len(articles)} articles")
    
    if articles:
        print("\n   Sample articles:")
        for i, art in enumerate(articles[:3], 1):
            print(f"   {i}. {art.get('title', 'N/A')[:60]}")

if __name__ == "__main__":
    asyncio.run(test_save_fix())
