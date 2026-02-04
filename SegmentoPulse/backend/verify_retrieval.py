"""
Final verification - check if articles are now retrievable
"""

import asyncio
import sys
import os

sys.path.append(os.getcwd())

async def verify_retrieval():
    print("=" * 80)
    print("ARTICLE RETRIEVAL VERIFICATION")
    print("=" * 80)
    
    from dotenv import load_dotenv
    load_dotenv()
    
    from app.services.appwrite_db import get_appwrite_db
    
    db = get_appwrite_db()
    
    categories_to_test = [
        'cloud-computing',
        'cloud-aws',
        'cloud-azure',
        'cloud-gcp',
        'ai',
        'data-security'
    ]
    
    print("\nTesting Article Retrieval:")
    print("-" * 80)
    
    total_articles = 0
    
    for category in categories_to_test:
        articles = await db.get_articles(category, limit=10)
        total_articles += len(articles)
        
        status = "✅" if len(articles) > 0 else "⚠️ "
        print(f"{status} {category}: {len(articles)} articles")
        
        if articles and len(articles) > 0:
            # Show first article as sample
            first = articles[0]
            print(f"   Sample: {first.get('title', 'N/A')[:65]}...")
    
    print("\n" + "=" * 80)
    if total_articles > 0:
        print(f"✅ SUCCESS: Retrieved {total_articles} total articles!")
        print("Database saving and retrieval is working correctly.")
    else:
        print(f"⚠️  WARNING: No articles retrieved")
        print("Check if ingestion completed successfully")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(verify_retrieval())
