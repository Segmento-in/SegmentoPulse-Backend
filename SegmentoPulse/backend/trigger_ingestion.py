"""
Trigger full news ingestion to populate database
"""

import asyncio
import sys
import os

sys.path.append(os.getcwd())

async def trigger_ingestion():
    print("=" * 80)
    print("TRIGGERING FULL NEWS INGESTION")
    print("=" * 80)
    print("\nThis will fetch articles for all 23 categories...")
    print("Expected duration: ~30-60 seconds\n")
    
    from dotenv import load_dotenv
    load_dotenv()
    
    from app.services.scheduler import fetch_all_news
    
    await fetch_all_news()
    
    print("\n" + "=" * 80)
    print("INGESTION COMPLETE")
    print("=" * 80)
    print("\nCheck logs above for results.")
    print("Articles should now be in Appwrite database!")

if __name__ == "__main__":
    asyncio.run(trigger_ingestion())
