import asyncio
import sys
import os

# Add backend directory to path
sys.path.append(os.path.join(os.getcwd(), 'SegmentoPulse', 'backend'))

from app.services.news_aggregator import NewsAggregator
from app.services.appwrite_db import get_appwrite_db
from dotenv import load_dotenv

# Load env vars explicitly
load_dotenv(os.path.join(os.getcwd(), 'SegmentoPulse', 'backend', '.env'))

async def populate():
    print("Fetching 'data-laws' articles...")
    aggregator = NewsAggregator()
    db = get_appwrite_db()
    
    # Fetch from all providers
    articles = await aggregator.fetch_by_category("data-laws")
    
    if articles:
        print(f"Found {len(articles)} articles. Saving to database...")
        saved_count = await db.save_articles(articles)
        print(f"Success! Saved {saved_count} articles to the database.")
    else:
        print("No articles found to save.")

if __name__ == "__main__":
    asyncio.run(populate())
