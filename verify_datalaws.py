import asyncio
import sys
import os

# Add backend directory to path
sys.path.append(os.path.join(os.getcwd(), 'SegmentoPulse', 'backend'))

from app.services.news_aggregator import NewsAggregator

async def verify():
    print("Testing 'data-laws' category fetch...")
    aggregator = NewsAggregator()
    
    # forcing simple fetch
    try:
        articles = await aggregator.fetch_by_category("data-laws")
        
        if articles:
            print(f"Success! Fetched {len(articles)} articles for 'data-laws'.")
            for a in articles[:3]:
                print(f"   - {a.title} ({a.source})")
        else:
            print("No articles found. Check provider logic.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(verify())
