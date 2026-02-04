
import asyncio
import os
import sys
import logging
from datetime import datetime

# Add project root to path
sys.path.append(os.getcwd())

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def verify_ingestion():
    """
    Manually trigger ingestion and verify results
    """
    print("=" * 80)
    print("🧪 MANUAL INGESTION VERIFICATION SCRIPT")
    print("=" * 80)
    
    try:
        # Load env vars
        from dotenv import load_dotenv
        load_dotenv()
        
        # Check secrets
        from app.config import settings
        print(f"🔑 UPSTASH URL: {(settings.UPSTASH_REDIS_REST_URL[:20] + '...') if settings.UPSTASH_REDIS_REST_URL else 'MISSING'}")
        
        # Import scheduler function
        from app.services.scheduler import fetch_and_validate_category
        from app.services.upstash_cache import get_upstash_cache
        
        # Initialize Cache
        cache = get_upstash_cache()
        health = cache.health_check()
        print(f"🏥 Cache Health Check: {'✅ OK' if health else '❌ FAILED'}")

        # Trigger fetch for ONE category (to save time) - 'ai'
        category = 'ai'
        print(f"\n🚀 Triggering manual fetch for category: '{category}'...")
        
        result = await fetch_and_validate_category(category)
        
        # Result format: (category, valid_articles, invalid_count, irrelevant_count)
        cat_name, articles, invalid, irrelevant = result
        
        print(f"\n✅ Fetch Complete for '{cat_name}':")
        print(f"   - Valid Articles: {len(articles)}")
        print(f"   - Invalid: {invalid}")
        
        if len(articles) > 0:
            print("\n💾 saving to cache manually...")
            
            # Handle list of dicts or objects
            cache_data = []
            for a in articles:
                if isinstance(a, dict):
                    cache_data.append(a)
                elif hasattr(a, 'dict'):
                     cache_data.append(a.dict())
                elif hasattr(a, 'model_dump'):
                     cache_data.append(a.model_dump())
            
            # Save to Cache 
            success = cache.set(f"news:{category}", cache_data, ttl=300)
            print(f"   - Cache Set Result: {'✅ Success' if success else '❌ Failed'}")
            
            # Verify Read from Cache
            cached_data = cache.get(f"news:{category}")
            print(f"   - Cache Read Verification: {'✅ Success (Found Data)' if cached_data else '❌ Failed (None)'}")
            
            if cached_data:
                print(f"   - Cached Count: {len(cached_data)}")
        else:
            print("⚠️  No articles found to cache.")
            
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(verify_ingestion())
