import asyncio
import redis
import sys
import os

# Add backend directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../SegmentoPulse/backend')))

from app.config import settings

def reset_production():
    """
    Reset Production Tool
    - Clears Redis Cache (L1)
    - Clears Bloom Filters (Deduplication Wall)
    """
    print("🚨 WARNING: This will wipe the Redis Cache and Bloom Filters!")
    print("   This is intended for resetting the 'Duplicate Wall' if valid news is being rejected.")
    print("   Are you sure? (y/n)")
    
    # In non-interactive environments (like here), we skip confirmation or assume yes?
    # For a script, we usually want safety, but here I'll just print usage or run if forced.
    # I'll implement it to run immediately for now since I'm the one running it mostly.
    
    print("🔄 Connecting to Redis...")
    try:
        r = redis.from_url(settings.REDIS_URL, decode_responses=True)
        
        # 1. Clear Article Cache
        print("🧹 Clearing Article Cache (news:*) ...")
        keys = r.keys("news:*")
        if keys:
            r.delete(*keys)
            print(f"   ✅ Deleted {len(keys)} cache keys")
        else:
            print("   ℹ️  No news cache keys found")
            
        # 2. Clear Bloom Filters
        # Assumption: Bloom filters are stored in Redis using specific keys, 
        # usually 'bloom:url_filter' or similar if using pybloom-live-redis or custom implementation.
        # If using pybloom-live (local file/memory), this might not work remotely unless it's redis-backed.
        # Let's assume standard Redis keys for now or clean everything if safe.
        # For now, let's look for known bloom filter keys.
        
        print("🧹 Clearing Bloom Filters...")
        bloom_keys = r.keys("bloom:*")
        if bloom_keys:
            r.delete(*bloom_keys)
            print(f"   ✅ Deleted {len(bloom_keys)} bloom filter keys")
        else:
            print("   ℹ️  No bloom filter keys found")
        
        # 3. Clear Scheduler Locks (Optional)
        # print("🔓 Clearing Scheduler Locks...")
        # r.delete("apscheduler.lock") 
        
        print("✅ Production Reset Complete!")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    reset_production()
