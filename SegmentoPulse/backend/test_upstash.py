import asyncio
from app.services.upstash_cache import get_upstash_cache

async def main():
    cache = get_upstash_cache()
    
    # 1. Test set
    success = await cache.set("test_bridge_key", {"status": "success"}, ttl=60)
    print(f"Set successful: {success}")
    
    # 2. Test get
    val = await cache.get("test_bridge_key")
    print(f"Got value: {val}")
    
if __name__ == "__main__":
    asyncio.run(main())
