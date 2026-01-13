try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    print("Redis not available - caching disabled")

from typing import Any, Optional
import json
from app.config import settings

class CacheService:
    """Redis caching service (optional)"""
    
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None if not REDIS_AVAILABLE else None
        self.ttl = settings.CACHE_TTL if hasattr(settings, 'CACHE_TTL') else 120
    
    async def connect(self):
        """Connect to Redis (if available)"""
        if not REDIS_AVAILABLE:
            return
        
        try:
            self.redis_client = await redis.from_url(
                settings.REDIS_URL if hasattr(settings, 'REDIS_URL') else 'redis://localhost:6379',
                password=settings.REDIS_PASSWORD if hasattr(settings, 'REDIS_PASSWORD') and settings.REDIS_PASSWORD else None,
                encoding="utf-8",
                decode_responses=True
            )
        except Exception as e:
            print(f"Redis connection failed: {e}")
            self.redis_client = None
    
    async def get(self, key: str) -> Optional[Any]:
        """Get cached value"""
        if not self.redis_client:
            await self.connect()
        
        if not self.redis_client:
            return None
        
        try:
            value = await self.redis_client.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            print(f"Cache get error: {e}")
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """Set cached value"""
        if not self.redis_client:
            await self.connect()
        
        if not self.redis_client:
            return False
        
        try:
            cache_ttl = ttl if ttl is not None else self.ttl
            # Convert Pydantic models to dict
            if hasattr(value, 'model_dump'):
                value = [item.model_dump() for item in value]
            
            await self.redis_client.setex(
                key,
                cache_ttl,
                json.dumps(value, default=str)
            )
            return True
        except Exception as e:
            print(f"Cache set error: {e}")
            return False
    
    async def delete(self, key: str):
        """Delete cached value"""
        if not self.redis_client:
            return False
        
        try:
            await self.redis_client.delete(key)
            return True
        except Exception as e:
            print(f"Cache delete error: {e}")
            return False
    
    async def clear_pattern(self, pattern: str):
        """Clear all keys matching pattern"""
        if not self.redis_client:
            return False
        
        try:
            keys = await self.redis_client.keys(pattern)
            if keys:
                await self.redis_client.delete(*keys)
            return True
        except Exception as e:
            print(f"Cache clear error: {e}")
            return False
