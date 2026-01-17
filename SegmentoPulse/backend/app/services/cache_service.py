"""
Cache Service using Redis
Provides caching layer to reduce external API calls with graceful bypass when disabled
"""

try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    print("Redis package not available - caching disabled")

from typing import Optional, Any, List
import json
from app.config import settings
from app.models import Article


class CacheService:
    """Redis cache service with graceful bypass when disabled"""
    
    def __init__(self):
        self.redis_client = None
        self.enabled = settings.ENABLE_REDIS and REDIS_AVAILABLE
        
        if settings.ENABLE_REDIS and not REDIS_AVAILABLE:
            print("⚠️  ENABLE_REDIS=True but Redis package not installed")
            print("  Running in cache-bypass mode")
        elif not settings.ENABLE_REDIS:
            print("ℹ️  Redis cache disabled (ENABLE_REDIS=False)")
            print("  Running in cache-bypass mode")
    
    async def connect(self):
        """Connect to Redis (if enabled)"""
        if not self.enabled:
            return
        
        try:
            self.redis_client = await redis.from_url(
                settings.REDIS_URL,
                password=settings.REDIS_PASSWORD if settings.REDIS_PASSWORD else None,
                encoding="utf-8",
                decode_responses=True
            )
            print("✓ Redis cache connected")
        except Exception as e:
            print(f"✗ Redis connection failed: {e}")
            print("  Running in cache-bypass mode")
            self.redis_client = None
    
    async def get(self, key: str) -> Optional[List[Article]]:
        """
        Get cached data by key
        
        Returns None if:
        - Redis is disabled
        - Connection failed
        - Key doesn't exist
        - Cache expired
        """
        if not self.enabled:
            return None
        
        if not self.redis_client:
            await self.connect()
        
        if not self.redis_client:
            return None
        
        try:
            data = await self.redis_client.get(key)
            if data:
                # Parse JSON and convert to Article objects
                articles_data = json.loads(data)
                articles = [Article(**article) for article in articles_data]
                return articles
            return None
        except Exception as e:
            # Silently fail - just return None (cache miss)
            return None
    
    async def set(self, key: str, value: List[Article], ttl: Optional[int] = None) -> bool:
        """
        Set cached data with optional TTL
        
        Returns:
            True if successful (or bypassed)
            False if error occurred
        """
        if not self.enabled:
            return True  # Bypass mode - pretend success
        
        if not self.redis_client:
            await self.connect()
        
        if not self.redis_client:
            return True  # Bypass mode - pretend success
        
        try:
            # Use configured TTL if not provided
            cache_ttl = ttl if ttl is not None else settings.CACHE_TTL
            
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
            # Silently fail - app continues without cache
            return False
    
    async def delete(self, key: str) -> bool:
        """
        Delete cached data by key
        
        Returns:
            True if successful (or bypassed)
            False if error occurred
        """
        if not self.enabled or not self.redis_client:
            return True  # Bypass mode - pretend success
        
        try:
            await self.redis_client.delete(key)
            return True
        except Exception as e:
            return False
    
    async def clear_all(self) -> bool:
        """Clear all cached data"""
        if not self.enabled or not self.redis_client:
            return True  # Bypass mode - pretend success
        
        try:
            await self.redis_client.flushdb()
            return True
        except Exception as e:
            return False
