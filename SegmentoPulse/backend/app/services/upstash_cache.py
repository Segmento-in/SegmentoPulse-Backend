"""
Upstash Redis Cache Service (REST API)
======================================

Optimized for Upstash Free Tier:
- Data Size: 256 MB (we target max 200 MB)
- Bandwidth: 50 GB/month
- Commands: 10,000/sec
- Request Size: 10 MB
- Record Size: 100 MB

Uses HTTP REST API instead of redis-py for serverless compatibility.
"""

import httpx
import json
import logging
from typing import Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class UpstashCache:
    """
    REST-based Redis caching service for Upstash
    
    Features:
    - HTTP REST client (no redis-py needed)
    - Automatic TTL management
    - Memory-efficient serialization
    - Graceful error handling
    """
    
    def __init__(
        self,
        rest_url: str,
        rest_token: str,
        enabled: bool = True,
        default_ttl: int = 300  # 5 minutes
    ):
        """
        Initialize Upstash cache
        
        Args:
            rest_url: Upstash REST API URL
            rest_token: Upstash REST API token
            enabled: Whether caching is enabled
            default_ttl: Default TTL in seconds
        """
        self.rest_url = rest_url.rstrip('/')
        if self.rest_url and not self.rest_url.startswith("http"):
             self.rest_url = f"https://{self.rest_url}"
             
        # Auto-disable if URL is missing
        if not self.rest_url:
            enabled = False
            logger.warning("⚠️  Upstash URL missing. Disabling cache.")
            
        self.rest_token = rest_token
        self.enabled = enabled
        self.default_ttl = default_ttl
        
        # HTTP client with timeout
        self.client = httpx.Client(
            timeout=5.0,  # 5 second timeout
            headers={
                "Authorization": f"Bearer {rest_token}",
                "Content-Type": "application/json"
            }
        )
        
        # Stats tracking
        self.stats = {
            'hits': 0,
            'misses': 0,
            'sets': 0,
            'errors': 0
        }
        
        if not self.enabled:
            logger.info("ℹ️  Upstash cache disabled (ENABLE_UPSTASH_CACHE=False)")
        else:
            logger.info("=" * 70)
            logger.info("🚀 [UPSTASH] Redis cache initialized")
            logger.info(f"   URL: {rest_url}")
            logger.info(f"   Default TTL: {default_ttl}s")
            logger.info(f"   Free Tier: 256 MB data, 50 GB/month bandwidth")
            logger.info("=" * 70)
    
    def _execute_command(self, command: list) -> Optional[Any]:
        """
        Execute Redis command via REST API
        
        Args:
            command: Redis command as list, e.g. ["GET", "key"]
            
        Returns:
            Command result or None on error
        """
        if not self.enabled:
            return None
        
        try:
            response = self.client.post(
                f"{self.rest_url}",
                json=command
            )
            
            if response.status_code == 200:
                result = response.json()
                return result.get("result")
            else:
                logger.warning(f"⚠️  Upstash error: {response.status_code} - {response.text}")
                self.stats['errors'] += 1
                return None
                
        except Exception as e:
            logger.error(f"❌ Upstash request failed: {e}")
            self.stats['errors'] += 1
            return None
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache
        
        Args:
            key: Cache key
            
        Returns:
            Cached value (deserialized) or None if not found
        """
        if not self.enabled:
            return None
        
        try:
            result = self._execute_command(["GET", key])
            
            if result is None:
                self.stats['misses'] += 1
                logger.debug(f"❌ Cache MISS: {key}")
                return None
            
            # Deserialize JSON
            value = json.loads(result)
            self.stats['hits'] += 1
            logger.debug(f"✅ Cache HIT: {key}")
            return value
            
        except Exception as e:
            logger.error(f"❌ Cache get error for {key}: {e}")
            self.stats['errors'] += 1
            return None
    
    def set(
        self, 
        key: str, 
        value: Any, 
        ttl: Optional[int] = None
    ) -> bool:
        """
        Set value in cache with TTL
        
        Args:
            key: Cache key
            value: Value to cache (will be JSON serialized)
            ttl: Time-to-live in seconds (uses default if not specified)
            
        Returns:
            True if successful, False otherwise
        """
        if not self.enabled:
            return False
        
        try:
            # Serialize to JSON
            serialized = json.dumps(value)
            
            # Check size (warn if >1MB)
            size_kb = len(serialized) / 1024
            if size_kb > 1024:  # >1MB
                logger.warning(f"⚠️  Large cache entry: {key} ({size_kb:.1f} KB)")
            
            # Use provided TTL or default
            ttl_seconds = ttl if ttl is not None else self.default_ttl
            
            # SETEX command (set with expiration)
            result = self._execute_command(["SETEX", key, ttl_seconds, serialized])
            
            if result == "OK" or result is not None:
                self.stats['sets'] += 1
                logger.debug(f"💾 Cache SET: {key} (TTL: {ttl_seconds}s, Size: {size_kb:.1f} KB)")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Cache set error for {key}: {e}")
            self.stats['errors'] += 1
            return False
    
    def delete(self, key: str) -> bool:
        """
        Delete key from cache
        
        Args:
            key: Cache key to delete
            
        Returns:
            True if deleted, False otherwise
        """
        if not self.enabled:
            return False
        
        try:
            result = self._execute_command(["DEL", key])
            deleted = result == 1
            
            if deleted:
                logger.debug(f"🗑️  Cache DELETE: {key}")
            
            return deleted
            
        except Exception as e:
            logger.error(f"❌ Cache delete error for {key}: {e}")
            return False
    
    def invalidate_pattern(self, pattern: str) -> int:
        """
        Invalidate all keys matching pattern
        
        Args:
            pattern: Redis pattern (e.g., "news:*")
            
        Returns:
            Number of keys deleted
        """
        if not self.enabled:
            return 0
        
        try:
            # Get all matching keys
            keys = self._execute_command(["KEYS", pattern])
            
            if not keys:
                return 0
            
            # Delete all keys
            for key in keys:
                self._execute_command(["DEL", key])
            
            logger.info(f"🗑️  Invalidated {len(keys)} keys matching '{pattern}'")
            return len(keys)
            
        except Exception as e:
            logger.error(f"❌ Cache invalidation error for {pattern}: {e}")
            return 0
    
    def get_stats(self) -> dict:
        """Get cache statistics"""
        total_requests = self.stats['hits'] + self.stats['misses']
        hit_rate = (
            self.stats['hits'] / total_requests * 100 
            if total_requests > 0 else 0
        )
        
        return {
            **self.stats,
            'total_requests': total_requests,
            'hit_rate_percent': round(hit_rate, 2),
            'enabled': self.enabled
        }
    
    def print_stats(self):
        """Print cache statistics"""
        stats = self.get_stats()
        
        logger.info("")
        logger.info("=" * 70)
        logger.info("📊 [UPSTASH] Cache Statistics")
        logger.info("=" * 70)
        logger.info(f"   🔹 Total Requests: {stats['total_requests']:,}")
        logger.info(f"   🔹 Cache Hits: {stats['hits']:,}")
        logger.info(f"   🔹 Cache Misses: {stats['misses']:,}")
        logger.info(f"   🔹 Hit Rate: {stats['hit_rate_percent']}%")
        logger.info(f"   🔹 Sets: {stats['sets']:,}")
        logger.info(f"   🔹 Errors: {stats['errors']:,}")
        logger.info("=" * 70)
        logger.info("")
    
    def health_check(self) -> bool:
        """
        Check if Upstash is reachable
        
        Returns:
            True if healthy, False otherwise
        """
        try:
            result = self._execute_command(["PING"])
            healthy = result == "PONG"
            
            if healthy:
                logger.info("✅ Upstash health check: OK")
            else:
                logger.warning("⚠️  Upstash health check: FAILED")
            
            return healthy
            
        except Exception as e:
            logger.error(f"❌ Upstash health check error: {e}")
            return False
    
    def close(self):
        """Close HTTP client"""
        if hasattr(self, 'client'):
            self.client.close()


# Global singleton instance
_upstash_cache: Optional[UpstashCache] = None


def get_upstash_cache() -> UpstashCache:
    """
    Get or create global Upstash cache instance
    
    Returns:
        UpstashCache: Singleton cache instance
    """
    global _upstash_cache
    
    if _upstash_cache is None:
        from app.config import settings
        
        _upstash_cache = UpstashCache(
            rest_url=settings.UPSTASH_REDIS_REST_URL,
            rest_token=settings.UPSTASH_REDIS_REST_TOKEN,
            enabled=settings.ENABLE_UPSTASH_CACHE,
            default_ttl=300  # 5 minutes default
        )
    
    return _upstash_cache
