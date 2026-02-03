"""
API Quota Tracking Service
Monitors API usage and prevents hitting rate limits
"""

from typing import Dict, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class APIQuotaTracker:
    """Track API usage and enforce rate limits"""
    
    def __init__(self):
        self.quotas = {
            "gnews": {
                "calls_per_day": 100,
                "calls_made": 0,
                "reset_time": None,
                "last_call": None
            },
            "newsapi": {
                "calls_per_day": 100,
                "calls_made": 0,
                "reset_time": None,
                "last_call": None
            },
            "newsdata": {
                "calls_per_day": 200,
                "calls_made": 0,
                "reset_time": None,
                "last_call": None
            },
            "groq": {
                "tokens_per_minute": 30000,
                "tokens_used": 0,
                "reset_time": None,
                "last_call": None
            }
        }
    
    def record_call(self, provider: str, tokens_or_calls: int = 1):
        """Record an API call"""
        if provider not in self.quotas:
            logger.warning(f"Unknown provider: {provider}")
            return
        
        now = datetime.now()
        quota = self.quotas[provider]
        
        # Reset daily counters if needed
        if quota["reset_time"] and now > quota["reset_time"]:
            if "calls_per_day" in quota:
                quota["calls_made"] = 0
            else:
                quota["tokens_used"] = 0
        
        # Set reset time if not set
        if not quota["reset_time"]:
            if "calls_per_day" in quota:
                quota["reset_time"] = now + timedelta(days=1)
            else:
                quota["reset_time"] = now + timedelta(minutes=1)
        
        # Record the call
        if "calls_per_day" in quota:
            quota["calls_made"] += tokens_or_calls
        else:
            quota["tokens_used"] += tokens_or_calls
        
        quota["last_call"] = now.isoformat()
        
        # Log warning if approaching limit
        self._check_limits(provider)
    
    def _check_limits(self, provider: str):
        """Check if approaching rate limits"""
        quota = self.quotas[provider]
        
        if "calls_per_day" in quota:
            limit = quota["calls_per_day"]
            used = quota["calls_made"]
            if used >= limit * 0.9:
                logger.warning(f"⚠️ {provider} approaching daily limit: {used}/{limit}")
            if used >= limit:
                logger.error(f"❌ {provider} daily limit exceeded: {used}/{limit}")
        else:
            limit = quota["tokens_per_minute"]
            used = quota["tokens_used"]
            if used >= limit * 0.9:
                logger.warning(f"⚠️ {provider} approaching token limit: {used}/{limit} per minute")
            if used >= limit:
                logger.error(f"❌ {provider} token limit exceeded: {used}/{limit} per minute")
    
    def can_make_call(self, provider: str, tokens_or_calls: int = 1) -> bool:
        """Check if an API call can be made without exceeding quotas"""
        if provider not in self.quotas:
            return True
        
        quota = self.quotas[provider]
        now = datetime.now()
        
        # Reset if needed
        if quota["reset_time"] and now > quota["reset_time"]:
            if "calls_per_day" in quota:
                quota["calls_made"] = 0
            else:
                quota["tokens_used"] = 0
            quota["reset_time"] = None
        
        # Check limits
        if "calls_per_day" in quota:
            return quota["calls_made"] + tokens_or_calls <= quota["calls_per_day"]
        else:
            return quota["tokens_used"] + tokens_or_calls <= quota["tokens_per_minute"]
    
    def get_stats(self) -> Dict:
        """Get current quota usage statistics"""
        stats = {}
        
        for provider, quota in self.quotas.items():
            if "calls_per_day" in quota:
                stats[provider] = {
                    "limit": quota["calls_per_day"],
                    "used": quota["calls_made"],
                    "remaining": quota["calls_per_day"] - quota["calls_made"],
                    "reset_time": quota["reset_time"].isoformat() if quota["reset_time"] else None,
                    "last_call": quota["last_call"]
                }
            else:
                stats[provider] = {
                    "limit": f"{quota['tokens_per_minute']} tokens/min",
                    "used": quota["tokens_used"],
                    "remaining": quota["tokens_per_minute"] - quota["tokens_used"],
                    "reset_time": quota["reset_time"].isoformat() if quota["reset_time"] else None,
                    "last_call": quota["last_call"]
                }
        
        return stats


# Global singleton
_quota_tracker: Optional[APIQuotaTracker] = None


def get_quota_tracker() -> APIQuotaTracker:
    """Get or create global quota tracker instance"""
    global _quota_tracker
    
    if _quota_tracker is None:
        _quota_tracker = APIQuotaTracker()
        logger.info("📊 API Quota Tracker initialized")
    
    return _quota_tracker
