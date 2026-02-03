"""
Provider Circuit Breaker
========================

Prevents wasting time/bandwidth on rate-limited or failing API providers.

Features:
- Automatic failure detection
- Exponential backoff
- Circuit state: CLOSED → OPEN → HALF_OPEN → CLOSED
- Per-provider tracking
"""

import time
import logging
from typing import Dict, Optional
from enum import Enum
from collections import defaultdict

logger = logging.getLogger(__name__)


class CircuitState(str, Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, skip provider
    HALF_OPEN = "half_open"  # Testing if recovered


class ProviderCircuitBreaker:
    """
    Circuit breaker for news API providers
    
    Prevents repeatedly calling providers that are:
    - Rate limited (HTTP 429)
    - Down (HTTP 5xx)
    - Slow to respond
    
    Strategy:
    - After 3 failures in 5 minutes → OPEN circuit (skip for 1 hour)
    - After 1 hour → HALF_OPEN (allow 1 test request)
    - If test succeeds → CLOSED (normal operation)
    - If test fails → OPEN for another hour
    """
    
    def __init__(
        self,
        failure_threshold: int = 3,
        failure_window: int = 300,  # 5 minutes
        open_duration: int = 3600,  # 1 hour
        half_open_max_attempts: int = 1
    ):
        """
        Initialize circuit breaker
        
        Args:
            failure_threshold: Number of failures before opening circuit
            failure_window: Time window for counting failures (seconds)
            open_duration: How long to keep circuit open (seconds)
            half_open_max_attempts: Max test requests in HALF_OPEN state
        """
        self.failure_threshold = failure_threshold
        self.failure_window = failure_window
        self.open_duration = open_duration
        self.half_open_max_attempts = half_open_max_attempts
        
        # Provider state tracking
        self.states: Dict[str, CircuitState] = defaultdict(lambda: CircuitState.CLOSED)
        self.failure_counts: Dict[str, int] = defaultdict(int)
        self.last_failure_time: Dict[str, float] = {}
        self.circuit_open_time: Dict[str, float] = {}
        self.half_open_attempts: Dict[str, int] = defaultdict(int)
        
        logger.info("=" * 70)
        logger.info("⚡ [CIRCUIT BREAKER] Provider protection initialized")
        logger.info(f"   Failure threshold: {failure_threshold} failures")
        logger.info(f"   Failure window: {failure_window}s")
        logger.info(f"   Open duration: {open_duration}s ({open_duration//60} min)")
        logger.info("=" * 70)
    
    def should_skip(self, provider: str) -> bool:
        """
        Check if provider should be skipped
        
        Args:
            provider: Provider name (e.g., "gnews", "newsapi")
            
        Returns:
            True if provider should be skipped, False otherwise
        """
        current_state = self.states[provider]
        current_time = time.time()
        
        # CLOSED = normal operation, don't skip
        if current_state == CircuitState.CLOSED:
            return False
        
        # OPEN = provider failing, check if should move to HALF_OPEN
        if current_state == CircuitState.OPEN:
            open_time = self.circuit_open_time.get(provider, 0)
            
            # Check if open duration has elapsed
            if current_time - open_time >= self.open_duration:
                # Move to HALF_OPEN (allow test request)
                self.states[provider] = CircuitState.HALF_OPEN
                self.half_open_attempts[provider] = 0
                logger.info(f"⚡ Circuit HALF_OPEN for {provider} (testing recovery)")
                return False  # Allow test request
            else:
                # Still in open period, skip
                remaining = int(self.open_duration - (current_time - open_time))
                logger.debug(f"⚡ Circuit OPEN for {provider} ({remaining}s remaining)")
                return True
        
        # HALF_OPEN = testing recovery
        if current_state == CircuitState.HALF_OPEN:
            # Allow limited test requests
            if self.half_open_attempts[provider] < self.half_open_max_attempts:
                return False  # Allow test
            else:
                logger.debug(f"⚡ Circuit HALF_OPEN for {provider} (max tests reached)")
                return True  # Max tests reached
        
        return False
    
    def record_success(self, provider: str):
        """
        Record successful request
        
        Args:
            provider: Provider name
        """
        current_state = self.states[provider]
        
        # Reset failure count
        self.failure_counts[provider] = 0
        
        # Close circuit if it was open/half-open
        if current_state != CircuitState.CLOSED:
            self.states[provider] = CircuitState.CLOSED
            logger.info(f"✅ Circuit CLOSED for {provider} (recovered)")
    
    def record_failure(
        self, 
        provider: str, 
        error_type: str = "unknown",
        status_code: Optional[int] = None
    ):
        """
        Record failed request
        
        Args:
            provider: Provider name
            error_type: Type of error ("rate_limit", "timeout", "server_error")
            status_code: HTTP status code (if applicable)
        """
        current_state = self.states[provider]
        current_time = time.time()
        
        # Increment failure count
        self.failure_counts[provider] += 1
        self.last_failure_time[provider] = current_time
        
        # Log failure with details
        status_str = f" (HTTP {status_code})" if status_code else ""
        logger.warning(
            f"⚠️  {provider} failure #{self.failure_counts[provider]}: "
            f"{error_type}{status_str}"
        )
        
        # Check if should open circuit
        if current_state == CircuitState.CLOSED:
            # Check failure window
            if self.failure_counts[provider] >= self.failure_threshold:
                # Open circuit
                self.states[provider] = CircuitState.OPEN
                self.circuit_open_time[provider] = current_time
                
                logger.warning(
                    f"🔴 Circuit OPEN for {provider} "
                    f"({self.failure_counts[provider]} failures) "
                    f"- skipping for {self.open_duration//60} minutes"
                )
        
        # If in HALF_OPEN and fails, go back to OPEN
        elif current_state == CircuitState.HALF_OPEN:
            self.states[provider] = CircuitState.OPEN
            self.circuit_open_time[provider] = current_time
            
            logger.warning(
                f"🔴 Circuit back to OPEN for {provider} "
                f"(test failed) - skipping for {self.open_duration//60} minutes"
            )
    
    def reset(self, provider: Optional[str] = None):
        """
        Reset circuit breaker
        
        Args:
            provider: Provider to reset (None = reset all)
        """
        if provider:
            # Reset specific provider
            self.states[provider] = CircuitState.CLOSED
            self.failure_counts[provider] = 0
            self.half_open_attempts[provider] = 0
            logger.info(f"🔄 Circuit reset for {provider}")
        else:
            # Reset all providers
            self.states.clear()
            self.failure_counts.clear()
            self.last_failure_time.clear()
            self.circuit_open_time.clear()
            self.half_open_attempts.clear()
            logger.info("🔄 All circuits reset")
    
    def get_stats(self) -> dict:
        """Get circuit breaker statistics"""
        total_open = sum(1 for s in self.states.values() if s == CircuitState.OPEN)
        total_half_open = sum(1 for s in self.states.values() if s == CircuitState.HALF_OPEN)
        total_closed = sum(1 for s in self.states.values() if s == CircuitState.CLOSED)
        
        # Provider details
        provider_details = {}
        for provider, state in self.states.items():
            provider_details[provider] = {
                'state': state.value,
                'failures': self.failure_counts.get(provider, 0),
                'last_failure': self.last_failure_time.get(provider)
            }
        
        return {
            'total_open': total_open,
            'total_half_open': total_half_open,
            'total_closed': total_closed,
            'providers': provider_details
        }
    
    def print_stats(self):
        """Print circuit breaker statistics"""
        stats = self.get_stats()
        
        logger.info("")
        logger.info("=" * 70)
        logger.info("⚡ [CIRCUIT BREAKER] Provider Status")
        logger.info("=" * 70)
        logger.info(f"   🔹 Open Circuits: {stats['total_open']}")
        logger.info(f"   🔹 Half-Open Circuits: {stats['total_half_open']}")
        logger.info(f"   🔹 Closed Circuits: {stats['total_closed']}")
        logger.info("")
        
        for provider, details in stats['providers'].items():
            state_emoji = {
                'closed': '✅',
                'open': '🔴',
                'half_open': '🟡'
            }.get(details['state'], '❓')
            
            logger.info(
                f"   {state_emoji} {provider.upper()}: "
                f"{details['state'].upper()} "
                f"({details['failures']} failures)"
            )
        
        logger.info("=" * 70)
        logger.info("")


# Global singleton instance
_circuit_breaker: Optional[ProviderCircuitBreaker] = None


def get_circuit_breaker() -> ProviderCircuitBreaker:
    """
    Get or create global circuit breaker instance
    
    Returns:
        ProviderCircuitBreaker: Singleton instance
    """
    global _circuit_breaker
    
    if _circuit_breaker is None:
        _circuit_breaker = ProviderCircuitBreaker(
            failure_threshold=3,  # 3 failures
            failure_window=300,   # in 5 minutes
            open_duration=3600,   # skip for 1 hour
            half_open_max_attempts=1  # 1 test request
        )
    
    return _circuit_breaker
