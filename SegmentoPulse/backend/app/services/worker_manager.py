"""
Worker Manager Service
Consumer process that pulls categories from Redis and executes them one by one.
Provides stability, reliability (RPOPLPUSH), and anti-ban pacing.
"""
import asyncio
import logging
import random
import signal
from datetime import datetime

from app.services.upstash_cache import get_upstash_cache
from app.services.news_aggregator import NewsAggregator
from app.services.news_processor import process_category
from app.utils.custom_logger import get_logger
from app.config import CATEGORIES

logger = get_logger(__name__)

class WorkerManager:
    def __init__(self):
        self.running = False
        self.aggregator = NewsAggregator()
        self.upstash = get_upstash_cache()
        self.pending_queue = "segmento:pending_news_queue"
        self.processing_queue = "segmento:processing_news_queue"
        self.dlq = "segmento:dead_letter_queue"
        self.visibility_map = "segmento:worker_visibility_tracker"
        self.visibility_timeout = 600  # 10 minutes
        
    async def start(self):
        """Main consumer loop"""
        self.running = True
        self.polling_wait = 5  # Start with 5s polling wait
        logger.info("👷 [WORKER] Starting consumer loop...")
        
        # Initial jitter to stagger startup across multiple instances if they exist
        await asyncio.sleep(random.uniform(5, 15))
        
        while self.running:
            try:
                # 1. Atomic extraction (Pending -> Processing)
                # This ensures we don't lose the task if the worker crashes mid-run.
                category = await self.upstash.rpoplpush(self.pending_queue, self.processing_queue)
                
                if not category:
                    # 1b. Polite Polling: Exponential Backoff (5s -> 10s -> ... -> 60s)
                    self.polling_wait = min(self.polling_wait * 2, 60)
                    
                    # No new tasks, check for zombies occasionally (5% chance per empty loop)
                    if random.random() < 0.05:
                        await self.cleanup_zombie_tasks()
                    
                    logger.debug("📭 [WORKER] Queue empty. Polling backoff: %ds", self.polling_wait)
                    await asyncio.sleep(self.polling_wait)
                    continue
                
                # Reset polling wait when a task is found
                self.polling_wait = 5
                
                # 2. Track start time for visibility timeout
                start_time = int(datetime.now().timestamp())
                await self.upstash._execute_command(["HSET", self.visibility_map, category, start_time])

                # 3. Process the category
                logger.info("🎯 [WORKER] Processing task from queue: %s", category.upper())
                
                try:
                    success = await process_category(category, self.aggregator)
                except Exception as proc_err:
                    logger.error("❌ [WORKER] Task failed: %s. Moving to DLQ.", category)
                    await self.upstash.lpush(self.dlq, f"{category} | {datetime.now().isoformat()} | {str(proc_err)}")
                    success = False

                if success:
                    # 4. Cleanup: Task finished successfully
                    await self.upstash.lrem(self.processing_queue, 1, category)
                    await self.upstash._execute_command(["HDEL", self.visibility_map, category])
                    logger.info("✅ [WORKER] Task completed and cleaned: %s", category.upper())
                else:
                    # If process_category failed, it's already in DLQ or re-queued by Reaper
                    await self.upstash.lrem(self.processing_queue, 1, category)
                    await self.upstash._execute_command(["HDEL", self.visibility_map, category])

                # 5. Mandatory spacing + Adaptive Backoff
                # We check if many providers are currently "Open" (tripped)
                # If they are, we sleep longer to allow them to recover.
                backoff_multiplier = 1.0
                try:
                    from app.services.circuit_breaker import ProviderCircuitBreaker
                    # Simple heuristic: if any major provider is open, slow down.
                    open_breakers = 0
                    for provider in ["google", "serper", "bing"]:
                        breaker = ProviderCircuitBreaker(provider)
                        state = await breaker.get_state()
                        if state == "open":
                            open_breakers += 1
                    
                    if open_breakers > 0:
                        backoff_multiplier = 1.5 + (0.5 * open_breakers)
                        logger.warning("🐌 [WORKER] %d circuit breakers are OPEN. Applying backoff multiplier: %.1f", open_breakers, backoff_multiplier)
                except Exception:
                    pass

                # Normal spacing: ~60s average. With backoff: can go up to 3-5 mins.
                base_wait = random.uniform(30, 90)
                wait_time = base_wait * backoff_multiplier
                logger.info("💤 [WORKER] Pacing: sleeping for %.1fs...", wait_time)
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                logger.error("❌ [WORKER] Fatal error in consumer loop: %s", e)
                # Should we move to DLQ? In Task 5 we will implement the Reaper.
                await asyncio.sleep(30) # Backoff on error

    def stop(self, *args):
        logger.info("👷 [WORKER] Stopping gracefully...")
        self.running = False

    async def cleanup_zombie_tasks(self):
        """
        The Reaper: Scans the processing queue for tasks that timed out.
        If a worker died while processing, this moves the task back to pending.
        """
        try:
            # 1. Get all tasks in processing queue
            processing_tasks = await self.upstash._execute_command(["LRANGE", self.processing_queue, 0, -1])
            if not processing_tasks:
                return

            # 2. Get start times from visibility map
            # Note: we use individual HGETs or HGETALL if safe
            current_time = int(datetime.now().timestamp())
            
            for category in processing_tasks:
                start_time_str = await self.upstash._execute_command(["HGET", self.visibility_map, category])
                if not start_time_str:
                    # Edge case: Task in processing but no timestamp? Move back to pending.
                    logger.warning("👻 [REAPER] Ghost task found for %s. Re-queueing.", category)
                    await self.upstash.lrem(self.processing_queue, 1, category)
                    await self.upstash.lpush(self.pending_queue, category)
                    continue
                
                start_time = int(start_time_str)
                if (current_time - start_time) > self.visibility_timeout:
                    # Task timed out!
                    logger.warning("🧟 [REAPER] Zombie task detected for %s (timed out). Recovering...", category)
                    await self.upstash.lrem(self.processing_queue, 1, category)
                    await self.upstash._execute_command(["HDEL", self.visibility_map, category])
                    await self.upstash.lpush(self.pending_queue, category)

        except Exception as reaper_err:
            logger.error("❌ [REAPER] Error: %s", reaper_err)

async def run_worker():
    worker = WorkerManager()
    
    # Handle shutdown signals
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, worker.stop)
    
    await worker.start()

if __name__ == "__main__":
    asyncio.run(run_worker())
