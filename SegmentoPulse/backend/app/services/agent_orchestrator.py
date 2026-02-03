"""
Agent Orchestrator Module - Reflex Architecture
------------------------------------------------
Handles the "Shadow Write Path" for Agentic RAG using asyncio.Queue pattern.

Architecture:
1. Producers: Push article IDs to queue (non-blocking)
2. Consumer: Single background worker processes queue serially
3. Rate Limiting: Global Groq API throttling (6 articles/min)
4. Non-Blocking: All CPU/IO operations wrapped in asyncio.to_thread()

This prevents:
- Event loop blocking
- Thundering herd 429 errors
- Memory accumulation from fire-and-forget tasks
"""

import asyncio
import logging
import os
import time
from typing import List, Dict, Any, Optional
from datetime import datetime

# Import the unified VectorStore singleton
from app.services.vector_store import vector_store as _vector_store

# Configure logging
logger = logging.getLogger(__name__)

# ============================================================================
# Global Configuration
# ============================================================================

# Groq API Rate Limiting
# Free Tier: 6000 tokens/min, ~500 tokens/article = 12 articles/min theoretical max
# We set 6 articles/min for safety buffer
GROQ_RATE_LIMIT = 6  # articles per minute
GROQ_MIN_INTERVAL = 60.0 / GROQ_RATE_LIMIT  # 10 seconds between calls
last_groq_call_time = 0.0
groq_rate_lock = asyncio.Lock()

# Space B Ollama Configuration (for fallback when Groq hits rate limits)
SPACE_B_OLLAMA_URL = "https://workwithshafisk-segmentopulse-factory.hf.space"
SPACE_B_TIMEOUT = 30  # seconds (Ollama on CPU is slower than Groq on TPU)

# Timeouts
AGENT_ANALYSIS_TIMEOUT = 30  # seconds
VECTOR_UPSERT_TIMEOUT = 15  # seconds

# Queue for Shadow Path
shadow_queue: Optional[asyncio.Queue] = None
shadow_worker_task: Optional[asyncio.Task] = None
_worker_running = False

class PulseAnalyst:
    """
    The 'Worker' Agent using CrewAI and Groq.
    """
    def __init__(self):
        self.agent = None
        self._initialized = False
        
    def _initialize(self):
        """Lazy init for CrewAI"""
        if self._initialized:
            return
            
        try:
            from crewai import Agent, Task, Crew
            from langchain_groq import ChatGroq
            
            api_key = os.getenv("GROQ_API_KEY")
            if not api_key:
                logger.warning("⚠️ [PulseAnalyst] GROQ_API_KEY not found. Agentic features disabled.")
                return

            # Initialize Groq LLM
            self.llm = ChatGroq(
                temperature=0,
                groq_api_key=api_key,
                model_name="llama-3.1-8b-instant"
            )
            
            # Define the Analyst Agent
            self.agent = Agent(
                role='Senior News Analyst',
                goal='Summarize technical news and extract latent categories',
                backstory="""You are an expert tech journalist for Segmento Pulse. 
                Your job is to read messy RSS feeds and turn them into crystal-clear, 
                categorized insights for busy developers.""",
                verbose=False,
                allow_delegation=False,
                llm=self.llm
            )
            
            self._initialized = True
            logger.info("🤖 [PulseAnalyst] Agent initialized with Llama-3 (Groq)")
            
        except Exception as e:
            logger.error("❌ [PulseAnalyst] Initialization failed: %s", e)
            self._initialized = False

    async def analyze(self, article_data: Dict) -> str:
        """
        Run the Agent on a single article with global rate limiting and timeout.
        
        Returns:
            Analysis text or fallback to description on error/timeout
        """
        if not self._initialized:
            self._initialize()
            
        if not self._initialized:
            return article_data.get('description', '')

        # Global rate limiting (prevent 429 errors)
        global last_groq_call_time
        async with groq_rate_lock:
            now = time.time()
            time_since_last = now - last_groq_call_time
            if time_since_last < GROQ_MIN_INTERVAL:
                sleep_time = GROQ_MIN_INTERVAL - time_since_last
                logger.debug(f"⏳ Rate limit throttle: sleeping {sleep_time:.1f}s")
                await asyncio.sleep(sleep_time)
            last_groq_call_time = time.time()

        retries = 2
        delay = 3  # Increased base delay

        for attempt in range(retries):
            try:
                from crewai import Task, Crew
                
                title = article_data.get('title', '')[:100]  # Limit title length
                desc = article_data.get('description', '')[:300]  # Limit desc length
                
                task_description = f"""
                Analyze this news article:
                Title: {title}
                Snippet: {desc}
                
                1. Summarize it in 2 sentences.
                2. Extract 3 key technical tags (e.g., 'Kubernetes', 'Antitrust', 'LLMs').
                3. Classify the sentiment (Positive/Neutral/Negative).
                
                Return ONLY the analysis text.
                """
                
                task = Task(
                    description=task_description,
                    agent=self.agent,
                    expected_output="A concise summary with tags and sentiment."
                )
                
                crew = Crew(
                    agents=[self.agent],
                    tasks=[task],
                    verbose=False
                )
                
                # Run with TIMEOUT (prevents infinite hangs)
                try:
                    result = await asyncio.wait_for(
                        asyncio.to_thread(crew.kickoff),  # Non-blocking!
                        timeout=AGENT_ANALYSIS_TIMEOUT
                    )
                    return str(result)
                    
                except asyncio.TimeoutError:
                    logger.warning(f"⏱️ [PulseAnalyst] Timeout ({AGENT_ANALYSIS_TIMEOUT}s): {title[:30]}")
                    return article_data.get('description', '')
                
            except Exception as e:
                error_str = str(e).lower()
                
                # Check if it's a rate limit error
                if '429' in error_str or 'rate limit' in error_str:
                    logger.warning(f"⚠️  [PulseAnalyst] Groq rate limit hit")
                    
                    # Try Space B Ollama fallback
                    try:
                        logger.info("🔄 [PulseAnalyst] Falling back to Space B Ollama...")
                        result = await self._analyze_with_space_b(article_data)
                        if result and result != article_data.get('description', ''):
                            logger.success("✅ Space B fallback successful!")
                            return result
                    except Exception as space_b_error:
                        logger.warning(f"⚠️  Space B fallback failed: {space_b_error}")
                    
                    # If Space B also fails, retry Groq after backoff
                    if attempt < retries - 1:
                        logger.warning(f"⏳ Retrying Groq in {delay}s...")
                        await asyncio.sleep(delay)
                        delay *= 2
                        continue
                
                logger.error(f"❌ [PulseAnalyst] Analysis failed: {e}")
                return article_data.get('description', '')
        
        return article_data.get('description', '')
    
    async def _analyze_with_space_b(self, article_data: Dict) -> str:
        """
        Fallback to Space B Ollama when Groq hits rate limits.
        
        Space B runs Ollama on CPU (slower but no rate limits).
        
        Args:
            article_data: Article dict with title, description, url
            
        Returns:
            Analysis text or empty string on failure
        """
        import httpx
        
        try:
            title = article_data.get('title', '')[:200]
            description = article_data.get('description', '')[:500]
            url = article_data.get('url', '')
            
            # Call Space B /process-article endpoint
            # This is the same endpoint used by ingestion_v2.py
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{SPACE_B_OLLAMA_URL}/process-article",
                    json={
                        "url": url,
                        "title": title,
                        "content": description
                    },
                    timeout=SPACE_B_TIMEOUT
                )
                
                if response.status_code != 200:
                    logger.warning(f"Space B returned {response.status_code}")
                    return ""
                
                result = response.json()
                summary = result.get('summary', '')
                tags = result.get('tags', [])
                
                # Format similar to Groq output
                if summary:
                    formatted = f"{summary}"
                    if tags:
                        formatted += f"\n\nTags: {', '.join(tags[:5])}"
                    return formatted
                
                return ""
                
        except Exception as e:
            logger.error(f"❌ Space B Ollama call failed: {e}")
            return ""

# Singleton Instances
_pulse_analyst = PulseAnalyst()

# ============================================================================
# Async Queue Worker Pattern
# ============================================================================

async def _shadow_path_worker():
    """
    Background worker that processes the shadow queue serially.
    This runs as a single long-lived task, preventing thundering herd.
    """
    global _worker_running, shadow_queue
    
    logger.info("👷 [Shadow Worker] Started background worker thread")
    _worker_running = True
    
    processed_count = 0
    error_count = 0
    
    while _worker_running:
        try:
            # Wait for next article (timeout to allow graceful shutdown)
            try:
                article = await asyncio.wait_for(shadow_queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                # No items in queue, continue loop
                continue
            
            try:
                # 1. AI Analysis (with global rate limiting + timeout)
                title = article.get('title', '')[:50]
                logger.debug(f"   🧐 Analyzing: {title}")
                
                analysis = await _pulse_analyst.analyze(article)
                
                logger.info(f"🤖 [Shadow Worker] Analyzed: '{title}'")
                logger.info(f"   → Generated {len(analysis)} chars of insight")
                
                # 2. Vector Embedding + ChromaDB Upsert (NON-BLOCKING)
                # This was the main blocking culprit!
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(_vector_store.upsert_article, article, analysis),
                        timeout=VECTOR_UPSERT_TIMEOUT
                    )
                    processed_count += 1
                    
                except asyncio.TimeoutError:
                    logger.error(f"   ⏱️ Vector upsert timeout ({VECTOR_UPSERT_TIMEOUT}s): {title}")
                    error_count += 1
                
            except Exception as e:
                logger.error(f"   ❌ Shadow processing failed: {e}")
                error_count += 1
            
            finally:
                shadow_queue.task_done()
                
                # Log progress every 10 articles
                if (processed_count + error_count) % 10 == 0:
                    logger.info(f"📊 [Shadow Stats] Processed: {processed_count}, Errors: {error_count}, Queue: {shadow_queue.qsize()}")
        
        except Exception as e:
            logger.exception(f"❌ [Shadow Worker] Unexpected error: {e}")
            await asyncio.sleep(1)  # Prevent tight loop on persistent errors
    
    logger.info(f"🏁 [Shadow Worker] Shutting down. Final stats: {processed_count} processed, {error_count} errors")


def start_shadow_worker():
    """
    Initialize and start the shadow path background worker.
    Call this once on startup.
    """
    global shadow_queue, shadow_worker_task
    
    if shadow_queue is None:
        shadow_queue = asyncio.Queue(maxsize=1000)  # Buffer up to 1000 articles
        logger.info("✅ [Shadow Worker] Queue initialized (maxsize=1000)")
    
    if shadow_worker_task is None or shadow_worker_task.done():
        shadow_worker_task = asyncio.create_task(_shadow_path_worker())
        logger.info("✅ [Shadow Worker] Background task started")


async def stop_shadow_worker():
    """
    Gracefully stop the shadow worker.
    Waits for queue to drain first.
    """
    global _worker_running, shadow_queue, shadow_worker_task
    
    logger.info("⏸️  [Shadow Worker] Stopping... waiting for queue to drain")
    _worker_running = False
    
    if shadow_queue:
        await shadow_queue.join()  # Wait for all items to be processed
    
    if shadow_worker_task:
        shadow_worker_task.cancel()
        try:
            await shadow_worker_task
        except asyncio.CancelledError:
            pass
    
    logger.info("✅ [Shadow Worker] Stopped gracefully")


async def process_shadow_path(articles: List[Dict]):
    """
    Entry point for the Shadow Write Path.
    Now uses Queue pattern instead of fire-and-forget asyncio.gather.
    
    This function is FAST - it just pushes to queue and returns immediately.
    The actual processing happens in the background worker.
    """
    global shadow_queue
    
    if not articles:
        return
    
    # Ensure worker is started
    if shadow_queue is None or shadow_worker_task is None or shadow_worker_task.done():
        start_shadow_worker()
    
    logger.info(f"🔵 [Shadow Path] Enqueuing {len(articles)} articles for background processing")
    
    enqueued = 0
    dropped = 0
    
    for article in articles:
        try:
            # Non-blocking put (drop if queue is full to prevent backpressure)
            shadow_queue.put_nowait(article)
            enqueued += 1
        except asyncio.QueueFull:
            dropped += 1
            logger.warning(f"⚠️  [Shadow Path] Queue full! Dropped article: {article.get('title', '')[:30]}")
    
    logger.info(f"✅ [Shadow Path] Enqueued: {enqueued}, Dropped: {dropped}, Queue size: {shadow_queue.qsize()}")
