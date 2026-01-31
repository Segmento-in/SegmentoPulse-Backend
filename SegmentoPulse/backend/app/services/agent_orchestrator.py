"""
Agent Orchestrator Module
-------------------------
Handles the "Shadow Write Path" for Agentic RAG.
This module is responsible for:
1. Asynchronous background processing of articles (Fire-and-Forget).
2. AI Analysis using CrewAI + Groq (Llama-3).
3. Vector Storage using ChromaDB + SentenceTransformers.
"""

import asyncio
import logging
import os
from typing import List, Dict, Any, Optional
from datetime import datetime

# Import the unified VectorStore singleton
from app.services.vector_store import vector_store as _vector_store

# Configure logging
logger = logging.getLogger(__name__)

# Global Semaphore to control concurrency (prevent Rate Limits)
# Reduced from 5 to 2 to stay within Groq's 6000 TPM limit
MAX_CONCURRENT_TASKS = 2
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

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
        Run the Agent on a single article.
        Includes manual Retry/Backoff for Rate Limits.
        """
        if not self._initialized:
            self._initialize()
            
        if not self._initialized:
            return article_data.get('description', '')

        retries = 3
        delay = 2

        for attempt in range(retries):
            try:
                from crewai import Task, Crew
                
                # Construct a prompt/task
                title = article_data.get('title', '')
                desc = article_data.get('description', '')
                
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
                
                # CrewAI is synchronous, so we run it in a thread to be non-blocking
                # This is crucial for the "Fire-and-Forget" pattern
                crew = Crew(
                    agents=[self.agent],
                    tasks=[task],
                    verbose=False
                )
                
                # Run blocking call in default executor
                result = await asyncio.to_thread(crew.kickoff)
                
                return str(result)
                
            except Exception as e:
                error_str = str(e).lower()
                if '429' in error_str or 'rate limit' in error_str:
                    if attempt < retries - 1:
                        logger.warning("⚠️ [PulseAnalyst] Rate Limit (429). Retrying in %ds...", delay)
                        await asyncio.sleep(delay)
                        delay *= 2 # Exponential backoff
                        continue
                
                logger.error("❌ [PulseAnalyst] Analysis failed: %s", e)
                return article_data.get('description', '')
        
        return article_data.get('description', '')


# Singleton Instances
_pulse_analyst = PulseAnalyst()

async def process_shadow_path(articles: List[Dict]):
    """
    Entry point for the Shadow Write Path.
    This function is called by the scheduler via asyncio.create_task() (Fire-and-Forget).
    """
    if not articles:
        return

    logger.info("🕵️ [Shadow Path] Background processing started for %d articles...", len(articles))
    
    async def process_single(article):
        async with semaphore: # Enforce Rate Limits (max 5 concurrent)
            try:
                # 1. Analyze (Agent)
                logger.debug("   ...Analyzing: %s", article.get('title')[:20])
                analysis = await _pulse_analyst.analyze(article)
                
                # Observability: Log Agent Output
                logger.info("🤖 [Agent Analysis] Article: '%s'", article.get('title', '')[:30])
                logger.info("   -> Generated %d chars of insight", len(analysis))
                
                # 2. Embed & Save (VectorStore)
                # Note: upsert_article might block if it does heavy CPU work? 
                # Actually sentence-transformers on CPU is blocking. 
                # Ideally we should potentially await to_thread this too if it's slow.
                # But for now we keep it as is since it's inside the async gather.
                # Just need to check if _vector_store.upsert_article is async or sync.
                # It is defined as sync in the class we moved.
                # We can wrap it in to_thread if we want true non-blocking, but for now we follow existing pattern.
                _vector_store.upsert_article(article, analysis)
                
            except Exception as e:
                logger.error("   ❌ Failed to process shadow article: %s", e)

    # Launch all tasks
    tasks = [process_single(art) for art in articles]
    await asyncio.gather(*tasks)
    
    logger.info("🏁 [Shadow Path] Completed background processing.")
