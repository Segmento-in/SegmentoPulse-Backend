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

# Configure logging
logger = logging.getLogger(__name__)

# Global Semaphore to control concurrency (prevent Rate Limits)
# Only allow 5 concurrent AI analysis tasks at a time
MAX_CONCURRENT_TASKS = 5
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

class VectorStore:
    """
    Manages interactions with ChromaDB.
    """
    def __init__(self):
        self.client = None
        self.collection = None
        self._initialized = False

    def _initialize(self):
        """Lazy initialization of ChromaDB to avoid startup overhead"""
        if self._initialized:
            return

        try:
            import chromadb
            from chromadb.config import Settings
            from sentence_transformers import SentenceTransformer
            
            # Initialize persistent client
            db_path = os.path.join(os.getcwd(), "chroma_db")
            self.client = chromadb.PersistentClient(path=db_path)
            
            # Create or get collection
            self.collection = self.client.get_or_create_collection(
                name="segmento_pulse_news",
                metadata={"hnsw:space": "cosine"}
            )
            
            # Initialize embedding model (cpu is fine for small batches)
            self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
            
            self._initialized = True
            logger.info("✅ [ChromaDB] Vector Store initialized successfully at %s", db_path)
            
        except Exception as e:
            logger.error("❌ [ChromaDB] Initialization failed: %s", e)
            self._initialized = False

    def upsert_article(self, article_data: Dict, analysis_result: str):
        """
        Convert article + analysis into vector and save to ChromaDB.
        """
        if not self._initialized:
            self._initialize()
        
        if not self._initialized:
            return

        try:
            # Prepare text for embedding: Title + Summary + Analysis
            # We treat the "analysis" as high-value semantic content
            combined_text = f"{article_data.get('title', '')} \n {article_data.get('description', '')} \n {analysis_result}"
            
            # Generate embedding
            embedding = self.embedder.encode(combined_text).tolist()
            
            # Metadata for filtering
            metadata = {
                "source": article_data.get('source', 'Unknown'),
                "category": article_data.get('category', 'General'),
                "published_at": str(article_data.get('published_at', '')),
                "url": article_data.get('url', '')
            }
            
            # Upsert to ChromaDB
            # Use Appwrite Document ID ($id) as the ChromaDB ID for 1:1 mapping
            doc_id = article_data.get('$id')
            if not doc_id:
                # Fallback if no ID provided (shouldn't happen with shadow path)
                doc_id = article_data.get('url_hash', 'unknown')

            self.collection.upsert(
                ids=[doc_id],
                embeddings=[embedding],
                metadatas=[metadata],
                documents=[combined_text] # Optional: store raw text for debugging
            )
            
            logger.info("🧠 [ChromaDB] Upserted vector for: %s", article_data.get('title')[:30])
            
        except Exception as e:
            logger.error("❌ [ChromaDB] Upsert failed: %s", e)


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
                model_name="llama3-8b-8192"
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
        """
        if not self._initialized:
            self._initialize()
            
        if not self._initialized:
            return article_data.get('description', '')

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
            logger.error("❌ [PulseAnalyst] Analysis failed: %s", e)
            return article_data.get('description', '')


# Singleton Instances
_vector_store = VectorStore()
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
                
                # 2. Embed & Save (VectorStore)
                _vector_store.upsert_article(article, analysis)
                
            except Exception as e:
                logger.error("   ❌ Failed to process shadow article: %s", e)

    # Launch all tasks
    tasks = [process_single(art) for art in articles]
    await asyncio.gather(*tasks)
    
    logger.info("🏁 [Shadow Path] Completed background processing.")
