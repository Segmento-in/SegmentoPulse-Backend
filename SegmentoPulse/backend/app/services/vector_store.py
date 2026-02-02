"""
Vector Store Service
--------------------
Manages interactions with ChromaDB for the Segmento Pulse application.
Handles semantic embedding and retrieval of news articles.
"""

import os
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)

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
            # from chromadb.config import Settings # Not strictly needed with PersistentClient
            from sentence_transformers import SentenceTransformer
            
            # Initialize persistent client
            # We use an absolute path or relative to CWD.
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
            # Retrieve collection count for observability
            count = self.collection.count()
            logger.info("✅ [ChromaDB] Vector Store initialized successfully at %s", db_path)
            logger.info("📊 [ChromaDB Observability] Current Vector Count: %d", count)
            
        except Exception as e:
            logger.error("❌ [ChromaDB] Initialization failed: %s", e)
            self._initialized = False

    def upsert_article(self, article_data: Dict, analysis_result: str):
        """
        Phase 3: Enhanced vector storage with rich metadata
        
        Converts article + AI analysis into searchable vector with:
        - Optimized embedding format: "{Title} : {Summary}"
        - Cloud news detection
        - Engagement metrics (likes, views)
        - Time-aware sorting (Unix timestamp)
        - Tag-based filtering (GLiNER output)
        """
        if not self._initialized:
            self._initialize()
        
        if not self._initialized:
            return

        try:
            # Import HTML stripping utility
            from app.utils import strip_html_if_needed
            import time
            
            # Clean text (only strips if HTML detected)
            title_clean = strip_html_if_needed(article_data.get('title', ''))
            desc_clean = strip_html_if_needed(article_data.get('description', ''))
            
            # Phase 3: Optimized Combined Embedding
            # Format: "{Title} : {Summary}"
            # The colon separator helps the model distinguish title from body
            text_to_embed = f"{title_clean} : {analysis_result}"
            
            # Observability: Log what we are embedding
            logger.info("📝 [Index] Embedding Article: '%s'", title_clean[:50])
            logger.info("   -> Format: '{Title} : {Summary}'")
            logger.info("   -> Total Length: %d chars", len(text_to_embed))
            
            # Generate embedding
            embedding = self.embedder.encode(text_to_embed).tolist()
            
            # Phase 3: Enhanced Metadata Schema
            metadata = {
                # Core identification
                "source": article_data.get('source', 'Unknown'),
                "category": article_data.get('category', 'General'),
                "url": article_data.get('url', ''),
                
                # Display data (cleaned)
                "title": title_clean,
                "description": desc_clean,
                "image": article_data.get('image', ''),
                
                # Phase 3: Filtering & Search
                "tags": article_data.get('tags', ''),  # GLiNER output (comma-separated)
                
                # Phase 3: Time-aware ranking
                "timestamp": int(time.time()),  # Unix timestamp (numeric, sortable)
                "published_at": str(article_data.get('published_at', '')),  # ISO string
                
                # Phase 3: Future features
                "audio_url": "",  # Placeholder for TTS
                
                # Phase 3: Cloud detection
                "is_cloud_news": article_data.get('is_cloud_news', False),
                "cloud_provider": article_data.get('cloud_provider', ''),  # "aws", "azure", etc.
                "is_official": article_data.get('is_official', False),  # True if official blog
                
                # Phase 3: Engagement metrics (for ranking)
                "likes": article_data.get('likes', 0),
                "dislikes": article_data.get('dislikes', 0),
                "views": article_data.get('views', 0),
                
                # Phase 3: Schema versioning
                "processing_version": "v2_phase3"
            }
            
            # Phase 3: Document field = Llama-3 summary ONLY (not original HTML)
            document = analysis_result
            
            # Upsert to ChromaDB
            # Use Appwrite Document ID ($id) as the ChromaDB ID for 1:1 mapping
            doc_id = article_data.get('$id')
            if not doc_id:
                # Fallback if no ID provided
                doc_id = article_data.get('url_hash', 'unknown')

            self.collection.upsert(
                ids=[doc_id],
                embeddings=[embedding],
                metadatas=[metadata],
                documents=[document]
            )
            
            # Enhanced logging
            cloud_status = "☁️ CLOUD" if metadata['is_cloud_news'] else "📰 REGULAR"
            logger.info("🧠 [ChromaDB] Upserted: %s | %s | Tags: %s", 
                       title_clean[:30], 
                       cloud_status,
                       metadata['tags'][:30] if metadata['tags'] else 'None')
            
        except Exception as e:
            logger.error("❌ [ChromaDB] Upsert failed: %s", e)


    def search_articles(self, query: str, limit: int = 10) -> List[Dict]:
        """
        Semantic Search: Find articles conceptually similar to the query.
        Returns a list of articles in the format expected by the frontend.
        """
        if not self._initialized:
            self._initialize()
            
        if not self._initialized or not self.collection:
            return []

        try:
            # Generate embedding for query
            query_embedding = self.embedder.encode(query).tolist()
            
            # Query ChromaDB
            results = self.collection.query(
                query_embeddings=[query_embedding],
                n_results=limit
            )
            
            if not results['ids'] or not results['ids'][0]:
                return []
                
            # Parse results into list of dicts
            articles = []
            
            # Chroma returns list of lists (one per query)
            ids = results['ids'][0]
            metadatas = results['metadatas'][0]
            distances = results['distances'][0]
            
            for i, doc_id in enumerate(ids):
                meta = metadatas[i]
                
                # Filter out low relevance (distance > 1.5 roughly implies low similarity for Cosine)
                # Lower distance = better match
                if distances[i] > 1.2: 
                    continue
                    
                article = {
                    "$id": doc_id,
                    "title": meta.get("title", "Untitled"),
                    "description": meta.get("description", ""),
                    "url": meta.get("url", "#"),
                    "source": meta.get("source", "Segmento AI"),
                    "publishedAt": meta.get("published_at", ""),
                    "image": meta.get("image", "/placeholder.png"),
                    "category": meta.get("category", "General"),
                    # Add relevance score for debugging?
                    "_relevance": round(1 - distances[i], 2) # Crude approximation
                }
                articles.append(article)
                
            logger.info("🧠 [ChromaDB] Search '%s' found %d semantic matches", query, len(articles))
            return articles
            
        except Exception as e:
            logger.error("❌ [ChromaDB] Search failed: %s", e)
            return []

    def delete_vector(self, doc_id: str):
        """
        Remove a vector from ChromaDB by ID.
        Used by the cleanup janitor to prevent 'Zombie Vectors'.
        """
        if not self._initialized:
            self._initialize()
            
        if not self._initialized or not self.collection:
            return

        try:
            self.collection.delete(ids=[doc_id])
            logger.info("🗑️  [ChromaDB] Deleted vector: %s", doc_id)
        except Exception as e:
            logger.error("❌ [ChromaDB] Delete failed for %s: %s", doc_id, e)

# Singleton Instance
vector_store = VectorStore()
