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
                "url": article_data.get('url', ''),
                "title": article_data.get('title', ''),                     # NEW: Store for search retrieval
                "description": article_data.get('description', ''),         # NEW: Store for search retrieval
                "image": article_data.get('image', '')                      # NEW: Store for search retrieval
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

# Singleton Instance
vector_store = VectorStore()
