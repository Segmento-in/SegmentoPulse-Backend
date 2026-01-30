import os
import logging
from typing import List, Dict, Optional
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)

# Global singleton
_vector_store = None

class ChromaVectorStore:
    """
    ChromaDB Vector Store Service
    
    Persists news articles with vector embeddings for semantic search.
    Hosted on HuggingFace Spaces (Persistent Storage).
    """
    
    def __init__(self, persist_directory: str = "./chroma_db"):
        self.persist_directory = persist_directory
        self.client = None
        self.collection = None
        self.embedding_function = None
        self.initialized = False
        
        self._initialize()

    def _initialize(self):
        """Initialize ChromaDB client and collection"""
        try:
            import chromadb
            from chromadb.utils import embedding_functions
            
            # 1. Initialize Persistent Client
            # settings = chromadb.Settings(allow_reset=True, anonymized_telemetry=False)
            self.client = chromadb.PersistentClient(path=self.persist_directory)
            
            # 2. Setup Embedding Function (Sentence Transformers)
            # Uses 'all-MiniLM-L6-v2' by default (small, fast, good for news)
            self.embedding_function = embedding_functions.SentenceTransformerEmbeddingFunction(
                model_name="all-MiniLM-L6-v2"
            )
            
            # 3. Get or Create Collection
            self.collection = self.client.get_or_create_collection(
                name="news_articles",
                embedding_function=self.embedding_function,
                metadata={"hnsw:space": "cosine"} # Cosine similarity for text search
            )
            
            self.initialized = True
            logger.info(f"✅ [ChromaDB] Initialized at {self.persist_directory}")
            
        except ImportError:
            logger.error("❌ [ChromaDB] 'chromadb' or 'sentence-transformers' not installed.")
            self.initialized = False
        except Exception as e:
            logger.error(f"❌ [ChromaDB] Initialization failed: {e}")
            self.initialized = False

    async def upsert_article(self, article: Dict) -> bool:
        """
        Embed and save an article to ChromaDB
        
        Args:
            article: Dict containing title, description, url, etc.
        """
        if not self.initialized:
            return False
            
        try:
            # Construct the text to embed (Title + Description is usually best)
            # We treat 'title' as having higher semantic weight, but concatenation is standard.
            text_to_embed = f"{article.get('title', '')}. {article.get('description', '')}"
            
            # Metadata for filtering
            # Chroma requires flat dicts (str, int, float, bool)
            metadata = {
                "source": article.get('source', 'Unknown'),
                "category": article.get('category', 'general'),
                "url": article.get('url', ''),
                "published_at": article.get('publishedAt', str(datetime.now())),
                "image_url": article.get('image', '') # Store to return in search results
            }
            
            # Upsert (Update if ID exists, Insert if new)
            self.collection.upsert(
                documents=[text_to_embed],
                metadatas=[metadata],
                ids=[article.get('url')] # URL as unique ID
            )
            return True
            
        except Exception as e:
            logger.error(f"⚠️ [ChromaDB] Upsert failed for {article.get('url')}: {e}")
            return False

    async def search(self, query: str, n_results: int = 10, category_filter: Optional[str] = None) -> List[Dict]:
        """
        Semantic Search execution
        
        Args:
            query: User's search text
            n_results: Number of matches
            category_filter: Optional category to restrict search (AWS, AI, etc.)
        """
        if not self.initialized:
            return []
            
        try:
            where_clause = {}
            if category_filter:
                where_clause = {"category": category_filter}
                
            results = self.collection.query(
                query_texts=[query],
                n_results=n_results,
                where=where_clause if category_filter else None
                # include=['documents', 'metadatas', 'distances'] # Default
            )
            
            # Format results for Frontend
            formatted_results = []
            if results['ids']:
                for i in range(len(results['ids'][0])):
                    meta = results['metadatas'][0][i]
                    formatted_results.append({
                        "id": results['ids'][0][i],
                        "title": meta.get('url'), # Metadata unfortunately doesn't strictly enforce title storage unless we put it there. 
                                                  # NOTE: For V1 we relied on URL, but we should verify metadata contents.
                                                  # Improved Strategy: We define output based on stored metadata.
                        "url": meta.get('url'),
                        "source": meta.get('source'),
                        "category": meta.get('category'),
                        "publishedAt": meta.get('published_at'),
                        "image": meta.get('image_url'),
                        "relevance_score": 1 - results['distances'][0][i] # Convert distance to similarity
                    })
            
            return formatted_results
            
        except Exception as e:
            logger.error(f"❌ [ChromaDB] Search failed: {e}")
            return []

def get_vector_store() -> ChromaVectorStore:
    """Singleton accessor"""
    global _vector_store
    if _vector_store is None:
        _vector_store = ChromaVectorStore()
    return _vector_store
