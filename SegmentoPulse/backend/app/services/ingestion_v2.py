"""
Ingestion Engine v2 - Custom Document Pipeline + Bloom Filter

News ingestion pipeline with hardcoded LlamaIndex value:
- Custom Document objects for standardized data structure
- Feedparser for robust RSS parsing
- Bloom Filter for URL deduplication
- Parallel processing for high throughput

No external LlamaIndex dependency - we implement the concepts ourselves.
"""

import asyncio
from datetime import datetime
from typing import List, Dict, Optional
import logging
import feedparser
import requests
import hashlib

# Custom Document class (replaces LlamaIndex)
from app.services.document import Document, create_document_from_rss_entry
from app.services.chunker import SentenceSplitter

from app.models import Article  
from app.services.deduplication import get_url_filter
from app.services.vector_store import vector_store

logger = logging.getLogger(__name__)


# ============================================================================
# Space B Configuration
# ============================================================================

SPACE_B_URL = "https://workwithshafisk-segmentopulse-backend.hf.space"
SPACE_B_TIMEOUT = 30  # seconds (Llama-3 is slow on CPU)


# RSS feed URLs for each category
CATEGORY_RSS_FEEDS = {
    "ai": [
        "https://news.google.com/rss/search?q=artificial+intelligence&hl=en-US&gl=US&ceid=US:en",
        "https://venturebeat.com/category/ai/feed/",
        "https://www.theverge.com/ai-artificial-intelligence/rss/index.xml"
    ],
    "data-security": [
        "https://news.google.com/rss/search?q=data+security+cybersecurity&hl=en-US&gl=US&ceid=US:en",
        "https://feeds.feedburner.com/TheHackersNews"
    ],
    "data-governance": [
        "https://news.google.com/rss/search?q=data+governance&hl=en-US&gl=US&ceid=US:en"
    ],
    "data-privacy": [
        "https://news.google.com/rss/search?q=data+privacy+GDPR&hl=en-US&gl=US&ceid=US:en"
    ],
    "data-engineering": [
        "https://news.google.com/rss/search?q=data+engineering&hl=en-US&gl=US&ceid=US:en"
    ],
    "data-management": [
        "https://news.google.com/rss/search?q=data+management&hl=en-US&gl=US&ceid=US:en"
    ],
    "business-intelligence": [
        "https://news.google.com/rss/search?q=business+intelligence&hl=en-US&gl=US&ceid=US:en"
    ],
    "business-analytics": [
        "https://news.google.com/rss/search?q=business+analytics&hl=en-US&gl=US&ceid=US:en"
    ],
    "customer-data-platform": [
        "https://news.google.com/rss/search?q=customer+data+platform+CDP&hl=en-US&gl=US&ceid=US:en"
    ],
    "data-centers": [
        "https://news.google.com/rss/search?q=data+centers&hl=en-US&gl=US&ceid=US:en"
    ],
    "cloud-computing": [
        "https://news.google.com/rss/search?q=cloud+computing&hl=en-US&gl=US&ceid=US:en"
    ],
    "magazines": [
        "https://news.google.com/rss/search?q=technology+magazine&hl=en-US&gl=US&ceid=US:en"
    ],
    "data-laws": [
        "https://news.google.com/rss/search?q=data+privacy+laws+regulations&hl=en-US&gl=US&ceid=US:en"
    ],
    # Official Cloud Providers
    "cloud-aws": [
        "https://aws.amazon.com/blogs/aws/feed/"
    ],
    "cloud-azure": [
        "https://azure.microsoft.com/en-us/blog/feed/"
    ],
    "cloud-gcp": [
        "https://cloudblog.withgoogle.com/rss/"
    ],
    "cloud-oracle": [
        "https://blogs.oracle.com/cloud-infrastructure/rss"
    ],
    "cloud-ibm": [
        "https://www.ibm.com/blog/rss"
    ],
    "cloud-alibaba": [
        "https://www.alibabacloud.com/blog/rss.xml"
    ],
    "cloud-digitalocean": [
        "https://www.digitalocean.com/blog/rss.xml"
    ],
    "cloud-huawei": [
        "https://news.google.com/rss/search?q=huawei+cloud&hl=en-US&gl=US&ceid=US:en"
    ],
    "cloud-cloudflare": [
        "https://blog.cloudflare.com/rss/"
    ]
}


async def fetch_category_rss(category: str, rss_urls: List[str]) -> List[Document]:
    """
    Fetch RSS feeds for a category using feedparser + custom Document
    
    Args:
        category: News category
        rss_urls: List of RSS feed URLs
        
    Returns:
        List of custom Document objects
    """
    try:
        logger.info(f"📡 [CUSTOM PARSER] Fetching RSS for {category.upper()}...")
        
        all_documents = []
        
        # Fetch each RSS feed
        for url in rss_urls:
            try:
                # Parse RSS feed with feedparser
                feed = await asyncio.to_thread(feedparser.parse, url)
                
                # Convert each entry to Document
                for entry in feed.entries:
                    doc = create_document_from_rss_entry(
                        entry=entry,
                        category=category,
                        source_feed=url
                    )
                    all_documents.append(doc)
                
                logger.debug(f"   ✓ Fetched {len(feed.entries)} articles from {url[:50]}...")
                
            except Exception as e:
                logger.warning(f"   ⚠️  Failed to fetch {url}: {e}")
                continue
        
        logger.info(f"   ✅ Total fetched: {len(all_documents)} documents for {category}")
        return all_documents
        
    except Exception as e:
        logger.error(f"❌ Error fetching category {category}: {e}")
        return []


def convert_llamaindex_to_article(doc: Document, category: str) -> Optional[Article]:
    """
    Convert LlamaIndex Document to Article model
    
    Args:
        doc: LlamaIndex Document object
        category: News category
        
    Returns:
        Article object or None if conversion fails
    """
    try:
        metadata = doc.metadata or {}
        
        # Extract fields from metadata
        title = metadata.get('title', '')[:200]
        url = metadata.get('link') or metadata.get('url', '')
        description = doc.text[:500] if doc.text else metadata.get('description', '')[:500]
        published_at = metadata.get('published', datetime.now().isoformat())
        source = metadata.get('source') or metadata.get('author', 'Unknown')
        
        # Basic validation
        if not title or not url:
            return None
        
        # Create Article
        article = Article(
            title=title,
            description=description,
            url=url,
            image=metadata.get('image', ''),
            publishedAt=published_at,
            source=source,
            category=category
        )
        
        return article
        
    except Exception as e:
        logger.error(f"❌ Error converting document to article: {e}")
        return None


async def fetch_latest_news(categories: List[str]) -> Dict[str, List[Article]]:
    """
    Main ingestion function using Custom Document + Bloom Filter
    
    Fetches news for multiple categories in parallel, deduplicates URLs,
    and returns structured Article objects.
    
    Args:
        categories: List of category names to fetch
        
    Returns:
        Dictionary mapping category -> List[Article]
    """
    start_time = datetime.now()
    
    logger.info("═" * 80)
    logger.info("🚀 [INGESTION V2] Starting Custom Document ingestion...")
    logger.info(f"🕐 Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"📂 Categories: {len(categories)}")
    logger.info("═" * 80)
    
    # Get URL filter for deduplication
    url_filter = get_url_filter()
    
    # Statistics tracking
    total_fetched = 0
    total_deduped = 0
    total_converted = 0
    
    results = {}
    
    # Fetch each category in parallel
    fetch_tasks = []
    for category in categories:
        rss_urls = CATEGORY_RSS_FEEDS.get(category, [])
        if not rss_urls:
            logger.warning(f"⚠️  No RSS feeds configured for {category}")
            continue
        
        task = fetch_category_rss(category, rss_urls)
        fetch_tasks.append((category, task))
    
    # Execute all fetches concurrently
    logger.info(f"⚡ Launching {len(fetch_tasks)} parallel fetch tasks...")
    
    for category, task in fetch_tasks:
        try:
            # Await each task
            documents = await task
            total_fetched += len(documents)
            
            # Deduplicate and convert to Article objects
            articles = []
            duplicates = 0
            
            for doc in documents:
                # Extract URL for deduplication
                url = doc.metadata.get('link') or doc.metadata.get('url', '')
                
                if not url:
                    continue
                
                # Check if URL is new
                if url_filter.check_and_add(url):
                    # New URL - convert to Article
                    article = convert_llamaindex_to_article(doc, category)
                    if article:
                        articles.append(article)
                        total_converted += 1
                else:
                    # Duplicate URL - skip
                    duplicates += 1
                    total_deduped += 1
            
            results[category] = articles
            
            logger.info(f"✅ {category.upper()}: {len(documents)} fetched, {len(articles)} new, {duplicates} duplicates")
            
        except Exception as e:
            logger.error(f"❌ Error processing {category}: {e}")
            results[category] = []
    
    # End-of-run summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("")
    logger.info("═" * 80)
    logger.info("🎉 [INGESTION V2] RUN COMPLETED")
    logger.info("═" * 80)
    logger.info("📊 SUMMARY STATISTICS:")
    logger.info(f"   🔹 Total Fetched: {total_fetched} documents")
    logger.info(f"   🔹 Total Converted: {total_converted} articles")
    logger.info(f"   🔹 Total Duplicates Skipped: {total_deduped} articles")
    logger.info(f"   🔹 Deduplication Rate: {(total_deduped / total_fetched * 100) if total_fetched > 0 else 0:.1f}%")
    logger.info("")
    logger.info("⏱️  PERFORMANCE:")
    logger.info(f"   🔹 Duration: {duration:.2f} seconds")
    logger.info(f"   🔹 Throughput: {total_fetched / duration if duration > 0 else 0:.1f} docs/second")
    logger.info("═" * 80)
    
    # Print URL filter stats
    url_filter.print_stats()
    
    return results


# ============================================================================
# Article Processing with Space B + ChromaDB
# ============================================================================

async def process_and_store_article(url: str, raw_text: str, category: str, title: str = "") -> Optional[Dict]:
    """
    Phase 2 CQRS: Offload processing to Space B, then store in ChromaDB
    
    Architecture:
    1. Send raw_text to Space B's /process-article endpoint
    2. Receive summary + tags from Space B
    3. Generate embeddings locally using sentence-transformers
    4. Store in ChromaDB
    
    Args:
        url: Article URL (used as ID)
        raw_text: Full article content
        category: Article category
        title: Article title (optional)
        
    Returns:
        Dictionary with processing results or None on error
    """
    try:
        logger.info(f"🏭 [SPACE A→B] Processing: {url[:60]}...")
        
        # -------------------------------------------------------------------------
        # Step 1: Call Space B for summarization + entity extraction
        # -------------------------------------------------------------------------
        try:
            response = requests.post(
                f"{SPACE_B_URL}/process-article",
                json={
                    "text": raw_text[:5000],  # Limit to avoid token limits
                    "max_tokens": 150,
                    "temperature": 0.7,
                    "entity_labels": ["Person", "Organization", "Location", "Technology", "Product", "Event"],
                    "entity_threshold": 0.5
                },
                timeout=SPACE_B_TIMEOUT
            )
            
            if response.status_code != 200:
                logger.warning(f"⚠️  Space B returned {response.status_code}: {response.text[:200]}")
                return None
                
            space_b_result = response.json()
            summary = space_b_result.get("summary", "")
            tags = space_b_result.get("tags", [])
            
            logger.info(f"✅ Space B processed: {len(summary)} char summary, {len(tags)} tags")
            
        except requests.exceptions.Timeout:
            logger.warning(f"⏳ Space B timeout (cold start?): {url[:50]}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Space B connection error: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Space B processing error: {e}")
            return None
        
        # -------------------------------------------------------------------------
        # Step 2: Generate embeddings locally with sentence-transformers
        # -------------------------------------------------------------------------
        # ChromaDB vector_store has embedded model (all-MiniLM-L6-v2)
        # We'll use the existing upsert_article method
        
        # -------------------------------------------------------------------------
        # Step 3: Prepare article data for ChromaDB
        # -------------------------------------------------------------------------
        url_hash = hashlib.md5(url.encode()).hexdigest()
        
        article_data = {
            "$id": url_hash,
            "title": title or summary[:100],  # Use title if available, else first part of summary
            "description": summary,
            "url": url,
            "source": "Segmento AI",
            "category": category,
            "published_at": datetime.now().isoformat(),
            "image": "",  # No image for now
            "tags": tags
        }
        
        # -------------------------------------------------------------------------
        # Step 4: Store in ChromaDB
        # -------------------------------------------------------------------------
        # Create combined text: Title + Summary + Tags (for richer embeddings)
        tags_text = " ".join(tags) if tags else ""
        combined_analysis = f"Summary: {summary}\nTags: {tags_text}"
        
        # Upsert to vector store (handles embedding generation internally)
        vector_store.upsert_article(article_data, combined_analysis)
        
        logger.info(f"🧠 [ChromaDB] Stored: {title[:50] if title else url[:50]}")
        
        return {
            "url": url,
            "summary": summary,
            "tags": tags,
            "stored": True
        }
        
    except Exception as e:
        logger.error(f"❌ [CQRS] Processing failed for {url}: {e}")
        return None


async def fetch_single_category(category: str) -> List[Article]:
    """
    Convenience function to fetch a single category
    
    Args:
        category: Category name
        
    Returns:
        List of Article objects
    """
    results = await fetch_latest_news([category])
    return results.get(category, [])
