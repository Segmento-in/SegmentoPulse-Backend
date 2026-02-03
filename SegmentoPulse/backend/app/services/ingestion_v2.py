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
from app.services.professional_logger import get_professional_logger, ingestion_stats

# Initialize professional logger
logger = get_professional_logger(__name__)


# ============================================================================
# Space B Configuration
# ============================================================================
# Constants
SPACE_B_URL = "https://workwithshafisk-segmentopulse-factory.hf.space"
SPACE_B_TIMEOUT = 30  # seconds (Llama-3 is slow on CPU)
REQUEST_TIMEOUT = 10  # seconds for RSS feed fetching

# Phase 3: Cloud News Categories
CLOUD_CATEGORIES = [
    "cloud-aws",
    "cloud-azure",
    "cloud-gcp",
    "cloud-oracle",
    "cloud-ibm",
    "cloud-alibaba",
    "cloud-digitalocean",
    "cloud-huawei",
    "cloud-cloudflare",
    "cloud-computing"  # General cloud news
]

# Phase 3: Official Cloud Provider Feeds
OFFICIAL_CLOUD_FEEDS = {
    "https://aws.amazon.com/blogs/aws/feed/": ("aws", True),
    "https://azure.microsoft.com/en-us/blog/feed/": ("azure", True),
    "https://cloudblog.withgoogle.com/rss/": ("gcp", True),
    "https://blogs.oracle.com/cloud-infrastructure/rss": ("oracle", True),
    "https://www.ibm.com/blog/category/ibm-cloud/feed/": ("ibm", True),
    "https://www.alibabacloud.com/blog/rss.xml": ("alibaba", True),
    "https://www.digitalocean.com/blog/rss.xml": ("digitalocean", True),
    "https://developer.huaweicloud.com/intl/en-us/feed": ("huawei", True),
    "https://blog.cloudflare.com/rss/": ("cloudflare", True)
}


def determine_cloud_provider(category: str, source_feed: str) -> tuple:
    """
    Phase 3: Determine cloud provider and whether article is from official blog.
    
    Args:
        category: News category (e.g., "cloud-aws")
        source_feed: RSS feed URL
        
    Returns:
        Tuple of (provider_name, is_official)
        
    Examples:
        ("aws", True) - From aws.amazon.com/blogs
        ("azure", False) - From Google News about Azure
    """
    # Check if from official feed
    if source_feed in OFFICIAL_CLOUD_FEEDS:
        return OFFICIAL_CLOUD_FEEDS[source_feed]
    
    # From news API - extract provider from category
    if category.startswith('cloud-'):
        provider = category.replace('cloud-', '')
        return (provider, False)
    
    return ("general", False)


def route_to_collection(category: str, config_obj) -> str:
    """
    Phase 3: Determine which Appwrite collection to use.
    
    Args:
        category: Article category
        config_obj: Settings object with collection IDs
        
    Returns:
        Collection ID string
    """
    if category in CLOUD_CATEGORIES and config_obj.APPWRITE_CLOUD_COLLECTION_ID:
        return config_obj.APPWRITE_CLOUD_COLLECTION_ID
    else:
        return config_obj.APPWRITE_COLLECTION_ID


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
                # Use requests to fetch content with a timeout to prevent feedparser from blocking indefinitely
                response = await asyncio.to_thread(requests.get, url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status() # Raise an exception for bad status codes
                
                # Parse RSS feed with feedparser using the fetched content
                feed = await asyncio.to_thread(feedparser.parse, response.content)
                
                # Convert each entry to Document
                for entry in feed.entries:
                    doc = create_document_from_rss_entry(
                        entry=entry,
                        category=category,
                        source_feed=url
                    )
                    all_documents.append(doc)
                
                logger.debug(f"   ✓ Fetched {len(feed.entries)} articles from {url[:50]}...")
                
            except requests.exceptions.RequestException as req_e:
                logger.warning(f"   ⚠️  Failed to fetch {url} due to network/timeout error: {req_e}")
                continue
            except Exception as e:
                logger.warning(f"   ⚠️  Failed to parse {url}: {e}")
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

async def process_and_store_article(
    url: str, 
    raw_text: str, 
    category: str, 
    title: str = "",
    source_feed: str = ""
) -> Optional[Dict]:
    """
    Phase 3: Enhanced processing with cloud detection and engagement metrics
    
    Architecture:
    1. Send raw_text to Space B's /process-article endpoint
    2. Receive summary + tags from Space B
    3. Detect cloud provider and routing
    4. Add engagement metrics (likes, views)
    5. Generate embeddings locally using sentence-transformers
    6. Store in ChromaDB with rich metadata
    
    Args:
        url: Article URL (used as ID)
        raw_text: Full article content
        category: Article category
        title: Article title (optional)
        source_feed: RSS feed URL (for cloud detection)
        
    Returns:
        Dictionary with processing results or None on error
    """
    try:
        from app.utils import strip_html_if_needed, list_to_comma_separated
        
        logger.space_b_call(url, "started")
        
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
                logger.space_b_call(url, "failure")
                logger.warning(f"Space B returned {response.status_code}: {response.text[:200]}")
                return None
                
            space_b_result = response.json()
            summary = space_b_result.get("summary", "")
            tags = space_b_result.get("tags", [])
            
            logger.space_b_call(url, "success")
            logger.metric("Summary Length", f"{len(summary)} chars", "📝")
            logger.metric("Tags Extracted", len(tags), "🏷️")
            
        except requests.exceptions.Timeout:
            logger.space_b_call(url, "timeout")
            return None
        except requests.exceptions.RequestException as e:
            logger.space_b_call(url, "failure")
            logger.error(f"Space B connection error: {e}")
            return None
        except Exception as e:
            logger.space_b_call(url, "failure")
            logger.error(f"Space B processing error: {e}")
            return None
        
        # -------------------------------------------------------------------------
        # Step 2: Phase 3 - Cloud Detection
        # -------------------------------------------------------------------------
        is_cloud = category in CLOUD_CATEGORIES
        provider, is_official = determine_cloud_provider(category, source_feed)
        
        if is_cloud:
            logger.info(f"☁️  Cloud article detected: {provider} (official={is_official})")
        
        # -------------------------------------------------------------------------
        # Step 3: Phase 3 - HTML Stripping & Text Cleaning
        # -------------------------------------------------------------------------
        title_clean = strip_html_if_needed(title) if title else summary[:100]
        summary_clean = strip_html_if_needed(summary)
        
        # -------------------------------------------------------------------------
        # Step 4: Prepare article data for ChromaDB with Phase 3 metadata
        # -------------------------------------------------------------------------
        url_hash = hashlib.md5(url.encode()).hexdigest()
        
        # Convert tags list to comma-separated string
        tags_str = list_to_comma_separated(tags)
        
        article_data = {
            "$id": url_hash,
            
            # Core content (cleaned)
            "title": title_clean,
            "description": summary_clean,
            "url": url,
            "source": "Segmento AI",
            "category": category,
            "published_at": datetime.now().isoformat(),
            "image": "",  # No image for now
            
            # Phase 3: Tags from GLiNER
            "tags": tags_str,
            
            # Phase 3: Cloud detection
            "is_cloud_news": is_cloud,
            "cloud_provider": provider if is_cloud else "",
            "is_official": is_official if is_cloud else False,
            
            # Phase 3: Engagement metrics
            "likes": 0,
            "dislikes": 0,
            "views": 0
        }
        
        # -------------------------------------------------------------------------
        # Step 5: Store in ChromaDB with Phase 3 enhanced schema
        # -------------------------------------------------------------------------
        # Create combined text for embedding: Title + Summary + Tags
        tags_text = " ".join(tags) if tags else ""
        combined_analysis = f"Summary: {summary_clean}\nTags: {tags_text}"
        
        # Upsert to vector store (handles embedding generation internally)
        vector_store.upsert_article(article_data, combined_analysis)
        ingestion_stats.chromadb_upserts += 1
        ingestion_stats.articles_saved += 1
        
        cloud_emoji = "☁️" if is_cloud else "📰"
        logger.success(f"{cloud_emoji} ChromaDB stored: {title_clean[:50]}")
        
        return {
            "url": url,
            "summary": summary_clean,
            "tags": tags,
            "is_cloud": is_cloud,
            "provider": provider if is_cloud else None,
            "stored": True
        }
        
    except Exception as e:
        logger.error(f"[Phase 3 CQRS] Processing failed for {url}: {e}")
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
