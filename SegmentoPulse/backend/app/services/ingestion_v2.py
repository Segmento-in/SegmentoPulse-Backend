"""
Ingestion Engine v2 - LlamaIndex + Bloom Filter

Next-generation news ingestion pipeline using:
- LlamaIndex RSSReader for robust RSS parsing
- LlamaIndex SimpleWebPageReader for web scraping
- Bloom Filter for URL deduplication
- Parallel processing for high throughput

This replaces manual feedparser/requests scripts with production-grade data loaders.
"""

import asyncio
from datetime import datetime
from typing import List, Dict, Optional
import logging
import feedparser
from dateutil import parser as date_parser

from app.models import Article
from app.services.deduplication import get_url_filter

logger = logging.getLogger(__name__)


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


async def fetch_category_rss(category: str, rss_urls: List[str]) -> List[Dict]:
    """
    Fetch RSS feeds for a category using feedparser
    
    Args:
        category: News category
        rss_urls: List of RSS feed URLs
        
    Returns:
        List of article dictionaries
    """
    try:
        logger.info(f"📡 [RSS] Fetching RSS for {category.upper()}...")
        
        all_articles = []
        
        # Fetch each RSS feed
        for url in rss_urls:
            try:
                # Parse RSS feed
                feed = await asyncio.to_thread(feedparser.parse, url)
                
                # Extract articles from feed
                for entry in feed.entries:
                    article_data = {
                        'title': entry.get('title', '')[:200],
                        'url': entry.get('link', ''),
                        'description': entry.get('summary', '')[:500] or entry.get('description', '')[:500],
                        'published': entry.get('published', datetime.now().isoformat()),
                        'source': feed.feed.get('title', 'Unknown'),
                        'category': category,
                        'source_feed': url
                    }
                    
                    all_articles.append(article_data)
                
                logger.debug(f"   ✓ Fetched {len(feed.entries)} articles from {url[:50]}...")
                
            except Exception as e:
                logger.warning(f"   ⚠️  Failed to fetch {url}: {e}")
                continue
        
        logger.info(f"   ✅ Total fetched: {len(all_articles)} articles for {category}")
        return all_articles
        
    except Exception as e:
        logger.error(f"❌ Error fetching category {category}: {e}")
        return []


def convert_to_article(article_data: Dict, category: str) -> Optional[Article]:
    """
    Convert article dictionary to Article model
    
    Args:
        article_data: Article data dictionary from feedparser
        category: News category
        
    Returns:
        Article object or None if conversion fails
    """
    try:
        # Extract fields
        title = article_data.get('title', '')[:200]
        url = article_data.get('url', '')
        description = article_data.get('description', '')[:500]
        published_at = article_data.get('published', datetime.now().isoformat())
        source = article_data.get('source', 'Unknown')
        
        # Basic validation
        if not title or not url:
            return None
        
        # Create Article
        article = Article(
            title=title,
            description=description,
            url=url,
            image='',  # No image from RSS
            publishedAt=published_at,
            source=source,
            category=category
        )
        
        return article
        
    except Exception as e:
        logger.error(f"❌ Error converting article: {e}")
        return None


async def fetch_latest_news(categories: List[str]) -> Dict[str, List[Article]]:
    """
    Main ingestion function using feedparser + Bloom Filter
    
    Fetches news for multiple categories in parallel, deduplicates URLs,
    and returns structured Article objects.
    
    Args:
        categories: List of category names to fetch
        
    Returns:
        Dictionary mapping category -> List[Article]
    """
    start_time = datetime.now()
    
    logger.info("═" * 80)
    logger.info("🚀 [INGESTION V2] Starting feedparser + Bloom Filter ingestion...")
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
            articles_data = await task
            total_fetched += len(articles_data)
            
            # Deduplicate and convert to Article objects
            articles = []
            duplicates = 0
            
            for article_data in articles_data:
                # Extract URL for deduplication
                url = article_data.get('url', '')
                
                if not url:
                    continue
                
                # Check if URL is new
                if url_filter.check_and_add(url):
                    # New URL - convert to Article
                    article = convert_to_article(article_data, category)
                    if article:
                        articles.append(article)
                        total_converted += 1
                else:
                    # Duplicate URL - skip
                    duplicates += 1
                    total_deduped += 1
            
            results[category] = articles
            
            logger.info(f"✅ {category.upper()}: {len(articles_data)} fetched, {len(articles)} new, {duplicates} duplicates")
            
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
    logger.info(f"   🔹 Total Fetched: {total_fetched} articles")
    logger.info(f"   🔹 Total Converted: {total_converted} articles")
    logger.info(f"   🔹 Total Duplicates Skipped: {total_deduped} articles")
    logger.info(f"   🔹 Deduplication Rate: {(total_deduped / total_fetched * 100) if total_fetched > 0 else 0:.1f}%")
    logger.info("")
    logger.info("⏱️  PERFORMANCE:")
    logger.info(f"   🔹 Duration: {duration:.2f} seconds")
    logger.info(f"   🔹 Throughput: {total_fetched / duration if duration > 0 else 0:.1f} articles/second")
    logger.info("═" * 80)
    
    # Print URL filter stats
    url_filter.print_stats()
    
    return results


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
