import httpx
from typing import List, Dict, Optional
from datetime import datetime
from app.models import Article
from app.services.rss_parser import RSSParser
from app.services.news_providers import (
    NewsProvider, 
    GNewsProvider, 
    NewsAPIProvider, 
    NewsDataProvider, 
    NewsDataProvider, 
    GoogleNewsRSSProvider,
    MediumRSSProvider
)
from app.config import settings

class NewsAggregator:
    """Service for aggregating news from multiple sources with automatic failover"""
    
    def __init__(self):
        self.rss_parser = RSSParser()
        
        # Initialize all available providers
        self.providers: Dict[str, NewsProvider] = {}
        
        # Initialize GNews if API key is available
        if settings.GNEWS_API_KEY:
            self.providers['gnews'] = GNewsProvider(settings.GNEWS_API_KEY)
        
        # Initialize NewsAPI if API key is available
        if settings.NEWSAPI_API_KEY:
            self.providers['newsapi'] = NewsAPIProvider(settings.NEWSAPI_API_KEY)
        
        # Initialize NewsData if API key is available
        if settings.NEWSDATA_API_KEY:
            self.providers['newsdata'] = NewsDataProvider(settings.NEWSDATA_API_KEY)
        
        # Always include Google News RSS as fallback (no API key needed)
        # Always include Google News RSS as fallback (no API key needed)
        self.providers['google_rss'] = GoogleNewsRSSProvider()

        # Always include Medium RSS (no API key, specialized content)
        self.providers['medium'] = MediumRSSProvider()
        
        # Provider priority order
        self.provider_priority = settings.NEWS_PROVIDER_PRIORITY
        
        # Cloud provider RSS feeds
        self.cloud_rss_urls = {
            "aws": "https://aws.amazon.com/blogs/aws/feed/",
            "gcp": "https://cloudblog.withgoogle.com/rss/",
            "azure": "https://azure.microsoft.com/en-us/blog/feed/",
            "ibm": "https://www.ibm.com/blog/rss",
            "oracle": "https://blogs.oracle.com/cloud-infrastructure/rss",
            "digitalocean": "https://www.digitalocean.com/blog/rss.xml"
        }
        
        # Statistics tracking
        self.stats = {
            'total_requests': 0,
            'provider_usage': {},
            'failover_count': 0
        }
    
    async def fetch_by_category(self, category: str) -> List[Article]:
        """
        Fetch news by category using hybrid approach with automatic failover
        Tries providers in priority order until successful
        """
        self.stats['total_requests'] += 1
        
        # Try each provider in priority order
        for provider_name in self.provider_priority:
            provider = self.providers.get(provider_name)
            
            # Skip if provider not configured
            if not provider:
                continue
            
            # Skip if provider is not available (rate limited)
            if not provider.is_available():
                print(f"⏭️  [{provider_name.upper()}] Not available (rate limited), trying next...")
                self.stats['failover_count'] += 1
                continue
            
            try:
                print(f"📡 [{provider_name.upper()}] Attempting to fetch '{category}' news...")
                articles = await provider.fetch_news(category, limit=20)
                
                # If we got articles, return them
                if articles:
                    # No need to print here, provider already printed success
                    
                    # Track usage statistics
                    if provider_name not in self.stats['provider_usage']:
                        self.stats['provider_usage'][provider_name] = 0
                    self.stats['provider_usage'][provider_name] += 1
                    
                    return articles
                else:
                    print(f"⏭️  [{provider_name.upper()}] No articles returned, trying next provider...")
                    
            except Exception as e:
                print(f"❌ [{provider_name.upper()}] Error: {e}, trying next...")
                self.stats['failover_count'] += 1
                continue
        
        # If all providers failed, return empty list
        print(f"😞 [NEWS AGGREGATOR] All providers exhausted for '{category}' - no articles available")
        return []

    async def fetch_from_provider(self, provider_name: str, category: str) -> List[Article]:
        """Fetch news specifically from a named provider (bypassing priority/failover)"""
        provider = self.providers.get(provider_name)
        if not provider or not provider.is_available():
            return []
        
        try:
            # print(f"📡 [{provider_name.upper()}] Fetching specific '{category}' news...")
            return await provider.fetch_news(category)
        except Exception as e:
            print(f"❌ [{provider_name.upper()}] Specific fetch error: {e}")
            return []
    
    async def fetch_rss(self, provider: str) -> List[Article]:
        """Fetch RSS from cloud providers"""
        url = self.cloud_rss_urls.get(provider)
        if not url:
            return []
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(url)
                if response.status_code == 200:
                    content = response.text
                    return await self.rss_parser.parse_provider_rss(content, provider)
                return []
        except Exception as e:
            print(f"Error fetching RSS for {provider}: {e}")
            return []
    
    async def search(self, query: str) -> List[Article]:
        """
        Search news articles using hybrid approach
        Currently uses Google News RSS for search functionality
        """
        # Use Google News RSS for search
        google_rss = self.providers.get('google_rss')
        if google_rss:
            try:
                # Create a custom search URL
                search_url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
                
                async with httpx.AsyncClient(timeout=10.0) as client:
                    response = await client.get(search_url)
                    if response.status_code == 200:
                        return await self.rss_parser.parse_google_news(response.text, "search")
            except Exception as e:
                print(f"Error searching news: {e}")
        
        return []
    
    def get_stats(self) -> Dict:
        """Get usage statistics for monitoring"""
        return {
            **self.stats,
            'available_providers': [
                name for name, provider in self.providers.items() 
                if provider.is_available()
            ],
            'provider_status': {
                name: {
                    'status': provider.status.value,
                    'request_count': provider.request_count,
                    'daily_limit': provider.daily_limit
                }
                for name, provider in self.providers.items()
            }
        }
