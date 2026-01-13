import httpx
from typing import List, Optional, Dict
from datetime import datetime
from abc import ABC, abstractmethod
from app.models import Article
import os
from enum import Enum

class ProviderStatus(Enum):
    """Provider status enum"""
    ACTIVE = "active"
    RATE_LIMITED = "rate_limited"
    ERROR = "error"

class NewsProvider(ABC):
    """Abstract base class for news providers"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.status = ProviderStatus.ACTIVE
        self.request_count = 0
        self.daily_limit = 0
        self.name = self.__class__.__name__
    
    @abstractmethod
    async def fetch_news(self, category: str, limit: int = 20) -> List[Article]:
        """Fetch news articles for a given category"""
        pass
    
    def is_available(self) -> bool:
        """Check if provider is available"""
        return self.status == ProviderStatus.ACTIVE and (
            self.daily_limit == 0 or self.request_count < self.daily_limit
        )
    
    def mark_rate_limited(self):
        """Mark provider as rate limited"""
        self.status = ProviderStatus.RATE_LIMITED
    
    def reset_daily_quota(self):
        """Reset daily quota"""
        self.request_count = 0
        self.status = ProviderStatus.ACTIVE


class GNewsProvider(NewsProvider):
    """GNews.io API provider"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__(api_key)
        self.base_url = "https://gnews.io/api/v4"
        self.daily_limit = 100
        
        # Category mapping
        self.category_map = {
            'ai': 'artificial intelligence machine learning',
            'data-security': 'data security cybersecurity',
            'data-governance': 'data governance compliance',
            'data-privacy': 'data privacy GDPR',
            'data-engineering': 'data engineering pipeline',
            'business-intelligence': 'business intelligence BI',
            'business-analytics': 'business analytics',
            'customer-data-platform': 'customer data platform CDP',
            'data-centers': 'data centers infrastructure',
            'cloud-computing': 'cloud computing',
            'magazines': 'technology news',
        }
    
    async def fetch_news(self, category: str, limit: int = 20) -> List[Article]:
        """Fetch news from GNews API"""
        if not self.api_key:
            return []
        
        try:
            query = self.category_map.get(category, category)
            url = f"{self.base_url}/search"
            params = {
                'q': query,
                'lang': 'en',
                'country': 'us',
                'max': min(limit, 10),  # GNews free tier max 10
                'apikey': self.api_key
            }
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(url, params=params)
                
                if response.status_code == 429:
                    self.mark_rate_limited()
                    return []
                
                if response.status_code == 200:
                    self.request_count += 1
                    data = response.json()
                    return self._parse_response(data, category)
                
                return []
        except Exception as e:
            print(f"GNews API error: {e}")
            return []
    
    def _parse_response(self, data: Dict, category: str) -> List[Article]:
        """Parse GNews API response"""
        articles = []
        for item in data.get('articles', []):
            try:
                article = Article(
                    title=item.get('title', ''),
                    description=item.get('description', ''),
                    url=item.get('url', ''),
                    image=item.get('image', ''),
                    publishedAt=item.get('publishedAt', datetime.now().isoformat()),
                    source=item.get('source', {}).get('name', 'GNews'),
                    category=category
                )
                articles.append(article)
            except Exception as e:
                print(f"Error parsing GNews article: {e}")
                continue
        return articles


class NewsAPIProvider(NewsProvider):
    """NewsAPI.org provider"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__(api_key)
        self.base_url = "https://newsapi.org/v2"
        self.daily_limit = 100
        
        # Category keywords
        self.category_keywords = {
            'ai': 'artificial intelligence OR "machine learning" OR "deep learning"',
            'data-security': '"data security" OR cybersecurity OR "data breach"',
            'data-governance': '"data governance" OR "data management" OR compliance',
            'data-privacy': '"data privacy" OR GDPR OR "privacy regulation"',
            'data-engineering': '"data engineering" OR "data pipeline" OR "big data"',
            'business-intelligence': '"business intelligence" OR "BI tools"',
            'business-analytics': '"business analytics" OR analytics',
            'customer-data-platform': '"customer data platform" OR CDP',
            'data-centers': '"data centers" OR "data centre"',
            'cloud-computing': '"cloud computing" OR cloud',
            'magazines': 'technology',
        }
    
    async def fetch_news(self, category: str, limit: int = 20) -> List[Article]:
        """Fetch news from NewsAPI"""
        if not self.api_key:
            return []
        
        try:
            query = self.category_keywords.get(category, category)
            url = f"{self.base_url}/everything"
            params = {
                'q': query,
                'language': 'en',
                'sortBy': 'publishedAt',
                'pageSize': min(limit, 20),
                'apiKey': self.api_key
            }
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(url, params=params)
                
                if response.status_code == 429 or response.status_code == 426:
                    self.mark_rate_limited()
                    return []
                
                if response.status_code == 200:
                    self.request_count += 1
                    data = response.json()
                    return self._parse_response(data, category)
                
                return []
        except Exception as e:
            print(f"NewsAPI error: {e}")
            return []
    
    def _parse_response(self, data: Dict, category: str) -> List[Article]:
        """Parse NewsAPI response"""
        articles = []
        for item in data.get('articles', []):
            try:
                article = Article(
                    title=item.get('title', ''),
                    description=item.get('description', ''),
                    url=item.get('url', ''),
                    image=item.get('urlToImage', ''),
                    publishedAt=item.get('publishedAt', datetime.now().isoformat()),
                    source=item.get('source', {}).get('name', 'NewsAPI'),
                    category=category
                )
                articles.append(article)
            except Exception as e:
                print(f"Error parsing NewsAPI article: {e}")
                continue
        return articles


class NewsDataProvider(NewsProvider):
    """NewsData.io provider"""
    
    def __init__(self, api_key: Optional[str] = None):
        super().__init__(api_key)
        self.base_url = "https://newsdata.io/api/1"
        self.daily_limit = 200
        
        # Category keywords
        self.category_keywords = {
            'ai': 'artificial intelligence,machine learning',
            'data-security': 'data security,cybersecurity',
            'data-governance': 'data governance,compliance',
            'data-privacy': 'data privacy,GDPR',
            'data-engineering': 'data engineering,big data',
            'business-intelligence': 'business intelligence',
            'business-analytics': 'business analytics',
            'customer-data-platform': 'customer data platform',
            'data-centers': 'data centers',
            'cloud-computing': 'cloud computing',
            'magazines': 'technology',
        }
    
    async def fetch_news(self, category: str, limit: int = 20) -> List[Article]:
        """Fetch news from NewsData.io"""
        if not self.api_key:
            return []
        
        try:
            query = self.category_keywords.get(category, category)
            url = f"{self.base_url}/news"
            params = {
                'q': query,
                'language': 'en',
                'country': 'us',
                'apikey': self.api_key
            }
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(url, params=params)
                
                if response.status_code == 429:
                    self.mark_rate_limited()
                    return []
                
                if response.status_code == 200:
                    self.request_count += 1
                    data = response.json()
                    return self._parse_response(data, category, limit)
                
                return []
        except Exception as e:
            print(f"NewsData.io error: {e}")
            return []
    
    def _parse_response(self, data: Dict, category: str, limit: int) -> List[Article]:
        """Parse NewsData.io response"""
        articles = []
        for item in data.get('results', [])[:limit]:
            try:
                article = Article(
                    title=item.get('title', ''),
                    description=item.get('description', ''),
                    url=item.get('link', ''),
                    image=item.get('image_url', ''),
                    publishedAt=item.get('pubDate', datetime.now().isoformat()),
                    source=item.get('source_id', 'NewsData'),
                    category=category
                )
                articles.append(article)
            except Exception as e:
                print(f"Error parsing NewsData article: {e}")
                continue
        return articles


class GoogleNewsRSSProvider(NewsProvider):
    """Google News RSS provider (no API key needed)"""
    
    def __init__(self):
        super().__init__(None)
        self.daily_limit = 0  # Unlimited (but rate limited by Google)
        
        # RSS feed URLs by category
        self.feed_urls = {
            'ai': 'https://news.google.com/rss/search?q=artificial+intelligence+OR+machine+learning&hl=en-US&gl=US&ceid=US:en',
            'data-security': 'https://news.google.com/rss/search?q=data+security+OR+cybersecurity+OR+data+breach&hl=en-US&gl=US&ceid=US:en',
            'data-governance': 'https://news.google.com/rss/search?q=data+governance+OR+data+management&hl=en-US&gl=US&ceid=US:en',
            'data-privacy': 'https://news.google.com/rss/search?q=data+privacy+OR+GDPR+OR+privacy+regulation&hl=en-US&gl=US&ceid=US:en',
            'data-engineering': 'https://news.google.com/rss/search?q=data+engineering+OR+data+pipeline+OR+big+data&hl=en-US&gl=US&ceid=US:en',
            'business-intelligence': 'https://news.google.com/rss/search?q=business+intelligence+OR+BI+tools&hl=en-US&gl=US&ceid=US:en',
            'business-analytics': 'https://news.google.com/rss/search?q=business+analytics&hl=en-US&gl=US&ceid=US:en',
            'customer-data-platform': 'https://news.google.com/rss/search?q=customer+data+platform+OR+CDP&hl=en-US&gl=US&ceid=US:en',
            'data-centers': 'https://news.google.com/rss/search?q=data+centers+OR+data+centre&hl=en-US&gl=US&ceid=US:en',
            'cloud-computing': 'https://news.google.com/rss/search?q=cloud+computing&hl=en-US&gl=US&ceid=US:en',
            'magazines': 'https://news.google.com/rss/headlines/section/topic/TECHNOLOGY?hl=en-US&gl=US&ceid=US:en',
        }
    
    async def fetch_news(self, category: str, limit: int = 20) -> List[Article]:
        """Fetch news from Google News RSS"""
        from app.services.rss_parser import RSSParser
        
        feed_url = self.feed_urls.get(category)
        if not feed_url:
            return []
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(feed_url)
                
                if response.status_code == 429:
                    self.mark_rate_limited()
                    return []
                
                if response.status_code == 200:
                    self.request_count += 1
                    parser = RSSParser()
                    return await parser.parse_google_news(response.text, category)
                
                return []
        except Exception as e:
            print(f"Google News RSS error: {e}")
            return []
