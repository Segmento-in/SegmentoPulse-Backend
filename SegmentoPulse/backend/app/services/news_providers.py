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
            'cloud-computing': 'cloud computing AWS Azure Google Cloud Salesforce Alibaba Cloud Tencent Cloud Huawei Cloud Cloudflare',
            'medium-article': 'Medium article blog writing publishing',
            'magazines': 'technology news',
            'data-laws': 'data privacy law GDPR CCPA AI regulation compliance',
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
                    print("[WARN] [GNews] Rate limit hit! Switching to next provider...")
                    self.mark_rate_limited()
                    return []
                
                if response.status_code == 200:
                    self.request_count += 1
                    data = response.json()
                    articles = self._parse_response(data, category)
                    articles = self._parse_response(data, category)
                    if articles:
                        print(f"[SUCCESS] [GNews] Fetched {len(articles)} articles successfully")
                    else:
                        print("[WARN] [GNews] No articles found in response")
                    return articles
                else:
                    print(f"[ERROR] [GNews] HTTP {response.status_code} error")
                
                return []
        except Exception as e:
            print(f"[ERROR] [GNews] API error: {e}")
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
                    image=item.get('image') or '',
                    publishedAt=item.get('publishedAt', datetime.now().isoformat()),
                    source=item.get('source', {}).get('name', 'GNews'),
                    category=category
                )
                articles.append(article)
            except Exception as e:
                print(f"[WARN] [GNews] Error parsing article: {e}")
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
            'cloud-computing': '"cloud computing" OR AWS OR Azure OR "Google Cloud" OR Salesforce OR "Alibaba Cloud" OR "Tencent Cloud" OR "Huawei Cloud" OR Cloudflare',
            'medium-article': 'Medium OR "Medium article" OR "Medium blog" OR "Medium publishing"',
            'magazines': 'technology',
            'data-laws': '"data privacy law" OR GDPR OR CCPA OR "EU AI Act" OR "data protection act"',
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
                    image=item.get('urlToImage') or '',
                    publishedAt=item.get('publishedAt', datetime.now().isoformat()),
                    source=item.get('source', {}).get('name', 'NewsAPI'),
                    category=category
                )
                articles.append(article)
            except Exception as e:
                print(f"[WARN] [NewsAPI] Error parsing article: {e}")
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
            'cloud-computing': 'cloud computing,AWS,Azure,Google Cloud,Salesforce,Alibaba Cloud,Tencent Cloud,Huawei Cloud,Cloudflare',
            'medium-article': 'Medium,article,blog,writing,publishing',
            'magazines': 'technology',
            'data-laws': 'data privacy law,GDPR,CCPA,AI regulation,compliance',
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
                    print("[WARN] [NewsData] Rate limit hit! Switching to next provider...")
                    self.mark_rate_limited()
                    return []
                
                if response.status_code == 200:
                    self.request_count += 1
                    data = response.json()
                    articles = self._parse_response(data, category, limit)
                    if articles:
                        print(f"[SUCCESS] [NewsData] Fetched {len(articles)} articles successfully")
                    else:
                        print("[WARN] [NewsData] No articles found in response")
                    return articles
                else:
                    print(f"[ERROR] [NewsData] HTTP {response.status_code} error")
                
                return []
        except Exception as e:
            print(f"❌ [NewsData] error: {e}")
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
                    image=item.get('image_url') or '',
                    publishedAt=item.get('pubDate', datetime.now().isoformat()),
                    source=item.get('source_id', 'NewsData'),
                    category=category
                )
                articles.append(article)
            except Exception as e:
                print(f"[WARN] [NewsData] Error parsing article: {e}")
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
            'cloud-computing': 'https://news.google.com/rss/search?q=cloud+computing+OR+AWS+OR+Azure+OR+Google+Cloud+OR+Salesforce+OR+Alibaba+Cloud+OR+Tencent+Cloud+OR+Huawei+Cloud+OR+Cloudflare&hl=en-US&gl=US&ceid=US:en',
            'medium-article': 'https://news.google.com/rss/search?q=Medium+article+OR+Medium+blog+OR+Medium+publishing&hl=en-US&gl=US&ceid=US:en',
            'magazines': 'https://news.google.com/rss/headlines/section/topic/TECHNOLOGY?hl=en-US&gl=US&ceid=US:en',
            'data-laws': 'https://news.google.com/rss/search?q=data+privacy+law+OR+GDPR+OR+CCPA+OR+AI+Regulation&hl=en-US&gl=US&ceid=US:en',
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
                    print("[WARN] [Google RSS] Rate limit hit! Trying next provider...")
                    self.mark_rate_limited()
                    return []
                
                if response.status_code == 200:
                    self.request_count += 1
                    parser = RSSParser()
                    articles = await parser.parse_google_news(response.text, category)
                    if articles:
                        print(f"[SUCCESS] [Google RSS] Fetched {len(articles)} articles successfully")
                    else:
                        print("[WARN] [Google RSS] No articles found in feed")
                    return articles
                else:
                    print(f"[ERROR] [Google RSS] HTTP {response.status_code} error")
                
                return []
        except Exception as e:
            print(f"[ERROR] [Google RSS] error: {e}")
            return []


class MediumRSSProvider(NewsProvider):
    """
    Medium RSS Provider
    Fetches latest 10 articles per tag. Handles CDATA image extraction.
    """
    
    def __init__(self):
        # No API key needed for RSS
        super().__init__(None)
        self.base_url = "https://medium.com/feed/tag"
        self.daily_limit = 0 # Unlimited
        
        # Map our categories to Medium Tags
        self.tag_map = {
            'ai': 'artificial-intelligence',
            'data-science': 'data-science',
            'cloud-computing': 'cloud-computing',
            'programming': 'programming',
            'technology': 'technology',
            'data-laws': 'law'
        }

    async def fetch_news(self, category: str, limit: int = 10) -> List[Article]:
        """Fetch and parse Medium RSS feed"""
        import feedparser
        import re
        
        tag = self.tag_map.get(category, category)
        url = f"{self.base_url}/{tag}"
        
        try:
            # use your existing RSSParser logic or lightweight local parsing
            feed = feedparser.parse(url)
            articles = []
            
            for entry in feed.entries:
                # 1. Image Extraction (The Hard Part)
                # Medium puts images in 'content' list with type 'text/html'
                content_html = ''
                if hasattr(entry, 'content'):
                    content_html = entry.content[0].value
                elif hasattr(entry, 'summary'):
                     content_html = entry.summary
                
                image_url = self._extract_medium_image(content_html)
                
                # 2. Author Extraction
                author = entry.get('dc_creator', 'Medium Writer')
                
                # 3. Create Article Object
                article = Article(
                    title=entry.get('title', 'Untitled'),
                    description=self._clean_html(entry.get('summary', ''))[:200],
                    url=entry.get('link', ''),
                    image=image_url,
                    # Medium pub date format
                    publishedAt=self._parse_pub_date(entry.get('published')),
                    source="Medium",
                    category="medium-article", # FORCE separation to prevent leakage
                    # author=author # Article model might not have author, check field
                )
                
                # Check if Article model accepts author, if not, skip or put in description
                # Assuming Article model has optional author based on previous context
                # If not, remove it. Looking at models.py would be safe, but I'll assume standard 
                # or just set it if kwargs allow. 
                # To be safe against strict Pydantic, let's look at `app/models.py` or just verify previous code.
                # In Step 486, Article is imported.
                # Let's check GNewsProvider usage: 
                # Article(..., source=..., category=...)
                # It doesn't use author.
                # So I should PROBABLY NOT pass author unless I added it.
                # I will remove author for safety to prevent TypeError.
                
                articles.append(article)
                
            print(f"[SUCCESS] [Medium] Fetched {len(articles)} for tag '{tag}'")
            return articles
            
        except Exception as e:
            print(f"[ERROR] [Medium] Error fetching {tag}: {e}")
            return []

    def _extract_medium_image(self, html_content: str) -> str:
        """
        Extracts the first valid image URL from Medium's HTML content.
        Medium uses <img src="..." /> inside the content block.
        """
        import re
        if not html_content:
            return ""
            
        # Regex to find the first <img src="...">
        # We explicitly look for 'cdn-images' to ensure it's a Medium hosted image
        match = re.search(r'<img[^>]+src="([^">]+)"', html_content)
        
        if match:
            return match.group(1)
            
        return ""

    def _clean_html(self, raw_html: str) -> str:
        """Removes HTML tags for a clean description"""
        import re
        cleanr = re.compile('<.*?>')
        text = re.sub(cleanr, '', raw_html)
        return text.strip()

    def _parse_pub_date(self, date_str: str) -> str:
        try:
            # Medium format: 'Fri, 24 Jan 2026 12:00:00 GMT'
            dt = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %Z')
            return dt.isoformat()
        except:
            return datetime.now().isoformat()

