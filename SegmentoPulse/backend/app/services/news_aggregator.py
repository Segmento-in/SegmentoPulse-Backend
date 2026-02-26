import asyncio
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
    MediumRSSProvider,
    OfficialCloudProvider
)
from app.config import settings
from app.services.api_quota import get_quota_tracker
from app.services.circuit_breaker import get_circuit_breaker

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
        
        # Official Cloud Provider (Strict Isolation)
        self.providers['official_cloud'] = OfficialCloudProvider()
        
        # ── Provider role lists ──────────────────────────────────────────────
        # PAID_CHAIN: tried in order, stop after the first success (save credits)
        # FREE_SOURCES: always tried, always in parallel (no cost, no limits)
        self.PAID_CHAIN  = ['gnews', 'newsapi', 'newsdata']
        self.FREE_SOURCES = ['google_rss', 'medium', 'official_cloud']

        # Medium only publishes articles for a small set of topics.
        # Calling it for 'data-centers' or 'cloud-oracle' would return nothing.
        self.MEDIUM_SUPPORTED_CATEGORIES = {
            'ai', 'data-science', 'cloud-computing', 'programming',
            'technology', 'data-laws'
        }

        # Official Cloud RSS only makes sense for cloud-related categories.
        self.CLOUD_CATEGORIES = {
            c for c in [
                'cloud-computing', 'cloud-aws', 'cloud-azure', 'cloud-gcp',
                'cloud-oracle', 'cloud-ibm', 'cloud-alibaba', 'cloud-digitalocean',
                'cloud-huawei', 'cloud-cloudflare'
            ]
        }
        
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

        # Async lock — keeps stats correct when 22 category tasks share this one aggregator.
        # Without this, two tasks updating the same counter at the same time could miss a count.
        self._lock = asyncio.Lock()

        # --- Phase 2 additions: infrastructure guards ---

        # Which providers cost real API credits.
        # Kept as a set for O(1) lookup inside the waterfall loop.
        self.paid_providers = set(self.PAID_CHAIN)

        # The Quota Tracker counts how many API calls we have made today.
        # It is a module-level singleton — once created it lives in memory for the
        # entire lifetime of the server process, surviving every hourly scheduler
        # run without resetting. (It DOES reset if the server itself restarts;
        # that is acceptable for now and noted as a future improvement.)
        self.quota = get_quota_tracker()

        # The Circuit Breaker watches each provider for repeated failures.
        # If a provider fails 3 times in 5 minutes, we stop calling it for 1 hour
        # (like hanging up on a broken phone line and trying it again later).
        # It is also a module-level singleton — same lifetime as the quota tracker.
        self.circuit = get_circuit_breaker()
    
    async def fetch_by_category(self, category: str) -> List[Article]:
        """
        Fetch news from ALL available sources for a category.

        Strategy (Phase 5 — True Multi-Source Aggregation):

          STEP A ─ Paid Waterfall:
            Try GNews → NewsAPI → NewsData in order.
            Stop as soon as one returns articles.
            We only want ONE paid call per category to stay inside our daily budget.
            Think of it like: only knock on the first open door, don't ring every bell.

          STEP B ─ Free Parallel Run (always runs, even if Step A succeeded):
            Simultaneously fetch from Google RSS, Medium, and Official Cloud.
            These are free and have no rate-limit cost, so we always want them.
            Think of it like: sending postcards to all your free newspaper subscriptions.

          STEP C ─ Combine:
            Merge paid + free results into one big list.
            Duplicates are fine here — the in-batch deduplication in scheduler.py
            will clean them up right after this function returns.
        """
        async with self._lock:
            self.stats['total_requests'] += 1

        combined_articles: List[Article] = []

        # ======================================================================
        # STEP A: PAID WATERFALL — one successful call is all we need
        # ======================================================================
        paid_success = False
        for provider_name in self.PAID_CHAIN:
            provider = self.providers.get(provider_name)

            # Skip if this paid provider was not configured (no API key set).
            if not provider:
                continue

            # Guard 1 ─ Circuit Breaker
            if self.circuit.should_skip(provider_name):
                print(f"[CIRCUIT] [{provider_name.upper()}] Circuit OPEN — skipping this run.")
                async with self._lock:
                    self.stats['failover_count'] += 1
                continue

            # Guard 2 ─ Quota Check (paid only)
            if not await self.quota.async_can_make_call(provider_name):
                print(f"[QUOTA]   [{provider_name.upper()}] Daily limit reached — skipping.")
                continue

            # Guard 3 ─ Provider's own 429 flag
            if not provider.is_available():
                print(f"[SKIP]    [{provider_name.upper()}] Provider reported 429 — recording and skipping.")
                self.circuit.record_failure(provider_name, error_type="rate_limit", status_code=429)
                async with self._lock:
                    self.stats['failover_count'] += 1
                continue

            try:
                print(f"[PAID]    [{provider_name.upper()}] Fetching '{category}'...")
                articles = await provider.fetch_news(category, limit=20)

                if articles:
                    self.circuit.record_success(provider_name)
                    await self.quota.async_record_call(provider_name)
                    async with self._lock:
                        self.stats['provider_usage'][provider_name] = \
                            self.stats['provider_usage'].get(provider_name, 0) + 1
                    combined_articles.extend(articles)
                    paid_success = True
                    print(f"[PAID]    [{provider_name.upper()}] Got {len(articles)} articles — stopping paid chain.")
                    break  # ← KEY: one success is enough, protect our credits
                else:
                    print(f"[PAID]    [{provider_name.upper()}] No articles — trying next paid provider.")

            except Exception as e:
                print(f"[ERROR]   [{provider_name.upper()}] Fetch failed: {e} — recording failure.")
                self.circuit.record_failure(provider_name, error_type="exception")
                async with self._lock:
                    self.stats['failover_count'] += 1
                continue  # try next paid provider

        if not paid_success:
            print(f"[PAID]    No paid provider delivered articles for '{category}'.")

        # ======================================================================
        # STEP B: FREE PARALLEL RUN — always fires, no cost
        # ======================================================================
        # We build a list of coroutines for free sources, but only include a
        # provider if it actually supports this category (avoid pointless calls).
        free_tasks: list = []
        free_names: list = []  # track which name maps to which task result

        # Google RSS supports ALL categories.
        google_rss = self.providers.get('google_rss')
        if google_rss and not self.circuit.should_skip('google_rss'):
            if google_rss.is_available():
                free_tasks.append(google_rss.fetch_news(category, limit=20))
                free_names.append('google_rss')

        # Medium only supports a small set of topics.
        if category in self.MEDIUM_SUPPORTED_CATEGORIES:
            medium = self.providers.get('medium')
            if medium and not self.circuit.should_skip('medium'):
                if medium.is_available():
                    free_tasks.append(medium.fetch_news(category, limit=10))
                    free_names.append('medium')

        # Official Cloud RSS only makes sense for cloud-* categories.
        if category in self.CLOUD_CATEGORIES:
            official = self.providers.get('official_cloud')
            if official and not self.circuit.should_skip('official_cloud'):
                if official.is_available():
                    free_tasks.append(official.fetch_news(category, limit=10))
                    free_names.append('official_cloud')

        if free_tasks:
            print(f"[FREE]    Launching {len(free_tasks)} free source(s) in parallel for '{category}'...")
            free_results = await asyncio.gather(*free_tasks, return_exceptions=True)

            for name, result in zip(free_names, free_results):
                if isinstance(result, Exception):
                    print(f"[ERROR]   [{name.upper()}] Free fetch error: {result}")
                    self.circuit.record_failure(name, error_type="exception")
                elif isinstance(result, list) and result:
                    self.circuit.record_success(name)
                    combined_articles.extend(result)
                    print(f"[FREE]    [{name.upper()}] Got {len(result)} articles.")
                    async with self._lock:
                        self.stats['provider_usage'][name] = \
                            self.stats['provider_usage'].get(name, 0) + 1

        # ======================================================================
        # STEP C: RETURN COMBINED LIST
        # ======================================================================
        # Return everything we collected. Duplicates are expected and welcome —
        # the in-batch dedup in scheduler.py (Phase 1) will strip them cleanly.
        if combined_articles:
            print(f"[DONE]    '{category}': {len(combined_articles)} total articles from all sources.")
        else:
            print(f"[WARN]    '{category}': No articles from any source this run.")

        return combined_articles

    async def fetch_from_provider(self, provider_name: str, category: str) -> List[Article]:
        """Fetch news specifically from a named provider (bypassing priority/failover)"""
        provider = self.providers.get(provider_name)
        if not provider or not provider.is_available():
            return []
        
        try:
            # print(f"📡 [{provider_name.upper()}] Fetching specific '{category}' news...")
            return await provider.fetch_news(category)
        except Exception as e:
            print(f"[ERROR] [{provider_name.upper()}] Specific fetch error: {e}")
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
