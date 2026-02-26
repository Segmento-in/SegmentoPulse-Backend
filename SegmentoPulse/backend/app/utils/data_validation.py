"""
Data Validation and Sanitization Layer
FAANG-Level Quality Control for News Articles

EMERGENCY HOTFIX (2026-01-23): Fixed AttributeError 'Article' object has no attribute 'get'
- Now supports both Pydantic Article models AND dicts
- Converts Pydantic models to dicts safely before validation
"""

from typing import Dict, Optional, List, Union
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo   # stdlib from Python 3.9+ — no extra install needed
import re
from urllib.parse import urlparse
from dateutil import parser as dateutil_parser


def is_valid_article(article: Union[Dict, 'Article']) -> bool:
    """
    Validate article data quality before database insertion
    
    HOTFIX: Now handles both Pydantic Article objects and dicts
    
    Returns True only if article meets all quality criteria
    """
    # HOTFIX: Convert Pydantic model to dict if needed
    if hasattr(article, 'model_dump'):
        # It's a Pydantic v2 model
        article_dict = article.model_dump()
    elif hasattr(article, 'dict'):
        # It's a Pydantic v1 model
        article_dict = article.dict()
    elif isinstance(article, dict):
        # Already a dict
        article_dict = article
    else:
        # Unknown type - reject
        return False
    
    # Required: Title must exist and be meaningful
    if not article_dict.get('title'):
        return False
    
    title = article_dict['title'].strip()
    if len(title) < 10 or len(title) > 500:
        return False
    
    # Required: Valid URL
    if not article_dict.get('url'):
        return False
    
    # Handle HttpUrl object from Pydantic
    url = article_dict['url']
    if hasattr(url, '__str__'):
        url = str(url)
    url = url.strip()
    
    if not url.startswith(('http://', 'https://')):
        return False
    
    # Validate URL format
    try:
        parsed = urlparse(url)
        if not parsed.netloc:
            return False
    except Exception:
        return False
    
    # Required: Published date must exist.
    raw_date = article_dict.get('publishedAt') or article_dict.get('published_at')
    if not raw_date:
        return False

    # ── FRESHNESS GATE ────────────────────────────────────────────────────────
    # We only want articles published today, where "today" is measured in
    # Indian Standard Time (IST = UTC+5:30) — because that is where our
    # users are.
    #
    # Why IST and not UTC?
    # With UTC midnight as the cutoff, articles published in India between
    # 12:00 AM IST and 5:30 AM IST (the first 5.5 hours of the Indian day)
    # were incorrectly rejected, because UTC midnight had not yet arrived.
    # Switching to IST midnight gives Indian users a full 24-hour day.
    #
    # CRITICAL ORDER: This check runs on the RAW date string, before
    # normalize_article_date() gets a chance to run. That function has a
    # silent fallback: if a date is unparseable it stamps the article with
    # 'right now'. Without this guard, a 3-day-old article with a broken
    # date string would survive normalization and appear fresh.
    try:
        if isinstance(raw_date, datetime):
            pub_dt = raw_date
        else:
            pub_dt = dateutil_parser.parse(str(raw_date))

        # Make timezone-aware if the provider gave us a naive datetime.
        if pub_dt.tzinfo is None:
            pub_dt = pub_dt.replace(tzinfo=timezone.utc)

        # Step 1: Find midnight IST today.
        # We get the current moment in IST, then zero out hours/minutes/seconds.
        # This gives us "12:00:00 AM of today in India".
        ist_zone   = ZoneInfo("Asia/Kolkata")
        now_ist    = datetime.now(ist_zone)
        cutoff_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)

        # Step 2: The article timestamp may be in any timezone (UTC, EST, etc.).
        # Python's datetime comparison handles mixed timezones correctly as long
        # as both sides are timezone-aware — which they both are here.
        if pub_dt < cutoff_ist:
            # Article was published before midnight IST today — reject it.
            return False

    except Exception:
        # If we genuinely cannot parse the date, we reject the article.
        # Better to miss one article than to save a zombie with a fake date.
        return False
    # ──────────────────────────────────────────────────────────────────────────

    # Optional but validate if present: Image URL
    # Handle both 'image' (raw API) and 'image_url' (Pydantic/DB)
    image_url = article_dict.get('image') or article_dict.get('image_url')
    if image_url:
        image_url = str(image_url).strip()
        if not image_url.startswith(('http://', 'https://')):
            # Invalid image URL - remove both keys to be safe
            if 'image' in article_dict: article_dict['image'] = None
            if 'image_url' in article_dict: article_dict['image_url'] = None

    return True


def sanitize_article(article: Union[Dict, 'Article']) -> Dict:
    """
    Clean and normalize article data
    
    HOTFIX: Now handles both Pydantic Article objects and dicts
    
    Ensures data fits schema constraints and is properly formatted
    """
    # HOTFIX: Convert Pydantic model to dict if needed
    if hasattr(article, 'model_dump'):
        article_dict = article.model_dump()
    elif hasattr(article, 'dict'):
        article_dict = article.dict()
    elif isinstance(article, dict):
        article_dict = article
    else:
        raise TypeError(f"Expected Dict or Article model, got {type(article)}")
    
    # Clean title
    title = article_dict.get('title', '').strip()
    title = re.sub(r'\s+', ' ', title)  # Normalize whitespace
    title = title[:500]  # Truncate to schema limit
    
    # Clean URL (handle HttpUrl objects)
    url = article_dict.get('url', '')
    if hasattr(url, '__str__'):
        url = str(url)
    url = url.strip()[:2048]
    
    # Clean description
    description = article_dict.get('description', '').strip()
    description = re.sub(r'\s+', ' ', description)
    description = description[:2000]
    
    # Clean image URL - Support both keys
    raw_image = article_dict.get('image') or article_dict.get('image_url')
    image_url = str(raw_image).strip() if raw_image else None
    
    if image_url:
        image_url = image_url[:2048] # Increased to match DB schema (was 1000)
        if not image_url.startswith(('http://', 'https://')):
            image_url = None
    
    # Clean source name
    source = article_dict.get('source', 'Unknown').strip()
    source = source[:200]
    
    # Generate slug from title
    slug = generate_slug(title)
    
    # Calculate quality score
    quality_score = calculate_quality_score(article_dict)
    
    # Handle publishedAt (convert datetime to ISO string if needed)
    # Check both keys
    published_at = article_dict.get('publishedAt') or article_dict.get('published_at')
    
    if isinstance(published_at, datetime):
        published_at = published_at.isoformat()
    elif not published_at:
        # Fallback to current time if missing
        published_at = datetime.now().isoformat()
    
    # Return standardized dict (using camelCase for legacy compatibility or standardized snake_case?)
    # The AppwriteDatabase understands both, checking 'published_at' OR 'publishedAt'.
    # But usually it's best to standardize on what the DB considers 'canonical'.
    # However, this function `sanitize_article` returns a dict that replaces the original object.
    # We should probably return both or standardize on snake_case?
    # Existing code returned 'publishedAt', 'image'.
    # Let's keep returning 'publishedAt' for backward compat with whatever else uses this,
    # BUT explicitly set the values we found.
    
    return {
        'title': title,
        'url': url,
        'description': description or '',
        'image': image_url, # Legacy key
        'image_url': image_url, # Modern key
        'publishedAt': published_at, # Legacy key
        'published_at': published_at, # Modern key
        'source': source,
        'category': article_dict.get('category', '').strip()[:100],
        'slug': slug,
        'quality_score': quality_score
    }


def generate_slug(title: str) -> str:
    """
    Generate URL-friendly slug from title
    
    Example: "Google Announces New AI" → "google-announces-new-ai"
    """
    slug = title.lower()
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)  # Remove special chars
    slug = re.sub(r'\s+', '-', slug)  # Replace spaces with hyphens
    slug = re.sub(r'-+', '-', slug)  # Remove duplicate hyphens
    slug = slug.strip('-')  # Remove leading/trailing hyphens
    slug = slug[:200]  # Limit length
    return slug


def calculate_quality_score(article: Dict) -> int:
    """
    Score article quality from 0-100
    
    Higher scores = better quality articles
    Used for sorting and filtering
    """
    score = 50  # Base score
    
    # Has image (+20)
    if article.get('image'):
        score += 20
    
    # Good description (+15)
    description = article.get('description', '')
    if len(description) > 100:
        score += 15
    
    # Premium sources (+15)
    source = article.get('source', '').lower()
    premium_sources = [
        'reuters', 'bloomberg', 'techcrunch', 'wired', 
        'the verge', 'zdnet', 'cnet', 'ars technica'
    ]
    if any(ps in source for ps in premium_sources):
        score += 15
    
    # Long title penalty (-10, might be clickbait)
    title = article.get('title', '')
    if len(title) > 100:
        score -= 10
    
    # Cap at 100
    return min(max(score, 0), 100)


def is_relevant_to_category(article: Union[Dict, 'Article'], category: str) -> bool:
    """
    Validate that article is relevant to the specified category
    
    HOTFIX: Now handles both Pydantic Article objects and dicts
    
    Prevents category pollution (e.g., "Apple pie" in Tech)
    
    Returns True only if article contains category-specific keywords
    """
    # HOTFIX: Convert to dict if needed
    if hasattr(article, 'model_dump'):
        article_dict = article.model_dump()
    elif hasattr(article, 'dict'):
        article_dict = article.dict()
    else:
        article_dict = article
    
    # Category keyword dictionaries
    # Each category has a list of words we scan for in the article's title,
    # description, AND URL path. If at least one word matches, the article passes.
    CATEGORY_KEYWORDS = {
        'ai': [
            'ai', 'artificial intelligence', 'machine learning', 'deep learning',
            'neural network', 'gpt', 'llm', 'chatgpt', 'generative ai',
            'computer vision', 'nlp', 'natural language', 'transformer'
        ],
        'data-security': [
            'security', 'cybersecurity', 'data breach', 'hacking', 'vulnerability',
            'encryption', 'malware', 'ransomware', 'firewall', 'threat'
        ],
        'data-governance': [
            'governance', 'compliance', 'regulation', 'audit', 'policy',
            'data quality', 'metadata', 'lineage', 'stewardship'
        ],
        'data-privacy': [
            'privacy', 'gdpr', 'ccpa', 'consent', 'personal data',
            'pii', 'anonymization', 'data protection', 'privacy law'
        ],
        'data-engineering': [
            'data engineering', 'pipeline', 'etl', 'big data', 'spark',
            'hadoop', 'kafka', 'airflow', 'data warehouse', 'snowflake'
        ],
        'data-management': [
            'data management', 'master data', 'mdm', 'data catalog',
            'data quality', 'data lineage', 'data stewardship',
            'data governance', 'data integration', 'reference data'
        ],
        'business-intelligence': [
            'business intelligence', 'bi', 'analytics', 'dashboard',
            'tableau', 'power bi', 'looker', 'reporting', 'kpi'
        ],
        'business-analytics': [
            'analytics', 'analysis', 'insights', 'metrics', 'data-driven',
            'business analytics', 'predictive', 'forecasting'
        ],
        'customer-data-platform': [
            'cdp', 'customer data', 'customer platform', 'crm',
            'customer experience', 'personalization', 'segmentation'
        ],
        'data-centers': [
            'data center', 'data centre', 'datacenter', 'server', 'infrastructure',
            'colocation', 'edge computing', 'hyperscale'
        ],
        'cloud-computing': [
            'cloud', 'aws', 'azure', 'google cloud', 'gcp', 'salesforce',
            'alibaba cloud', 'tencent cloud', 'huawei cloud', 'cloudflare',
            'saas', 'paas', 'iaas', 'serverless', 'kubernetes'
        ],
        # ── Cloud sub-categories (each maps to a specific provider) ──────────
        'cloud-aws': [
            'aws', 'amazon web services', 's3', 'ec2', 'lambda',
            'cloudfront', 'sagemaker', 'dynamodb', 'amazon'
        ],
        'cloud-azure': [
            'azure', 'microsoft azure', 'azure devops', 'azure ml',
            'azure openai', 'microsoft cloud'
        ],
        'cloud-gcp': [
            'gcp', 'google cloud', 'bigquery', 'vertex ai',
            'cloud run', 'dataflow', 'google cloud platform'
        ],
        'cloud-oracle': [
            'oracle cloud', 'oci', 'oracle database', 'oracle fusion',
            'oracle cloud infrastructure'
        ],
        'cloud-ibm': [
            'ibm cloud', 'ibm watson', 'red hat', 'openshift', 'ibm z'
        ],
        'cloud-alibaba': [
            'alibaba cloud', 'aliyun', 'alicloud'
        ],
        'cloud-digitalocean': [
            'digitalocean', 'droplet', 'app platform'
        ],
        'cloud-huawei': [
            'huawei cloud', 'huaweicloud'
        ],
        'cloud-cloudflare': [
            'cloudflare', 'cloudflare workers', 'cloudflare r2',
            'cloudflare pages', 'zero trust'
        ],
        # ── Content / publishing categories ───────────────────────────────────
        'medium-article': [
            'medium', 'article', 'blog', 'writing', 'publishing',
            'content', 'story', 'author', 'blogging'
        ],
        'magazines': [
            'technology', 'tech', 'innovation', 'digital', 'startup',
            'software', 'hardware', 'gadget'
        ]
    }
    
    # Get keywords for this category
    keywords = CATEGORY_KEYWORDS.get(category, [])
    
    if not keywords:
        # Unknown category - allow (don't reject)
        return True
    
    # Build the text we will search for keywords.
    # We use title + description as the primary source.
    # We also append the article's URL path because RSS feeds (especially Google News)
    # often return empty descriptions. The URL itself usually tells you what the
    # article is about — e.g. "/aws-launches-new-s3-feature" clearly contains 'aws' and 's3'.
    # Hyphens and slashes are replaced with spaces so words can be matched individually.
    title = (article_dict.get('title') or '').lower()
    description = (article_dict.get('description') or '').lower()

    # Extract the URL path safely.
    raw_url = article_dict.get('url') or ''
    url_str = str(raw_url).lower()
    try:
        parsed_url = urlparse(url_str)
        # Replace hyphens and slashes with spaces so
        # "/aws-new-s3-launch" becomes "aws new s3 launch".
        url_words = parsed_url.path.replace('-', ' ').replace('/', ' ')
    except Exception:
        url_words = ''

    text = f"{title} {description} {url_words}"
    
    # Count keyword matches
    matches = sum(1 for keyword in keywords if keyword.lower() in text)
    
    # Require at least 1 keyword match (lenient for now)
    # Can increase to 2+ for stricter filtering
    if matches >= 1:
        return True
    
    # Log rejection for monitoring
    print(f"🚫 Rejected '{article_dict.get('title', 'Unknown')[:50]}' from {category} (0 keyword matches)")
    return False


# Export functions
__all__ = [
    'is_valid_article',
    'sanitize_article',
    'generate_slug',
    'calculate_quality_score',
    'is_relevant_to_category'
]
