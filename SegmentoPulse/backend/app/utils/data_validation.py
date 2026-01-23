"""
Data Validation and Sanitization Layer
FAANG-Level Quality Control for News Articles

EMERGENCY HOTFIX (2026-01-23): Fixed AttributeError 'Article' object has no attribute 'get'
- Now supports both Pydantic Article models AND dicts
- Converts Pydantic models to dicts safely before validation
"""

from typing import Dict, Optional, List, Union
from datetime import datetime
import re
from urllib.parse import urlparse


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
    
    # Required: Published date
    if not article_dict.get('publishedAt'):
        return False
    
    # Optional but validate if present: Image URL
    if article_dict.get('image'):
        image_url = article_dict['image'].strip()
        if image_url and not image_url.startswith(('http://', 'https://')):
            # Invalid image URL - set to None
            article_dict['image'] = None
    
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
    
    # Clean image URL
    image_url = article_dict.get('image', '').strip() if article_dict.get('image') else None
    if image_url:
        image_url = image_url[:1000]
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
    published_at = article_dict.get('publishedAt')
    if isinstance(published_at, datetime):
        published_at = published_at.isoformat()
    
    return {
        'title': title,
        'url': url,
        'description': description or '',
        'image': image_url,
        'publishedAt': published_at,
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
    
    # Combine title and description for checking
    title = article_dict.get('title', '').lower()
    description = article_dict.get('description', '').lower()
    text = f"{title} {description}"
    
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
