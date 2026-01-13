from pydantic import BaseModel, HttpUrl, field_validator
from typing import Optional, List
from datetime import datetime
from email.utils import parsedate_to_datetime

class Article(BaseModel):
    """News article model"""
    title: str
    description: Optional[str] = ""
    url: HttpUrl
    image: Optional[str] = ""
    publishedAt: datetime
    source: Optional[str] = ""
    category: Optional[str] = ""
    
    @field_validator('publishedAt', mode='before')
    @classmethod
    def parse_datetime(cls, v):
        """Parse datetime from various formats including RFC 2822 (RSS feeds)"""
        if isinstance(v, datetime):
            return v
        if isinstance(v, str):
            try:
                # Try RFC 2822 format (used by RSS feeds like Google News)
                # Example: "Tue, 06 Jan 2026 19:14:27 GMT"
                return parsedate_to_datetime(v)
            except:
                try:
                    # Try ISO format
                    return datetime.fromisoformat(v.replace('Z', '+00:00'))
                except:
                    try:
                        # Fallback to dateutil parser
                        from dateutil import parser
                        return parser.parse(v)
                    except:
                        # Last resort: return current time
                        return datetime.now()
        return v

class NewsResponse(BaseModel):
    """Response model for news endpoints"""
    success: bool
    category: str
    count: int
    articles: List[Article]
    cached: bool = False

class SearchResponse(BaseModel):
    """Response model for search endpoints"""
    success: bool
    query: str
    count: int
    articles: List[Article]

class ViewCountRequest(BaseModel):
    """Request model for view count increment"""
    article_url: HttpUrl

class ViewCountResponse(BaseModel):
    """Response model for view count"""
    success: bool
    article_url: str
    view_count: int

class ErrorResponse(BaseModel):
    """Error response model"""
    success: bool = False
    error: str
    detail: Optional[str] = None
