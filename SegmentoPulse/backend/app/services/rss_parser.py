import feedparser
from typing import List
from datetime import datetime
from app.models import Article
import re

class RSSParser:
    """RSS feed parser for news sources"""
    
    async def parse_google_news(self, content: str, category: str) -> List[Article]:
        """Parse Google News RSS feed with advanced XML parsing"""
        try:
            articles = []
            
            # Extract items from XML using regex
            item_regex = r'<item>([\s\S]*?)</item>'
            matches = re.findall(item_regex, content)
            
            for item in matches[:20]:  # Limit to 20 articles
                title = self._extract_tag(item, 'title') or 'No title'
                link = self._extract_tag(item, 'link') or self._extract_tag(item, 'guid') or ''
                description = self._extract_tag(item, 'description') or self._extract_tag(item, 'content:encoded') or ''
                pub_date = self._extract_tag(item, 'pubDate') or self._extract_tag(item, 'published') or datetime.now().isoformat()
                creator = self._extract_tag(item, 'dc:creator') or self._extract_tag(item, 'author') or 'Google News'
                
                # Extract image from multiple sources
                image = self._extract_image_from_xml(item, description, category, title)
                
                # Extract source name from description (Google News format: <a href="...">Source</a>)
                source_match = re.search(r'<a[^>]*>([^<]+)</a>', description)
                article_source = source_match.group(1) if source_match else 'Google News'
                
                # Clean description (Google News RSS only contains links, not actual content)
                cleaned_description = self._clean_google_news_description(description)
                
                article = Article(
                    title=self._clean_html(title),
                    description=cleaned_description,
                    url=link,
                    image=image,
                    publishedAt=pub_date,
                    source=self._clean_html(article_source),
                    category=category
                )
                articles.append(article)
            
            return articles
        except Exception as e:
            print(f"Error parsing Google News: {e}")
            return []
    
    def _extract_image_from_xml(self, item: str, description: str, category: str, title: str) -> str:
        """Extract image from multiple XML sources with fallbacks"""
        # 1. Try enclosure tag
        enclosure_match = re.search(r'<enclosure[^>]*url="([^"]+)"', item)
        if enclosure_match:
            return enclosure_match.group(1)
        
        # 2. Try media:content or media:thumbnail
        media_match = re.search(r'<media:(content|thumbnail)[^>]*url="([^"]+)"', item)
        if media_match:
            return media_match.group(2)
        
        # 3. Try img tag in description
        img_match = re.search(r'<img[^>]*src="([^"]+)"', description)
        if img_match:
            return img_match.group(1)
        
        # 4. Category-specific fallback images
        fallbacks = {
            'ai': 'https://images.unsplash.com/photo-1677442136019-21780ecad995?w=400&h=200&fit=crop',
            'data-security': 'https://images.unsplash.com/photo-1563986768494-4dee2763ff3f?w=400&h=200&fit=crop',
            'data-governance': 'https://images.unsplash.com/photo-1551288049-bebda4e38f71?w=400&h=200&fit=crop',
            'data-privacy': 'https://images.unsplash.com/photo-1614064641938-3bbee52942c7?w=400&h=200&fit=crop',
            'data-engineering': 'https://images.unsplash.com/photo-1558494949-ef010cbdcc31?w=400&h=200&fit=crop',
            'business-intelligence': 'https://images.unsplash.com/photo-1551288049-bebda4e38f71?w=400&h=200&fit=crop',
            'business-analytics': 'https://images.unsplash.com/photo-1460925895917-afdab827c52f?w=400&h=200&fit=crop',
            'customer-data-platform': 'https://images.unsplash.com/photo-1432888622747-4eb9a8d82266?w=400&h=200&fit=crop',
            'data-centers': 'https://images.unsplash.com/photo-1544197150-b99a580bb7a8?w=400&h=200&fit=crop',
            'magazines': 'https://images.unsplash.com/photo-1504711434969-e33886168f5c?w=400&h=200&fit=crop',
        }
        
        # Use hash of title for consistent fallback selection
        default_images = list(fallbacks.values())
        index = abs(hash(title)) % len(default_images)
        return default_images[index]
    
    def _clean_google_news_description(self, description: str) -> str:
        """Clean Google News description - they typically only contain links, not actual content"""
        # Check if this is a Google News link-only description
        if 'news.google.com/rss/articles' in description:
            return ''  # No real content, just redirect links
        
        # Try to extract content after the link
        after_link_match = re.search(r'</a>([\s\S]*)', description)
        if after_link_match:
            extracted = self._clean_html(after_link_match.group(1))
            if len(extracted) > 30:
                return extracted[:200]
        
        # Fallback: clean entire description if meaningful
        full_clean = self._clean_html(description)
        if len(full_clean) > 30 and not full_clean.startswith('http'):
            return full_clean[:200]
        
        return ''
    
    def _extract_tag(self, xml: str, tag_name: str) -> str:
        """Extract XML tag content"""
        pattern = f'<{tag_name}[^>]*>([\\s\\S]*?)</{tag_name}>'
        match = re.search(pattern, xml, re.IGNORECASE)
        return match.group(1).strip() if match else ''
    
    def _clean_html(self, html: str) -> str:
        """Remove HTML tags and decode entities"""
        text = html
        
        # Remove CDATA
        text = re.sub(r'<!\[CDATA\[([\s\S]*?)\]\]>', r'\1', text)
        
        # Remove HTML tags (multiple passes for nested tags)
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'<[^>]*', '', text)
        text = re.sub(r'>', '', text)
        
        # Decode HTML entities
        entities = {
            '&nbsp;': ' ', '&amp;': '&', '&lt;': '<', '&gt;': '>',
            '&quot;': '"', '&#39;': "'", '&apos;': "'",
            '&hellip;': '...', '&mdash;': '—', '&ndash;': '–'
        }
        for entity, char in entities.items():
            text = text.replace(entity, char)
        
        # Remove numeric entities
        text = re.sub(r'&#\d+;', '', text)
        
        # Clean whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    async def parse_provider_rss(self, content: str, provider: str) -> List[Article]:
        """Parse cloud provider RSS feed"""
        try:
            feed = feedparser.parse(content)
            articles = []
            
            for entry in feed.entries[:20]:
                # Extract image
                image_url = self._extract_image_from_entry(entry)
                
                # Parse date
                published_at = self._parse_date(entry.get('published', ''))
                
                # Get description
                description = entry.get('summary', '')
                if description:
                    # Strip HTML tags
                    description = re.sub(r'<[^>]+>', '', description)
                    description = description[:200] + '...' if len(description) > 200 else description
                
                article = Article(
                    title=entry.get('title', ''),
                    description=description,
                    url=entry.get('link', ''),
                    image=image_url,
                    publishedAt=published_at,
                    source=provider.upper(),
                    category=f'cloud-{provider}'
                )
                articles.append(article)
            
            return articles
        except Exception as e:
            print(f"Error parsing provider RSS: {e}")
            return []
    
    def _extract_image_from_entry(self, entry) -> str:
        """Extract image URL from feed entry"""
        # Try media:content
        if hasattr(entry, 'media_content') and entry.media_content:
            return entry.media_content[0].get('url', '')
        
        # Try media:thumbnail
        if hasattr(entry, 'media_thumbnail') and entry.media_thumbnail:
            return entry.media_thumbnail[0].get('url', '')
        
        # Try enclosures
        if hasattr(entry, 'enclosures') and entry.enclosures:
            for enclosure in entry.enclosures:
                if enclosure.get('type', '').startswith('image'):
                    return enclosure.get('href', '')
        
        # Default fallback image
        return "https://via.placeholder.com/400x300/4F46E5/ffffff?text=Segmento+Pulse"
    
    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string to datetime"""
        try:
            # feedparser usually provides a parsed date
            # but we'll handle string parsing as fallback
            from dateutil import parser
            return parser.parse(date_str)
        except:
            return datetime.now()
