import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.models import Article
from app.utils.data_validation import is_valid_article, sanitize_article

def test_validation():
    print("🧪 Testing Data Validation Fix...")
    
    # 1. Create a Pydantic Article (snake_case fields)
    article = Article(
        title="Test Article Title For Validation",
        url="https://example.com/test-article",
        published_at=datetime.now(),
        image_url="https://example.com/image.jpg",
        source="Test Source",
        category="ai"
    )
    
    print(f"\n📋 Model Data: {article.model_dump()}")
    
    # 2. Test is_valid_article
    is_valid = is_valid_article(article)
    print(f"\n✅ is_valid_article: {is_valid}")
    
    if is_valid:
        print("   -> PASSED: Pydantic model validated successfully!")
    else:
        print("   -> FAILED: Pydantic model rejected!")
        
    # 3. Test sanitize_article
    sanitized = sanitize_article(article)
    print(f"\n🧹 Sanitized Data Keys: {list(sanitized.keys())}")
    print(f"   publishedAt: {sanitized.get('publishedAt')}")
    print(f"   published_at: {sanitized.get('published_at')}")
    print(f"   image: {sanitized.get('image')}")
    print(f"   image_url: {sanitized.get('image_url')}")
    
    if sanitized.get('publishedAt') and sanitized.get('image_url'):
         print("   -> PASSED: Sanitization preserved critical fields!")
    else:
         print("   -> FAILED: Sanitization missing fields!")

if __name__ == "__main__":
    test_validation()
