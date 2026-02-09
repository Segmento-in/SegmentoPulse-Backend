"""
Engagement API Test Utilities
==============================

Test script for validating engagement endpoints with SHA-256 IDs.

Usage:
    python test_engagement.py
    
Or from backend directory:
    python -m app.tests.test_engagement
"""

import asyncio
import httpx
from app.utils.id_generator import generate_article_id
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Test configuration
BASE_URL = "http://localhost:7860"  # Change for production
TEST_URLS = [
    "https://news.google.com/rss/articles/test-article-1",
    "https://venturebeat.com/ai/test-article-2",
    "https://techcrunch.com/test-article-3"
]


async def test_engagement_endpoints():
    """
    Comprehensive test suite for engagement API.
    
    Tests:
    1. Article stats retrieval
    2. Like functionality
    3. Dislike functionality  
    4. View tracking
    5. SHA-256 ID generation

    """
    
    print("\n" + "="*60)
    print("ENGAGEMENT API TEST SUITE")
    print("="*60 + "\n")
    
    async with httpx.AsyncClient() as client:
        for i, test_url in enumerate(TEST_URLS, 1):
            print(f"\n📝 Test Article {i}: {test_url[:50]}...")
            
            # Generate SHA-256 ID
            article_id = generate_article_id(test_url)
            print(f"   Generated ID: {article_id} (length: {len(article_id)})")
            
            # Test 1: Get Stats (should start at 0)
            try:
                response = await client.get(f"{BASE_URL}/api/engagement/articles/{article_id}/stats")
                if response.status_code == 200:
                    data = response.json()
                    print(f"   ✅ Stats: likes={data.get('likes')}, views={data.get('views')}, dislikes={data.get('dislikes')}")
                else:
                    print(f"   ❌ Stats failed: {response.status_code} - {response.text[:100]}")
                    continue
            except Exception as e:
logger.error(f"   ❌ Stats error: {e}")
                continue
            
            # Test 2: Like Article
            try:
                response = await client.post(f"{BASE_URL}/api/engagement/articles/{article_id}/like")
                if response.status_code == 200:
                    data = response.json()
                    print(f"   ✅ Like: {data.get('likes')} likes")
                else:
                    print(f"   ❌ Like failed: {response.status_code}")
            except Exception as e:
                logger.error(f"   ❌ Like error: {e}")
            
            # Test 3: Track View
            try:
                response = await client.post(f"{BASE_URL}/api/engagement/articles/{article_id}/view")
                if response.status_code == 200:
                    data = response.json()
                    print(f"   ✅ View: {data.get('views')} views")
                else:
                    print(f"   ❌ View failed: {response.status_code}")
            except Exception as e:
                logger.error(f"   ❌ View error: {e}")
            
            # Test 4: Dislike Article
            try:
                response = await client.post(f"{BASE_URL}/api/engagement/articles/{article_id}/dislike")
                if response.status_code == 200:
                    data = response.json()
                    print(f"   ✅ Dislike: {data.get('dislikes')} dislikes")
                else:
                    print(f"   ❌ Dislike failed: {response.status_code}")
            except Exception as e:
                logger.error(f"   ❌ Dislike error: {e}")
            
            # Test 5: Verify Final Stats
            try:
                response = await client.get(f"{BASE_URL}/api/engagement/articles/{article_id}/stats")
                if response.status_code == 200:
                    data = response.json()
                    print(f"   📊 Final Stats: likes={data.get('likes')}, views={data.get('views')}, dislikes={data.get('dislikes')}")
                    
                    # Validate expected values
                    if data.get('likes') >= 1 and data.get('views') >= 1 and data.get('dislikes') >= 1:
                        print(f"   ✅ All engagement metrics working correctly!")
                    else:
                        print(f"   ⚠️  Warning: Some metrics may not have incremented")
                else:
                    print(f"   ❌ Final stats check failed")
            except Exception as e:
                logger.error(f"   ❌ Final stats error: {e}")
    
    print("\n" + "="*60)
    print("TEST SUITE COMPLETE")
    print("="*60 + "\n")





async def test_id_generation():
    """Test SHA-256 ID generation utility."""
    print("\n" + "="*60)
    print("ID GENERATION TESTS")
    print("="*60 + "\n")
    
    test_cases = [
        "https://example.com/article-1",
        "https://example.com/article-1",  # Duplicate (should generate same ID)
        "https://example.com/article-2",  # Different URL
        "https://news.google.com/rss/articles/CBMiXWh0dHBz...",  # Very long URL
    ]
    
    for i, url in enumerate(test_cases, 1):
        article_id = generate_article_id(url)
        print(f"{i}. URL: {url[:60]}...")
        print(f"   ID:  {article_id} (length: {len(article_id)})")
        
        # Verify ID is valid Appwrite format
        if len(article_id) == 32 and article_id.isalnum():
            print(f"   ✅ Valid Appwrite document ID format")
        else:
            print(f"   ❌ Invalid format!")
    
    # Test duplicate detection
    id1 = generate_article_id(test_cases[0])
    id2 = generate_article_id(test_cases[1])
    if id1 == id2:
        print(f"\n✅ Duplicate URLs generate identical IDs (good for deduplication)")
    else:
        print(f"\n❌ Duplicate URLs generated different IDs!")
    
    print("\n" + "="*60)


async def main():
    """Run all tests."""
    print("\n🧪 Starting Engagement API Test Suite...\n")
    
    # Test 1: ID Generation
    await test_id_generation()
    
    # Test 2: Engagement Endpoints
    await test_engagement_endpoints()
    

    
    print("\n✅ All tests complete!\n")


if __name__ == "__main__":
    asyncio.run(main())
