"""
Test Script for Hybrid Search V2
=================================
Demonstrates the new search capabilities
"""

import asyncio
import httpx

BASE_URL = "http://localhost:8000"

async def test_search_v2():
    """Test the V2 search endpoint with various queries"""
    
    async with httpx.AsyncClient() as client:
        print("=" * 80)
        print("🔬 Testing Hybrid Search V2 Endpoint")
        print("=" * 80)
        
        # Test 1: Basic search
        print("\n📍 Test 1: Basic Search")
        print("-" * 40)
        response = await client.get(
            f"{BASE_URL}/api/search/v2",
            params={"q": "kubernetes"}
        )
        data = response.json()
        print(f"Query: 'kubernetes'")
        print(f"Results: {data['count']}")
        print(f"Cache Hit: {data['cache_hit']}")
        print(f"Processing Time: {data['processing_time_ms']}ms")
        
        if data['results']:
            top = data['results'][0]
            print(f"\nTop Result:")
            print(f"  Title: {top['title'][:60]}")
            print(f"  Final Score: {top['final_score']}")
            print(f"  Relevance: {top['relevance_score']}")
            print(f"  Time Decay: {top['time_decay']}")
            print(f"  Hours Old: {top['hours_old']}")
        
        # Test 2: Category filter
        print("\n\n📍 Test 2: Category Filter")
        print("-" * 40)
        response = await client.get(
            f"{BASE_URL}/api/search/v2",
            params={
                "q": "serverless",
                "category": "cloud-aws"
            }
        )
        data = response.json()
        print(f"Query: 'serverless' in category 'cloud-aws'")
        print(f"Results: {data['count']}")
        print(f"Filters Applied: {data['filters_applied']}")
        
        # Test 3: Cloud provider filter
        print("\n\n📍 Test 3: Cloud Provider Filter")
        print("-" * 40)
        response = await client.get(
            f"{BASE_URL}/api/search/v2",
            params={
                "q": "lambda",
                "cloud_provider": "aws",
                "limit": 5
            }
        )
        data = response.json()
        print(f"Query: 'lambda' for cloud_provider 'aws'")
        print(f"Results: {data['count']}")
        
        # Test 4: Recency filter
        print("\n\n📍 Test 4: Recency Filter")
        print("-" * 40)
        response = await client.get(
            f"{BASE_URL}/api/search/v2",
            params={
                "q": "artificial intelligence",
                "max_hours": 24
            }
        )
        data = response.json()
        print(f"Query: 'artificial intelligence' (last 24h)")
        print(f"Results: {data['count']}")
        
        if data['results']:
            print(f"\nAll results within 24h:")
            for r in data['results'][:3]:
                print(f"  - {r['title'][:50]} ({r['hours_old']}h old)")
        
        # Test 5: Cache hit
        print("\n\n📍 Test 5: Cache Hit Test")
        print("-" * 40)
        print("First call (cache miss)...")
        response1 = await client.get(
            f"{BASE_URL}/api/search/v2",
            params={"q": "nvidia"}
        )
        data1 = response1.json()
        print(f"  Cache Hit: {data1['cache_hit']}")
        print(f"  Time: {data1['processing_time_ms']}ms")
        
        print("\nSecond call (cache hit expected)...")
        response2 = await client.get(
            f"{BASE_URL}/api/search/v2",
            params={"q": "nvidia"}
        )
        data2 = response2.json()
        print(f"  Cache Hit: {data2['cache_hit']}")
        print(f"  Time: {data2['processing_time_ms']}ms")
        
        if data2['cache_hit']:
            speedup = data1['processing_time_ms'] / data2['processing_time_ms']
            print(f"  Speedup: {speedup:.1f}x faster!")
        
        # Test 6: Aggressive time decay
        print("\n\n📍 Test 6: Time Decay Comparison")
        print("-" * 40)
        
        # Default decay (0.1)
        response_default = await client.get(
            f"{BASE_URL}/api/search/v2",
            params={"q": "openai", "decay_factor": 0.1}
        )
        data_default = response_default.json()
        
        # Aggressive decay (0.5)
        response_aggressive = await client.get(
            f"{BASE_URL}/api/search/v2",
            params={"q": "openai", "decay_factor": 0.5}
        )
        data_aggressive = response_aggressive.json()
        
        print("Query: 'openai'")
        print(f"\nDefault decay (0.1):")
        if data_default['results']:
            top = data_default['results'][0]
            print(f"  Top: {top['title'][:50]}")
            print(f"  Hours Old: {top['hours_old']}")
            print(f"  Final Score: {top['final_score']}")
        
        print(f"\nAggressive decay (0.5):")
        if data_aggressive['results']:
            top = data_aggressive['results'][0]
            print(f"  Top: {top['title'][:50]}")
            print(f"  Hours Old: {top['hours_old']}")
            print(f"  Final Score: {top['final_score']}")
        
        print("\n" + "=" * 80)
        print("✅ All tests completed!")
        print("=" * 80)

if __name__ == "__main__":
    asyncio.run(test_search_v2())
