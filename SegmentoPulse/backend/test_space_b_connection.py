"""
Test Space A -> Space B Connection
Quick verification script to ensure Phase 2 CQRS is working
"""
import requests
import json

# Configuration
SPACE_B_URL = "https://workwithshafisk-segmentopulse-factory.hf.space"

print("=" * 80)
print("🧪 Testing Space A -> Space B Connection")
print("=" * 80)

# Test 1: Health Check
print("\n1️⃣  Testing Space B Health Endpoint...")
try:
    response = requests.get(f"{SPACE_B_URL}/health", timeout=10)
    print(f"   ✅ Status: {response.status_code}")
    health_data = response.json()
    print(f"   📊 Status: {health_data.get('status')}")
    print(f"   🤖 Llama Loaded: {health_data.get('llama_loaded')}")
    print(f"   🔍 GLiNER Loaded: {health_data.get('gliner_loaded')}")
    print(f"   ⏱️  Uptime: {health_data.get('uptime_seconds')}s")
except Exception as e:
    print(f"   ❌ Error: {e}")

# Test 2: Process Article Endpoint
print("\n2️⃣  Testing /process-article Endpoint...")
test_article = """
Artificial Intelligence continues to transform industries worldwide. 
Recent advances in machine learning have enabled more efficient data processing.
Companies like Google and Microsoft are leading the AI revolution.
"""

try:
    response = requests.post(
        f"{SPACE_B_URL}/process-article",
        json={
            "text": test_article,
            "max_tokens": 100,
            "temperature": 0.7,
            "entity_labels": ["Organization", "Technology", "Location"],
            "entity_threshold": 0.5
        },
        timeout=60  # Llama-3 takes time
    )
    
    print(f"   ✅ Status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"   📝 Summary: {result.get('summary', '')[:100]}...")
        print(f"   🏷️  Tags: {result.get('tags', [])}")
        print(f"   ⏱️  Processing Time: {result.get('processing_time_ms')}ms")
        print(f"   🤖 Models: {result.get('model_info')}")
    else:
        print(f"   ❌ Response: {response.text[:200]}")
        
except Exception as e:
    print(f"   ❌ Error: {e}")

print("\n" + "=" * 80)
print("✅ Connection Test Complete")
print("=" * 80)
