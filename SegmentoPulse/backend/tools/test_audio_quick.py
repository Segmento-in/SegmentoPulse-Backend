"""
Quick test script for audio API after fixes
"""
import requests
import json

print("=" * 60)
print("🎵 AUDIO API QUICK TEST")
print("=" * 60)

# Test 1: Health Check
print("\n1️⃣  Testing backend health...")
try:
    health = requests.get("http://127.0.0.1:8000/health", timeout=5)
    print(f"   ✅ Backend is running (Status: {health.status_code})")
except Exception as e:
    print(f"   ❌ Backend not responding: {e}")
    exit(1)

# Test 2: Audio Endpoint
print("\n2️⃣  Testing /api/audio/generate endpoint...")
payload = {
    "article_url": "https://www.example.com/test-article",
    "title": "Test Article Title",
    "category": "ai",
    "image_url": "https://www.example.com/image.jpg"
}

print(f"\n📦 Payload:")
print(json.dumps(payload, indent=2))

try:
    print("\n⏳ Sending request (this may take 10-30 seconds)...")
    response = requests.post(
        "http://127.0.0.1:8000/api/audio/generate",
        json=payload,
        timeout=60
    )
    
    print(f"\n📊 Status Code: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        if data.get('success'):
            print("✅ SUCCESS! Audio generated")
            print(f"🔊 Audio URL: {data.get('audio_url')}")
        else:
            print("⚠️  Request succeeded but success=False")
            print(f"Message: {data.get('message', 'No message')}")
    else:
        print(f"❌ HTTP {response.status_code}")
        try:
            error_data = response.json()
            print(f"Detail: {error_data.get('detail', 'No detail')}")
        except:
            print(f"Response: {response.text[:200]}")
            
except requests.exceptions.Timeout:
    print("❌ Request timed out (>60s)")
    print("Possible causes:")
    print("  - GROQ_API_KEY not set in .env")
    print("  - Appwrite bucket 'audio-summaries' doesn't exist")
    print("  - Article content scraping failed")
    
except Exception as e:
    print(f"❌ Error: {type(e).__name__}: {e}")

print("\n" + "=" * 60)
print("\n💡 If test failed, check:")
print("  1. GROQ_API_KEY in .env file")
print("  2. 'audio-summaries' bucket exists in Appwrite")
print("  3. Backend terminal logs for detailed error")
print("=" * 60)
