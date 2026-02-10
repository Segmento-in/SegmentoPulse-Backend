"""
Test script for audio generation API
Tests the complete flow from request to response
"""
import requests
import json

# Test configuration
API_URL = "http://127.0.0.1:8000/api/audio/generate"

# Sample article data
test_payload = {
    "article_url": "https://www.rawstory.com%2FTrump-ic e-2675183222%2F&title=Analyst%20issues%20dire%20warning%20about%20Trump%27s%20%27horrifying%27%20directive%20to%20ICE%20to%20detain%20immigration%20judges",
    "title": "Test Article for Audio Generation",
    "image_url": "https://example.com/image.jpg",
    "category": "ai"
}

print("=" * 60)
print("🧪 Testing Audio Generation API")
print("=" * 60)
print(f"\n📍 Endpoint: {API_URL}")
print(f"\n📦 Payload:")
print(json.dumps(test_payload, indent=2))
print("\n" + "=" * 60)

try:
    print("\n🚀 Sending POST request...")
    response = requests.post(
        API_URL,
        json=test_payload,
        headers={"Content-Type": "application/json"},
        timeout=60  # Audio generation can take time
    )
    
    print(f"\n📊 Response Status: {response.status_code}")
    print(f"\n📄 Response Body:")
    
    try:
        response_data = response.json()
        print(json.dumps(response_data, indent=2))
        
        if response.status_code == 200:
            if response_data.get('success'):
                print("\n✅ SUCCESS! Audio generated successfully")
                print(f"🔊 Audio URL: {response_data.get('audio_url')}")
            else:
                print("\n❌ FAILED: success=False in response")
                print(f"💬 Message: {response_data.get('message', 'No message')}")
        else:
            print(f"\n❌ HTTP ERROR: {response.status_code}")
            print(f"💬 Detail: {response_data.get('detail', 'No detail')}")
            
    except json.JSONDecodeError:
        print("⚠️  Response is not valid JSON:")
        print(response.text)
        
except requests.exceptions.ConnectionError:
    print("\n❌ CONNECTION ERROR: Cannot connect to backend")
    print("Make sure the backend is running on http://127.0.0.1:8000")
    
except requests.exceptions.Timeout:
    print("\n❌ TIMEOUT: Request took longer than 60 seconds")
    
except Exception as e:
    print(f"\n❌ UNEXPECTED ERROR: {type(e).__name__}: {e}")

print("\n" + "=" * 60)
