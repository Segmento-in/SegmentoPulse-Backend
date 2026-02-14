import asyncio
import logging
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from app.services.browser_manager import browser_manager

logging.basicConfig(level=logging.INFO)

async def test_browser_manager():
    print("Testing BrowserManager...")
    try:
        await browser_manager.start()
        
        url = "https://example.com"
        print(f"Fetching {url}...")
        content = await browser_manager.get_content(url)
        
        if content and "Example Domain" in content:
            print("✅ Successfully fetched content!")
        else:
            print("❌ Failed to fetch content or content mismatch.")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await browser_manager.shutdown()

if __name__ == "__main__":
    asyncio.run(test_browser_manager())
