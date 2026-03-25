import asyncio
import httpx
from playwright.async_api import async_playwright
import sys

async def test_env():
    print(f"Testing environment on Python {sys.version}")
    
    # Test 1: HTTpx + AnyIO
    async with httpx.AsyncClient() as client:
        resp = await client.get("https://www.google.com")
        print(f"HTTpx test: {resp.status_code}")

    # Test 2: Playwright
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://www.google.com")
        title = await page.title()
        print(f"Playwright test: {title}")
        await browser.close()

if __name__ == "__main__":
    try:
        asyncio.run(test_env())
        print("RESULT: SUCCESS")
    except Exception as e:
        print(f"RESULT: FAILED - {e}")
