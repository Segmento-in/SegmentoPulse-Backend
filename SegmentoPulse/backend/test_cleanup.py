"""
Test script to manually trigger cleanup job
Run this to test if the cleanup scheduler works with Appwrite credentials
"""

import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

async def main():
    from app.services.scheduler import trigger_cleanup_now
    
    print("=" * 80)
    print("🧪 MANUAL TEST: Cleanup Scheduler")
    print("=" * 80)
    print("")
    
    await trigger_cleanup_now()
    
    print("")
    print("=" * 80)
    print("✅ Test completed!")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
