"""
Test script to view scheduler status and jobs
"""

import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

async def main():
    from app.services.scheduler import scheduler
    from app.services.appwrite_db import get_appwrite_db
    
    print("=" * 80)
    print("📊 SCHEDULER STATUS CHECK")
    print("=" * 80)
    print("")
    
    # Check Appwrite connection
    print("🔹 Appwrite Database Status:")
    appwrite_db = get_appwrite_db()
    print(f"   Initialized: {appwrite_db.initialized}")
    print("")
    
    # Check scheduler status  
    print("🔹 Scheduler Status:")
    print(f"   Running: {scheduler.running}")
    print("")
    
    # List jobs
    print("🔹 Registered Jobs:")
    jobs = scheduler.get_jobs()
    if jobs:
        for job in jobs:
            print(f"   - {job.name} (ID: {job.id})")
            print(f"     Next run: {job.next_run_time}")
            print(f"     Trigger: {job.trigger}")
            print("")
    else:
        print("   No jobs registered")
        print("")
    
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
