"""
Newsletter Test Script
Tests the newsletter service functionality without sending emails
"""

import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

async def main():
    print("=" * 80)
    print("🧪 NEWSLETTER SERVICE TEST")
    print("=" * 80)
    print("")
    
    # Test 1: Preview newsletter content
    print("📋 Test 1: Preview Newsletter Content")
    print("-" * 80)
    
    from app.services.newsletter_service import preview_newsletter_content
    
    preferences = ["Morning", "Afternoon", "Evening", "Weekly", "Monthly"]
    
    for preference in preferences:
        print(f"\n🔍 Testing {preference} preference...")
        result = await preview_newsletter_content(preference)
        
        print(f"   Articles found: {result['article_count']}")
        print(f"   Config: {result['config']['subject']}")
        
        if result['article_count'] > 0:
            print(f"   ✅ Content available for {preference} newsletter")
        else:
            print(f"   ⚠️  No articles found (may need to run fetcher first)")
    
    print("")
    print("-" * 80)
    
    # Test 2: Check subscriber analytics
    print("\n📊 Test 2: Subscriber Analytics")
    print("-" * 80)
    
    from app.services.firebase_service import get_firebase_service
    
    firebase = get_firebase_service()
    
    if firebase.initialized:
        all_subs = firebase.get_all_subscribers()
        
        print(f"   Total subscribers: {len(all_subs)}")
        
        # Count by preference
        pref_counts = {}
        for sub in all_subs:
            pref = sub.get('preference', 'Weekly')
            pref_counts[pref] = pref_counts.get(pref, 0) + 1
        
        print(f"   Distribution by preference:")
        for pref, count in pref_counts.items():
            print(f"      - {pref}: {count}")
        
        print(f"   ✅ Firebase service operational")
    else:
        print(f"   ⚠️  Firebase not initialized (credentials may be missing)")
    
    print("")
    print("-" * 80)
    
    # Test 3: Check scheduler jobs
    print("\n⏰ Test 3: Scheduler Jobs")
    print("-" * 80)
    
    from app.services.scheduler import scheduler
    
    jobs = scheduler.get_jobs()
    newsletter_jobs = [j for j in jobs if 'newsletter' in j.id]
    
    print(f"   Total scheduler jobs: {len(jobs)}")
    print(f"   Newsletter jobs: {len(newsletter_jobs)}")
    
    for job in newsletter_jobs:
        print(f"\n   Job: {job.name}")
        print(f"      ID: {job.id}")
        print(f"      Next run: {job.next_run_time}")
        print(f"      Trigger: {job.trigger}")
    
    if len(newsletter_jobs) == 5:
        print(f"\n   ✅ All 5 newsletter jobs registered correctly")
    else:
        print(f"\n   ⚠️  Expected 5 newsletter jobs, found {len(newsletter_jobs)}")
    
    print("")
    print("=" * 80)
    print("✅ Test Complete!")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
