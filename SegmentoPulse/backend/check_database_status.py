"""
Database Status Checker
Provides comprehensive diagnostics for Appwrite database and scheduler status
"""

import asyncio
import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

async def main():
    from app.services.appwrite_db import get_appwrite_db
    from app.services.scheduler import scheduler
    from app.config import settings
    from appwrite.query import Query
    
    print("=" * 80)
    print("🔍 DATABASE & SCHEDULER STATUS REPORT")
    print(f"📅 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print("")
    
    # ===== PART 1: Appwrite Connection =====
    print("━" * 80)
    print("📊 PART 1: APPWRITE DATABASE CONNECTION")
    print("━" * 80)
    
    appwrite_db = get_appwrite_db()
    print(f"✓ Initialized: {appwrite_db.initialized}")
    print(f"✓ Endpoint: {settings.APPWRITE_ENDPOINT}")
    print(f"✓ Database ID: {settings.APPWRITE_DATABASE_ID}")
    print(f"✓ Collection ID: {settings.APPWRITE_COLLECTION_ID}")
    print("")
    
    if not appwrite_db.initialized:
        print("❌ ERROR: Appwrite is not initialized!")
        print("💡 Check your .env file for valid credentials")
        return
    
    # ===== PART 2: Total Article Count =====
    print("━" * 80)
    print("📈 PART 2: TOTAL ARTICLE COUNT")
    print("━" * 80)
    
    try:
        stats = await appwrite_db.get_stats()
        total_articles = stats.get('total_articles', 0)
        
        print(f"🔢 TOTAL ARTICLES IN DATABASE: {total_articles:,}")
        print("")
        
        # ===== PART 3: Articles by Category =====
        print("━" * 80)
        print("📋 PART 3: ARTICLES BY CATEGORY")
        print("━" * 80)
        
        articles_by_category = stats.get('articles_by_category', {})
        for category, count in sorted(articles_by_category.items(), key=lambda x: x[1], reverse=True):
            print(f"   {category:30s} : {count:6,} articles")
        print("")
        
    except Exception as e:
        print(f"❌ Error getting stats: {e}")
        print("")
    
    # ===== PART 4: Age Analysis =====
    print("━" * 80)
    print("⏰ PART 4: ARTICLE AGE ANALYSIS")
    print("━" * 80)
    
    try:
        # Articles older than 48 hours (should be deleted by cleanup)
        cutoff_48h = (datetime.now() - timedelta(hours=48)).isoformat()
        response_48h = appwrite_db.databases.list_documents(
            database_id=settings.APPWRITE_DATABASE_ID,
            collection_id=settings.APPWRITE_COLLECTION_ID,
            queries=[
                Query.less_than('published_at', cutoff_48h),
                Query.limit(1)
            ]
        )
        old_articles = response_48h['total']
        
        # Articles from last 24 hours (recent)
        cutoff_24h = (datetime.now() - timedelta(hours=24)).isoformat()
        response_24h = appwrite_db.databases.list_documents(
            database_id=settings.APPWRITE_DATABASE_ID,
            collection_id=settings.APPWRITE_COLLECTION_ID,
            queries=[
                Query.greater_than('published_at', cutoff_24h),
                Query.limit(1)
            ]
        )
        recent_articles = response_24h['total']
        
        print(f"📅 Articles older than 48 hours: {old_articles:,}")
        print(f"   ⚠️  These SHOULD be cleaned up by the scheduler")
        print("")
        print(f"🆕 Articles from last 24 hours: {recent_articles:,}")
        print(f"   ✓ These are fresh articles")
        print("")
        
        if old_articles > 0:
            print("⚠️  WARNING: Old articles detected!")
            print(f"   The scheduler should delete {min(old_articles, 500)} articles on next run")
            print("")
        else:
            print("✅ GOOD: No articles older than 48 hours found")
            print("   Cleanup scheduler is working correctly!")
            print("")
            
    except Exception as e:
        print(f"❌ Error analyzing article age: {e}")
        print("")
    
    # ===== PART 5: Scheduler Status =====
    print("━" * 80)
    print("⏰ PART 5: SCHEDULER STATUS")
    print("━" * 80)
    
    print(f"🔄 Scheduler Running: {scheduler.running}")
    print("")
    
    jobs = scheduler.get_jobs()
    if jobs:
        print(f"📋 Registered Jobs: {len(jobs)}")
        print("")
        for job in jobs:
            print(f"   🔹 {job.name}")
            print(f"      ID: {job.id}")
            print(f"      Next Run: {job.next_run_time}")
            print(f"      Trigger: {job.trigger}")
            print("")
    else:
        print("⚠️  No jobs registered")
        print("💡 The scheduler might not have started yet")
        print("")
    
    # ===== PART 6: Cleanup Efficiency =====
    print("━" * 80)
    print("🧹 PART 6: CLEANUP SCHEDULER EFFICIENCY")
    print("━" * 80)
    
    print("📊 Cleanup Configuration:")
    print(f"   • Retention Policy: 48 hours (articles older than this are deleted)")
    print(f"   • Cleanup Frequency: Every 6 hours (00:00, 06:00, 12:00, 18:00 UTC)")
    print(f"   • Cleanup Capacity: 500 articles per run")
    print(f"   • Daily Cleanup Limit: 2,000 articles/day")
    print("")
    
    if old_articles > 0:
        days_to_clear = (old_articles / 2000)
        print(f"⏳ Estimated time to clear {old_articles:,} old articles:")
        print(f"   {days_to_clear:.1f} days at current cleanup rate")
        print("")
        
        if days_to_clear > 3:
            print("⚠️  WARNING: Cleanup is falling behind!")
            print("💡 Recommendations:")
            print("   1. Run manual cleanup: python test_cleanup.py")
            print("   2. Consider increasing cleanup limit in scheduler.py")
            print("   3. Reduce article retention to 24 hours instead of 48")
            print("")
    
    # ===== PART 7: Why Platform Shows Fewer Articles =====
    print("━" * 80)
    print("🔍 PART 7: WHY PLATFORM SHOWS FEWER ARTICLES")
    print("━" * 80)
    
    print(f"💡 Your database has {total_articles:,} articles")
    print(f"   BUT your API is configured to show only 20 articles per category")
    print("")
    print("📍 Location: app/routes/news.py:49")
    print("   Code: db_articles = await appwrite_db.get_articles(category, limit=20)")
    print("")
    print("This is INTENTIONAL for:")
    print("   ✓ Fast response times")
    print("   ✓ Better user experience")
    print("   ✓ Reduced bandwidth usage")
    print("")
    print("If you want to show more articles, you can:")
    print("   1. Increase the limit parameter in the API")
    print("   2. Implement pagination to load more articles on demand")
    print("")
    
    print("=" * 80)
    print("✅ REPORT COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(main())
