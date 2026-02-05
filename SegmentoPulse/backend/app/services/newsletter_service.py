"""
Newsletter Service
Orchestrates newsletter sending with time-based preferences and smart content selection.
Handles IST-to-UTC timezone conversion and stale data protection.
"""
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import pytz
from app.services.appwrite_db import get_appwrite_db
from app.services.firebase_service import get_firebase_service
from app.services.brevo_email_service import get_brevo_service
from app.services.alert_service import alert_zero_articles, alert_quota_exhausted, alert_high_failure_rate
from app.config import settings


# Timezone constants
IST = pytz.timezone('Asia/Kolkata')
UTC = pytz.timezone('UTC')


# Newsletter configuration
PREFERENCE_CONFIG = {
    "Morning": {
        "hours_back": 12,
        "max_articles": 5,
        "subject": "☀️ Your Morning Tech Brief - SegmentoPulse",
        "greeting": "Good morning! Start your day with the latest tech news:"
    },
    "Afternoon": {
        "hours_back": 6,
        "max_articles": 5,
        "subject": "📰 Midday Tech Update - SegmentoPulse",
        "greeting": "Here's your midday tech update to keep you informed:"
    },
    "Evening": {
        "hours_back": 24,
        "max_articles": 7,
        "subject": "🌙 Evening Tech Digest - SegmentoPulse",
        "greeting": "Wrapping up the day? Catch up on today's top stories:"
    },
    "Weekly": {
        "days_back": 7,
        "max_articles": 15,
        "subject": "📅 Your Weekly Tech Roundup - SegmentoPulse",
        "greeting": "Your curated tech highlights from the past week:"
    },
    "Monthly": {
        "days_back": 30,
        "max_articles": 25,
        "subject": "📊 Monthly Tech Intelligence - SegmentoPulse",
        "greeting": "The most impactful tech stories from this month:"
    }
}


async def get_newsletter_content(preference: str) -> List[Dict]:
    """
    Fetch articles using "Category Carousel" (Round-Robin) Logic.
    
    Strategy:
    1. Primary Slots: Fetch top articles from AI, Cloud, Data Engineering (2 each).
    2. Wildcard Slot: Fetch 1 random article from other categories.
    3. Time Window: Apply strict time filtering (Morning/Afternoon/Evening).
    4. Fallback: If primary empty, fill with others to reach 5 articles.
    """
    from appwrite.query import Query
    import random
    
    if preference not in PREFERENCE_CONFIG:
        print(f"❌ Invalid preference: {preference}")
        return []
    
    config = PREFERENCE_CONFIG[preference]
    appwrite_db = get_appwrite_db()
    
    if not appwrite_db.initialized:
        print("⚠️ Appwrite database not initialized")
        return []
    
    try:
        # Step 1: Calculate Time Windows (Keep existing logic)
        now_ist = datetime.now(IST)
        
        # Default defaults
        start_time = now_ist - timedelta(hours=24)
        end_time = now_ist
        
        if preference == "Morning":
            end_time = now_ist  
            start_time = now_ist - timedelta(hours=8)
        elif preference == "Afternoon":
            end_time = now_ist 
            start_time = now_ist - timedelta(hours=7)
        elif preference == "Evening":
            end_time = now_ist
            start_time = now_ist - timedelta(hours=5)
        elif preference == "Weekly":
            end_time = now_ist
            start_time = now_ist - timedelta(days=7)
        elif preference == "Monthly":
            end_time = now_ist
            start_time = now_ist - timedelta(days=30)

        start_utc = start_time.astimezone(UTC)
        end_utc = end_time.astimezone(UTC)
        
        print(f"🔍 Fetching {preference} (Round-Robin)...")
        print(f"   Window (UTC): {start_utc.isoformat()} to {end_utc.isoformat()}")

        # Step 2: Define Categories
        primary_categories = ["ai", "cloud-computing", "data-engineering"]
        wildcard_categories = [
            "data-security", "data-privacy", "business-intelligence", 
            "magazines", "data-centers"
        ]
        
        selected_articles = []
        seen_urls = set()

        async def fetch_category(cat, limit):
            queries = [
                Query.equal('category', cat),
                Query.greater_than_equal('published_at', start_utc.isoformat()),
                Query.less_than_equal('published_at', end_utc.isoformat()),
                Query.order_desc('published_at'), # Fallback sort since we lack engagement metrics
                Query.limit(limit)
            ]
            return await appwrite_db.get_articles_with_queries(queries)

        # Step 3: Fetch Primary Slots (2 each)
        for cat in primary_categories:
            articles = await fetch_category(cat, 2)
            for a in articles:
                if a['url'] not in seen_urls:
                    selected_articles.append(a)
                    seen_urls.add(a['url'])
        
        # Step 4: Fetch Wildcard Slot (1 random category)
        random_wildcard = random.choice(wildcard_categories)
        wild_articles = await fetch_category(random_wildcard, 2) # Fetch 2 just in case
        for a in wild_articles:
            if a['url'] not in seen_urls and len(selected_articles) < 5:
                selected_articles.append(a)
                seen_urls.add(a['url'])
                break # Just 1 wildcard needed usually
                
        # Step 5: Fallback / Fill Up
        # If we don't have 5 articles yet, scan other categories to fill up
        if len(selected_articles) < 5:
            print("⚠️ Primary slots underfilled, running fallback fill...")
            remaining_cats = list(set(wildcard_categories) - {random_wildcard})
            for cat in remaining_cats:
                if len(selected_articles) >= 5:
                    break
                fillers = await fetch_category(cat, 2)
                for a in fillers:
                    if a['url'] not in seen_urls:
                        selected_articles.append(a)
                        seen_urls.add(a['url'])
                        if len(selected_articles) >= 5:
                            break
                            
        # Final Limit
        final_list = selected_articles[:5]
        
        print(f"✅ Selected {len(final_list)} articles (Round-Robin)")
        for a in final_list:
            print(f"   - [{a.get('category')}] {a.get('title')[:40]}...")
            
        return final_list

    except Exception as e:
        print(f"❌ Error fetching newsletter content: {e}")
        import traceback
        traceback.print_exc()
        return []


async def send_scheduled_newsletter(preference: str) -> Dict[str, int]:
    """
    Main newsletter orchestrator.
    
    CRITICAL SAFETY CHECKS:
    1. Validates preference parameter
    2. Fetches content from Appwrite (with timezone conversion)
    3. SKIPS sending if no articles found (stale data protection)
    4. Gets subscribers for this preference
    5. Sends emails via Brevo API
    
    Returns: {"sent": int, "failed": int, "skipped": Optional[str]}
    """
    print(f"\n{'='*80}")
    print(f"📧 NEWSLETTER SEND TRIGGER: {preference}")
    print(f"⏰ Trigger Time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"{'='*80}\n")
    
    # Validation
    if preference not in PREFERENCE_CONFIG:
        print(f"❌ Invalid preference: {preference}")
        return {"sent": 0, "failed": 0, "skipped": "invalid_preference"}
    
    # SAFETY CHECK #1: Fetch articles with timezone conversion
    articles = await get_newsletter_content(preference)
    
    if not articles or len(articles) == 0:
        # CRITICAL ALERT: Zero articles found
        error_msg = f"CRITICAL ALERT: No articles for {preference} newsletter!"
        timestamp_ist = datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S %Z')
        
        print(f"")
        print(f"{'!'*80}")
        print(f"⚠️  {error_msg}")
        print(f"   Preference: {preference}")
        print(f"   Time: {timestamp_ist}")
        print(f"   Possible causes:")
        print(f"   1. News fetcher hasn't run yet")
        print(f"   2. All APIs hit rate limits (429 errors)")
        print(f"   3. No articles match the time window")
        print(f"   4. Appwrite database is empty")
        print(f"   ")
        print(f"   ACTION REQUIRED: Check /api/admin/scheduler/fetch-now")
        print(f"{'!'*80}")
        print(f"")
        
        # ACTIVE ALERT: Send webhook to admin (Discord/Slack)
        await alert_zero_articles(preference, timestamp_ist)
        
        return {"sent": 0, "failed": 0, "skipped": "no_articles", "alert": True}
    
    # SAFETY CHECK #2: Get subscribers for this preference
    firebase = get_firebase_service()
    subscribers = firebase.get_subscribers_by_preference(preference)
    
    if not subscribers or len(subscribers) == 0:
        print(f"ℹ️ SKIP: No active subscribers for {preference} preference.")
        print(f"\n{'='*80}\n")
        return {"sent": 0, "failed": 0, "skipped": "no_subscribers"}
    
    print(f"👥 Found {len(subscribers)} active subscribers")
    print(f"📰 Sending {len(articles)} curated articles")
    
    # Send newsletter via Brevo
    config = PREFERENCE_CONFIG[preference]
    brevo = get_brevo_service()
    
    result = brevo.send_newsletter(
        preference=preference,
        subject=config["subject"],
        greeting=config["greeting"],
        articles=articles,
        subscribers=subscribers
    )
    
    # Check for quota issues and alert if needed
    if result.get('quota_limited', False):
        print(f"")
        print(f"{'!'*80}")
        print(f"⚠️  QUOTA ALERT: Brevo API limit reached!")
        print(f"   Sent: {result['sent']}")
        print(f"   Skipped: {result.get('skipped_count', 0)}")
        print(f"   Remaining credits: {result.get('remaining_credits', 'unknown')}")
        print(f"   ")
        print(f"   ACTION REQUIRED: Upgrade Brevo plan or reduce frequency")
        print(f"{'!'*80}")
        print(f"")
        
        # ACTIVE ALERT: Send webhook to admin
        await alert_quota_exhausted(
            preference=preference,
            sent=result['sent'],
            skipped=result.get('skipped_count', 0),
            remaining=result.get('remaining_credits', 0)
        )
    
    # Check for high failure rate and alert
    total_attempted = result.get('sent', 0) + result.get('failed', 0)
    if total_attempted > 0:
        failure_rate = result.get('failed', 0) / total_attempted
        await alert_high_failure_rate(
            preference=preference,
            sent=result.get('sent', 0),
            failed=result.get('failed', 0),
            failure_rate=failure_rate
        )
    
    print(f"\n✅ Newsletter send complete!")
    print(f"   Sent: {result.get('sent', 0)}")
    print(f"   Failed: {result.get('failed', 0)}")
    if result.get('quota_limited'):
        print(f"   ⚠️ Quota Limited: {result.get('skipped_count', 0)} skipped")
    print(f"   Remaining credits: {result.get('remaining_credits', 'N/A')}")
    print(f"{'='*80}\n")
    
    # Update last sent timestamp for all SENT subscribers only
    sent_count = result.get('sent', 0)
    if sent_count > 0:
        for i, subscriber in enumerate(subscribers[:sent_count]):
            email = subscriber.get('email')
            if email:
                firebase.update_last_sent(email)
    
    return result


def get_subscribers_by_preference(preference: str) -> List[Dict]:
    """
    Helper function to get subscribers for a specific preference.
    Used by admin endpoints and testing.
    """
    firebase = get_firebase_service()
    return firebase.get_subscribers_by_preference(preference)


async def preview_newsletter_content(preference: str) -> Dict:
    """
    Preview newsletter content without sending emails.
    Useful for testing and debugging.
    """
    articles = await get_newsletter_content(preference)
    
    return {
        "preference": preference,
        "article_count": len(articles),
        "articles": articles,
        "config": PREFERENCE_CONFIG.get(preference, {})
    }
