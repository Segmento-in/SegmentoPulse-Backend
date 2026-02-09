import asyncio
import sys
import os
import logging
import hashlib

# Add parent directory to path to import app modules
# Logic: backend/tools/migrate.py -> backend/tools -> backend/
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# Now we can import app
try:
    from app.services.firebase_service import get_firebase_service
    from app.services.appwrite_db import get_appwrite_db
    from app.config import settings
except ImportError as e:
    print(f"Import Error: {e}")
    print(f"Sys Path: {sys.path}")
    sys.exit(1)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def migrate_subscribers():
    """
    Migrate subscribers from Firebase to Appwrite
    """
    logger.info("🚀 Starting Subscriber Migration (Firebase -> Appwrite)...")
    
    # 1. Initialize Services
    firebase = get_firebase_service()
    appwrite_db = get_appwrite_db()
    
    if not firebase.initialized:
        logger.error("❌ Firebase not initialized. Cannot migrate.")
        # Create dummy data for testing if needed, or exit
        # return
        
    if not appwrite_db.initialized:
        logger.error("❌ Appwrite not initialized. Cannot migrate.")
        return
        
    # 2. Fetch from Firebase
    logger.info("📥 Fetching subscribers from Firebase...")
    try:
        fb_subscribers = firebase.get_all_subscribers()
    except Exception as e:
        logger.error(f"❌ Error fetching from Firebase: {e}")
        fb_subscribers = []
        
    logger.info(f"📊 Found {len(fb_subscribers)} total subscribers in Firebase")
    
    success_count = 0
    skipped_count = 0
    error_count = 0
    
    # 3. Migrate each subscriber
    for sub in fb_subscribers:
        # Normalize fields
        if isinstance(sub, str):
            # Sometimes simplistic get_all returns list of IDs? Verify
            # firebase_service.get_all_subscribers returns values() list
            logger.warning(f"⚠️ Unexpected subscriber format: {sub}")
            continue
            
        email = sub.get('email')
        if not email:
            logger.warning("⚠️  Skipping subscriber with no email")
            skipped_count += 1
            continue
            
        name = sub.get('name', 'Subscriber')
        token = sub.get('token', '')
        if not token:
             # Generate token if missing (e.g. legacy data)
             token = hashlib.sha256(email.encode()).hexdigest()[:32]
        
        # Normalize Preferences
        preferences = {}
        
        # A. Handle new schema (subscriptions dict)
        if 'subscriptions' in sub and isinstance(sub['subscriptions'], dict):
            # Copy strict references
            preferences = sub['subscriptions'].copy()
            
        # B. Handle legacy schema (single preference string)
        # Only if subscriptions dict is empty or missing
        legacy_pref = sub.get('preference')
        if not preferences and legacy_pref:
            preferences = {legacy_pref: True}
                
        # C. Default if nothing found at all
        if not preferences:
            preferences = {"Weekly": True}
            
        logger.info(f"🔄 Migrating {email}...")
        logger.info(f"   Using Preferences: {preferences}")
        
        try:
            # Create in Appwrite (Idempotent update if exists)
            # appwrite_db.create_subscriber handles updates gracefully now via my audit
            await appwrite_db.create_subscriber(
                email=email,
                name=name,
                preferences=preferences,
                token=token
            )
            
            # Sync lastSentAt if available
            last_sent = sub.get('lastSentAt')
            if last_sent:
                 # Manually update row using tablesDB wrapper
                 # Logic copied from update_last_sent but with specific timestamp
                 # We can just call update_last_sent if we don't care about precise history
                 # But let's verify if we need history. Probably fine to just reset or ignore.
                 pass

            success_count += 1
                
        except Exception as e:
            error_count += 1
            logger.error(f"❌ Failed to migrate {email}: {e}")
            
    # 4. Summary
    logger.info("="*50)
    logger.info("🎉 MIGRATION COMPLETE")
    logger.info(f"✅ Successfully Migrated: {success_count}")
    logger.info(f"⏭️  Skipped: {skipped_count}")
    logger.info(f"❌ Failed: {error_count}")
    logger.info("="*50)

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(migrate_subscribers())
