"""
Comprehensive Database Ingestion Diagnostic
Identifies why articles aren't being saved and retrieved properly
"""

import asyncio
import sys
import os
import logging
from datetime import datetime, timedelta

sys.path.append(os.getcwd())

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def diagnose_ingestion_pipeline():
    """
    Complete diagnostic of the ingestion pipeline
    """
    print("=" * 80)
    print("🔍 DATABASE INGESTION DIAGNOSTIC")
    print("=" * 80)
    
    try:
        from dotenv import load_dotenv
        load_dotenv()
        
        from app.services.appwrite_db import get_appwrite_db
        from app.config import settings
        
        db = get_appwrite_db()
        
        # Test 1: Check Appwrite Connection
        print("\n📡 TEST 1: Appwrite Database Connection")
        print("-" * 80)
        
        if not db.initialized:
            print("❌ CRITICAL: Appwrite database NOT initialized!")
            print(f"   Project ID: {settings.APPWRITE_PROJECT_ID}")
            print(f"   Database ID: {settings.APPWRITE_DATABASE_ID}")
            print(f"   Collection ID: {settings.APPWRITE_COLLECTION_ID}")
            print("\n💡 Check your .env file for correct Appwrite credentials")
            return
        
        print("✅ Appwrite connected successfully")
        print(f"   Database: {settings.APPWRITE_DATABASE_ID}")
        print(f"   Collection: {settings.APPWRITE_COLLECTION_ID}")
        
        # Test 2: Count Total Articles
        print("\n📊 TEST 2: Current Database State")
        print("-" * 80)
        
        from appwrite.query import Query
        
        try:
            # Get total count
            response = db.databases.list_documents(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=settings.APPWRITE_COLLECTION_ID,
                queries=[Query.limit(1)]
            )
            
            total_count = response.get('total', 0)
            print(f"📈 Total Articles in Database: {total_count}")
            
            if total_count == 0:
                print("⚠️  WARNING: Database is EMPTY!")
                print("   This explains why retrieval returns 0 articles")
            
        except Exception as e:
            print(f"❌ Error querying database: {e}")
            return
        
        # Test 3: Check Articles by Category
        print("\n📋 TEST 3: Articles Per Category")
        print("-" * 80)
        
        categories = [
            "ai", "cloud-aws", "cloud-azure", "cloud-gcp", "cloud-computing",
            "data-security", "business-intelligence"
        ]
        
        category_counts = {}
        for category in categories:
            try:
                response = db.databases.list_documents(
                    database_id=settings.APPWRITE_DATABASE_ID,
                    collection_id=settings.APPWRITE_COLLECTION_ID,
                    queries=[
                        Query.equal('category', category),
                        Query.limit(1)
                    ]
                )
                count = response.get('total', 0)
                category_counts[category] = count
                
                status = "✅" if count > 0 else "❌"
                print(f"   {status} {category}: {count} articles")
                
            except Exception as e:
                print(f"   ❌ {category}: Error - {e}")
        
        # Test 4: Check Recent Articles
        print("\n🕐 TEST 4: Recent Articles (Last 24 Hours)")
        print("-" * 80)
        
        try:
            cutoff_time = (datetime.now() - timedelta(hours=24)).isoformat()
            
            response = db.databases.list_documents(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=settings.APPWRITE_COLLECTION_ID,
                queries=[
                    Query.greater_than('fetched_at', cutoff_time),
                    Query.limit(10)
                ]
            )
            
            recent_count = response.get('total', 0)
            print(f"📅 Articles fetched in last 24h: {recent_count}")
            
            if recent_count == 0:
                print("⚠️  WARNING: No articles fetched recently!")
                print("   Scheduler may not be running or saving is failing")
            else:
                print(f"\n   Sample of {len(response['documents'])} most recent:")
                for i, doc in enumerate(response['documents'][:5], 1):
                    print(f"   {i}. {doc.get('title', 'N/A')[:60]}...")
                    print(f"      Category: {doc.get('category')}, Fetched: {doc.get('fetched_at')}")
            
        except Exception as e:
            print(f"❌ Error checking recent articles: {e}")
        
        # Test 5: Test Manual Save
        print("\n💾 TEST 5: Manual Article Save Test")
        print("-" * 80)
        
        test_article = {
            'title': f'TEST ARTICLE - {datetime.now().isoformat()}',
            'description': 'This is a test article to verify saving works',
            'url': f'https://test.com/article-{datetime.now().timestamp()}',
            'image': 'https://test.com/image.jpg',
            'publishedAt': datetime.now().isoformat(),
            'source': 'TEST',
            'category': 'ai'
        }
        
        try:
            saved_count, saved_docs = await db.save_articles([test_article])
            
            if saved_count > 0:
                print(f"✅ SUCCESS: Test article saved!")
                print(f"   Saved count: {saved_count}")
                print(f"   Document ID: {saved_docs[0].get('url_hash') if saved_docs else 'N/A'}")
            else:
                print(f"❌ FAILED: Test article NOT saved")
                print(f"   This indicates save_articles() is broken")
            
        except Exception as e:
            print(f"❌ CRITICAL ERROR in save_articles(): {e}")
            import traceback
            traceback.print_exc()
        
        # Test 6: Check Vectorization (Ollama)
        print("\n🤖 TEST 6: Ollama/Vectorization Status")
        print("-" * 80)
        
        try:
            from app.services.vector_store import vector_store
            
            print(f"   Vector Store Initialized: {vector_store is not None}")
            
            # Check if there are any vectors
            try:
                collection = vector_store.collection
                count = collection.count() if collection else 0
                print(f"   Vectors in ChromaDB: {count}")
                
                if count == 0:
                    print("   ⚠️  No vectors found - embeddings not being created")
                
            except Exception as e:
                print(f"   ⚠️  Cannot check vector count: {e}")
            
        except Exception as e:
            print(f"   ❌ Vector store error: {e}")
        
        # Test 7: Check if Agent Orchestrator is Running
        print("\n🕵️ TEST 7: Agent Orchestrator (Shadow Path)")
        print("-" * 80)
        
        try:
            from app.services.agent_orchestrator import process_shadow_path
            
            print("   Agent orchestrator module: ✅ Imported")
            print("   Note: This runs async in background after saves")
            print("   Check logs for 'Triggering Agent Analyst' messages")
            
        except Exception as e:
            print(f"   ❌ Cannot import agent_orchestrator: {e}")
        
        # SUMMARY
        print("\n" + "=" * 80)
        print("📋 DIAGNOSTIC SUMMARY")
        print("=" * 80)
        
        issues = []
        
        if total_count == 0:
            issues.append("❌ CRITICAL: Database is empty - no articles at all")
        
        if recent_count == 0:
            issues.append("❌ CRITICAL: No recent ingestion - scheduler not saving")
        
        if all(count == 0 for count in category_counts.values()):
            issues.append("❌ CRITICAL: All categories empty")
        
        if issues:
            print("\n🚨 ISSUES FOUND:")
            for issue in issues:
                print(f"   {issue}")
            
            print("\n💡 LIKELY ROOT CAUSES:")
            print("   1. Scheduler is not running (check if uvicorn started correctly)")
            print("   2. save_articles() is failing silently")
            print("   3. Appwrite permissions issue (check collection permissions)")
            print("   4. URL hash collisions causing all articles to be 'duplicates'")
        else:
            print("\n✅ No critical issues found in database")
            print("   Articles are being saved and retrieved normally")
        
    except Exception as e:
        print(f"\n❌ DIAGNOSTIC FAILED: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(diagnose_ingestion_pipeline())
