"""
Migration Script: Backfill Slug and Quality Score
Adds missing fields to existing articles in Appwrite

Run this once to update all existing articles with:
- slug: SEO-friendly URL slug
- quality_score: Article quality ranking (0-100)
"""

import asyncio
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.query import Query
import os
from dotenv import load_dotenv

# Add parent directory to path
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.utils.data_validation import generate_slug, calculate_quality_score

# Load environment variables
load_dotenv()

# Appwrite Configuration
APPWRITE_ENDPOINT = os.getenv('APPWRITE_ENDPOINT')
APPWRITE_PROJECT_ID = os.getenv('APPWRITE_PROJECT_ID')
APPWRITE_API_KEY = os.getenv('APPWRITE_API_KEY' )
APPWRITE_DATABASE_ID = os.getenv('APPWRITE_DATABASE_ID')
APPWRITE_COLLECTION_ID = os.getenv('APPWRITE_COLLECTION_ID')


async def migrate_articles():
    """
    Migrate existing articles to add slug and quality_score
    """
    print("=" * 60)
    print("📊 Appwrite Article Migration Script")
    print("=" * 60)
    print(f"Database: {APPWRITE_DATABASE_ID}")
    print(f"Collection: {APPWRITE_COLLECTION_ID}")
    print()
    
    # Initialize Appwrite client
    client = Client()
    client.set_endpoint(APPWRITE_ENDPOINT)
    client.set_project(APPWRITE_PROJECT_ID)
    client.set_key(APPWRITE_API_KEY)
    
    databases = Databases(client)
    
    # Fetch all articles (paginated)
    offset = 0
    limit = 100
    total_updated = 0
    total_skipped = 0
    total_errors = 0
    
    while True:
        try:
            print(f"📥 Fetching articles {offset + 1} to {offset + limit}...")
            
            # Query articles
            response = databases.list_documents(
                database_id=APPWRITE_DATABASE_ID,
                collection_id=APPWRITE_COLLECTION_ID,
                queries=[
                    Query.limit(limit),
                    Query.offset(offset)
                ]
            )
            
            documents = response['documents']
            
            if not documents:
                print("✅ No more articles to process")
                break
            
            print(f"📝 Processing {len(documents)} articles...")
            
            # Update each document
            for doc in documents:
                try:
                    doc_id = doc['$id']
                    title = doc.get('title', '')
                    
                    # Check if already has slug and quality_score
                    has_slug = doc.get('slug')
                    has_quality = doc.get('quality_score') is not None
                    
                    if has_slug and has_quality:
                        total_skipped += 1
                        continue
                    
                    # Generate missing fields
                    updates = {}
                    
                    if not has_slug:
                        updates['slug'] = generate_slug(title)
                    
                    if not has_quality:
                        updates['quality_score'] = calculate_quality_score({
                            'title': title,
                            'description': doc.get('description', ''),
                            'image': doc.get('image_url'),
                            'source': doc.get('source', '')
                        })
                    
                    # Update document
                    if updates:
                        databases.update_document(
                            database_id=APPWRITE_DATABASE_ID,
                            collection_id=APPWRITE_COLLECTION_ID,
                            document_id=doc_id,
                            data=updates
                        )
                        total_updated += 1
                        print(f"  ✓ Updated: {title[:50]}... (score: {updates.get('quality_score', 'N/A')})")
                
                except Exception as e:
                    total_errors += 1
                    print(f"  ✗ Error updating {doc.get('title', 'unknown')[:30]}: {e}")
                    continue
            
            # Move to next batch
            offset += limit
            
            # Small delay to avoid rate limiting
            await asyncio.sleep(0.5)
            
        except Exception as e:
            print(f"❌ Error fetching batch at offset {offset}: {e}")
            break
    
    # Summary
    print()
    print("=" * 60)
    print("📊 MIGRATION SUMMARY")
    print("=" * 60)
    print(f"✅ Updated: {total_updated} articles")
    print(f"⏭️  Skipped: {total_skipped} articles (already have fields)")
    print(f"❌ Errors: {total_errors} articles")
    print(f"📈 Total Processed: {total_updated + total_skipped + total_errors}")
    print("=" * 60)


if __name__ == "__main__":
    print("Starting migration...")
    asyncio.run(migrate_articles())
    print("Migration complete!")
