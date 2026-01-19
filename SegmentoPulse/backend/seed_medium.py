"""
Seed Medium Article Category
=============================
One-time script to insert the initial/pinned Medium article into Appwrite database.

This ensures the "Medium Article" category starts with a specific guide article.

Usage:
    python seed_medium.py
"""

from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.id import ID
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def seed_medium_article():
    """Insert the seed Medium article into Appwrite"""
    
    # Initialize Appwrite client
    client = Client()
    client.set_endpoint(os.getenv('APPWRITE_ENDPOINT', 'https://cloud.appwrite.io/v1'))
    client.set_project(os.getenv('APPWRITE_PROJECT_ID'))
    client.set_key(os.getenv('APPWRITE_API_KEY'))
    
    # Initialize database service
    databases = Databases(client)
    
    # Database and collection IDs
    database_id = os.getenv('APPWRITE_DATABASE_ID', 'segmento_db')
    collection_id = os.getenv('APPWRITE_COLLECTION_ID', 'articles')
    
    # Article data to insert
    article_data = {
        'title': 'Using RSS feeds of profiles, publications, and topics',
        'description': 'Learn how to use RSS feeds to stay updated with your favorite Medium profiles, publications, and topics. This comprehensive guide covers everything you need to know about accessing and using Medium RSS feeds.',
        'url': 'https://help.medium.com/hc/en-us/articles/214874118-Using-RSS-feeds-of-profiles-publications-and-topics',
        'image': 'https://miro.medium.com/v2/resize:fit:1200/1*F0LADxTtsKOgmPa-_7iRcQ.png',  # Medium logo
        'publishedAt': datetime.now().isoformat(),
        'source': 'Medium Help',
        'category': 'medium-article',
        'isPinned': True,  # Mark as pinned so it always appears first
    }
    
    try:
        # Check if article already exists (by URL)
        existing = databases.list_documents(
            database_id=database_id,
            collection_id=collection_id,
            queries=[
                f'equal("url", "{article_data["url"]}")'
            ]
        )
        
        if existing['total'] > 0:
            print("✅ Article already exists in database")
            print(f"   Document ID: {existing['documents'][0]['$id']}")
            return existing['documents'][0]
        
        # Create the document
        result = databases.create_document(
            database_id=database_id,
            collection_id=collection_id,
            document_id=ID.unique(),
            data=article_data
        )
        
        print("✅ Successfully seeded Medium Article!")
        print(f"   Title: {result['title']}")
        print(f"   Document ID: {result['$id']}")
        print(f"   Category: {result['category']}")
        print(f"   Published At: {result['publishedAt']}")
        
        return result
        
    except Exception as e:
        print(f"❌ Error seeding article: {str(e)}")
        raise

if __name__ == '__main__':
    print("=" * 60)
    print("Seeding Medium Article Category")
    print("=" * 60)
    print()
    
    # Verify environment variables
    required_vars = ['APPWRITE_ENDPOINT', 'APPWRITE_PROJECT_ID', 'APPWRITE_API_KEY']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("   Please set them in your .env file")
        exit(1)
    
    seed_medium_article()
    
    print()
    print("=" * 60)
    print("Seeding Complete!")
    print("=" * 60)
