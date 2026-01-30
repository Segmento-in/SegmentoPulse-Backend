"""
Verify Agent Shadow Path
------------------------
This script manually triggers the Agent RAG pipeline with a mock article.
It validates:
1. CrewAI Agent Initialization (Groq).
2. Agent Analysis (Summary + Tags).
3. ChromaDB Embedding & Storage.
"""

import asyncio
import os
import logging
from datetime import datetime

# Setup basic logging to see the output
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_shadow_pipeline():
    print("🧪 [TEST] Starting Manual Verification of Shadow Path...")
    
    # 1. Imports (lazy loaded in actual code, strict here to fail fast)
    try:
        from app.services.agent_orchestrator import process_shadow_path, _vector_store, _pulse_analyst
        print("✅ [TEST] Modules imported successfully.")
    except ImportError as e:
        print(f"❌ [TEST] Import Error: {e}")
        print("   Did you run: pip install chromadb crewai langchain-groq sentence-transformers?")
        return

    # 2. Mock Article Data
    mock_article = {
        '$id': 'test_doc_001', # Simulating Appwrite ID
        'title': 'The Future of Agentic AI in 2026',
        'description': 'AI agents are moving from simple chatbots to autonomous workers capable of complex reasoning and task execution.',
        'url': 'https://example.com/agentic-ai-2026',
        'source': 'Test Source',
        'category': 'ai',
        'published_at': datetime.now().isoformat()
    }
    
    print(f"\n📄 [TEST] Processing Mock Article: '{mock_article['title']}'")
    
    # 3. Trigger Shadow Path
    # We call it directly instead of via create_task to await it and see the result
    try:
        await process_shadow_path([mock_article])
        print("✅ [TEST] `process_shadow_path` execution completed.")
    except Exception as e:
        print(f"❌ [TEST] Execution Failed: {e}")
        return

    # 4. Verify ChromaDB
    print("\n🔍 [TEST] Verifying Vector Store Storage...")
    try:
        # Force init if not already
        if not _vector_store._initialized:
            _vector_store._initialize()
            
        results = _vector_store.collection.query(
            query_texts=["autonomous workers"],
            n_results=1
        )
        
        if results['ids'] and results['ids'][0]:
            found_id = results['ids'][0][0]
            print(f"✅ [TEST] ChromaDB Query Successful! Found ID: {found_id}")
            print(f"   Metadata: {results['metadatas'][0][0]}")
            print(f"   Stored Text Fragment: {results['documents'][0][0][:100]}...")
            
            if found_id == mock_article['$id']:
                print("\n🎉 [SUCCESS] FULL VERIFICATION PASSED!")
                print("   The pipeline successfully analyzed, embedded, and stored the article.")
            else:
                print("⚠️ [TEST] ID Mismatch. Found something else?")
        else:
            print("❌ [TEST] Not found in ChromaDB.")
            
    except Exception as e:
        print(f"❌ [TEST] Verification Failed: {e}")

if __name__ == "__main__":
    # Check for API Key
    if not os.getenv("GROQ_API_KEY"):
        print("⚠️ [WARN] GROQ_API_KEY is not set. The Agent might fail or skip.")
        print("   Please set it in your environment or .env file before running.")
    
    asyncio.run(test_shadow_pipeline())
