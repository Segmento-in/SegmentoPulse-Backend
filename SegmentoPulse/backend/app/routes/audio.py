from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
import os
import aiofiles
from typing import Optional
from datetime import datetime
from app.services.appwrite_db import get_appwrite_db
from app.services.audio_service import audio_service
from app.config import settings

router = APIRouter()

class AudioGenerationRequest(BaseModel):
    article_url: str
    collection_id: str = settings.APPWRITE_COLLECTION_ID
    title: Optional[str] = None
    image_url: Optional[str] = None
    category: Optional[str] = None

class AudioResponse(BaseModel):
    success: bool
    audio_url: str
    message: str

@router.post("/generate", response_model=AudioResponse)
async def generate_audio_summary(request: AudioGenerationRequest):
    """
    Generate audio summary for an article by URL
    """
    try:
        # DEBUG: Log incoming request
        print(f"\n🎵 ========== AUDIO GENERATION REQUEST ==========")
        print(f"📝 URL: {request.article_url}")
        print(f"📋 Title: {request.title}")
        print(f"🏷️  Category: {request.category}")
        print(f"🖼️  Image: {request.image_url}")
        print(f"===============================================\n")
        
        appwrite = get_appwrite_db()
        from appwrite.query import Query
        
        # 1. Fetch Article by URL
        # FIX: Use multi-collection search like engagement endpoints
        # Re-implement ID generation logic to find the doc directly
        import hashlib
        url_hash = hashlib.sha256(request.article_url.encode()).hexdigest()
        article_id = url_hash[:32]
        
        print(f"🔑 Generated Article ID: {article_id}")
        
        # Determine strict collection if category provided
        target_collection_ids = []
        if request.category:
            target_collection_ids.append(appwrite.get_collection_id(request.category))
        
        # Always fallback to checking ALL known collections if not found (Safety Net)
        fallback_collections = [
            settings.APPWRITE_COLLECTION_ID,
            settings.APPWRITE_CLOUD_COLLECTION_ID,
            settings.APPWRITE_AI_COLLECTION_ID,
            settings.APPWRITE_DATA_COLLECTION_ID,
            settings.APPWRITE_MAGAZINE_COLLECTION_ID,
            settings.APPWRITE_MEDIUM_COLLECTION_ID
        ]
        
        for cid in fallback_collections:
            if cid and cid not in target_collection_ids:
                target_collection_ids.append(cid)
        
        article = None
        found_collection_id = None
        
        # Try to find article in target collections
        for collection_id in target_collection_ids:
            try:
                article = appwrite.tablesDB.get_row(
                    database_id=settings.APPWRITE_DATABASE_ID,
                    collection_id=collection_id,
                    document_id=article_id
                )
                found_collection_id = collection_id
                print(f"✅ Found article in collection: {collection_id}")
                break
            except Exception:
                continue
        
        # If not found, create it
        if not article:
            print(f"Audio: Article not found in any collection, creating from metadata... URL: {request.article_url}")
            
            if not request.title:
                raise HTTPException(status_code=404, detail="Article not found and no title provided for creation")

            # Determine target collection for creation
            target_collection_id = appwrite.get_collection_id(request.category) if request.category else settings.APPWRITE_COLLECTION_ID
            
            # Create document
            new_doc = {
                "url": request.article_url,
                "title": request.title,
                "image_url": request.image_url or "",
                "source": "pulse-audio",
                "published_at": datetime.now().isoformat(),
                "fetched_at": datetime.now().isoformat(),
                "likes": 0,
                "dislike": 0,
                "views": 0,
                "category": request.category or "wildcard",
                "url_hash": url_hash  # Store full hash
            }
            
            appwrite.tablesDB.create_row(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=target_collection_id,
                document_id=article_id,
                data=new_doc
            )
            
            # Fetch it back
            article = appwrite.tablesDB.get_row(
                database_id=settings.APPWRITE_DATABASE_ID,
                collection_id=target_collection_id,
                document_id=article_id
            )
            found_collection_id = target_collection_id
            print(f"✅ Created article in collection: {target_collection_id}")

        # 2. Check if audio already exists
        if article.get('audio_url'):
            return AudioResponse(
                success=True,
                audio_url=article['audio_url'],
                message="Audio already exists"
            )
            
        # 3. Prepare text for summary
        # FETCH FULL CONTENT using Trafilatura
        import trafilatura
        
        # Determine URL to scrape
        target_view_url = article.get('url', request.article_url)
        
        # Scrape
        print(f"Scraping content from: {target_view_url}")
        downloaded = trafilatura.fetch_url(target_view_url)
        extracted_text = trafilatura.extract(downloaded) if downloaded else None
        
        # Fallback to description if scraping fails
        if not extracted_text or len(extracted_text) < 100:
            print("Scraping failed or content too short, falling back to description")
            text_content = f"{article.get('title', '')}. {article.get('description', '')}"
        else:
            # Truncate to avoid token limits (Groq Llama3-8b limit ~8k tokens, but let's keep it safe)
            # 10,000 chars is roughly 2-3k tokens.
            text_content = extracted_text[:10000]
            
        if not text_content or len(text_content) < 10:
             raise HTTPException(status_code=400, detail="Article content too short for summary")
             
        # 4. Generate Summary (Groq)
        # Update prompt to reflect full article usage
        summary = await audio_service.generate_summary(text_content)
        if not summary:
             raise HTTPException(status_code=500, detail="Failed to generate summary")
             
        # 5. Generate Audio (EdgeTTS)
        temp_filename = f"audio_{article_id}.mp3"
        temp_path = os.path.abspath(temp_filename)
        
        audio_success = await audio_service.generate_audio(summary, temp_path)
        if not audio_success or not os.path.exists(temp_path):
             raise HTTPException(status_code=500, detail="Failed to generate audio file")
             
        # 6. Upload to Appwrite
        audio_url = await audio_service.upload_audio(temp_path, temp_filename)
        
        # 7. Cleanup temp file
        try:
            os.remove(temp_path)
        except Exception as e:
            print(f"Warning: Failed to delete temp file {temp_path}: {e}")
            
        if not audio_url:
             raise HTTPException(status_code=500, detail="Failed to upload audio to storage")
             
        # 8. Update Article
        update_success = await appwrite.update_article_audio(
            collection_id=found_collection_id,
            document_id=article_id,
            audio_url=audio_url
        )
        
        return AudioResponse(
            success=True,
            audio_url=audio_url,
            message="Audio generated successfully"
        )

    except HTTPException as he:
        print(f"❌ HTTPException in audio generation: {he.status_code} - {he.detail}")
        raise
    except Exception as e:
        print(f"❌ Unexpected error in audio generation: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
