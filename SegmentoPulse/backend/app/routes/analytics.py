from fastapi import APIRouter, HTTPException
from app.models import ViewCountRequest, ViewCountResponse
from app.services.firebase_service import FirebaseService

router = APIRouter()
firebase_service = FirebaseService()

@router.post("/view", response_model=ViewCountResponse)
async def increment_view_count(request: ViewCountRequest):
    """
    Increment view count for an article
    """
    try:
        view_count = await firebase_service.increment_view(str(request.article_url))
        return ViewCountResponse(
            success=True,
            article_url=str(request.article_url),
            view_count=view_count
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/views")
async def get_view_count(article_url: str):
    """
    Get view count for an article
    """
    try:
        view_count = await firebase_service.get_view_count(article_url)
        return ViewCountResponse(
            success=True,
            article_url=article_url,
            view_count=view_count
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
