"""
Subscription API Routes
Handles newsletter subscriptions and unsubscribe functionality
"""
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, EmailStr, field_validator
from typing import List, Optional
from datetime import datetime

from app.services.brevo_email_service import get_brevo_service
from app.services.firebase_service import get_firebase_service

router = APIRouter(prefix="/api/subscription", tags=["subscription"])


# Pydantic models
class SubscribeRequest(BaseModel):
    email: EmailStr
    name: str
    topics: Optional[List[str]] = ["news", "security", "cloud", "ai"]
    preference: str = "Weekly"  # Default to Weekly for backward compatibility
    
    @field_validator('preference')
    @classmethod
    def validate_preference(cls, v):
        allowed = ["Morning", "Afternoon", "Evening", "Weekly", "Monthly"]
        if v not in allowed:
            raise ValueError(f"Preference must be one of: {allowed}")
        return v


class SubscribeResponse(BaseModel):
    success: bool
    message: str
    token: Optional[str] = None


class UnsubscribeResponse(BaseModel):
    success: bool
    message: str
    email: Optional[str] = None


@router.post("/subscribe", response_model=SubscribeResponse)
async def subscribe(request: SubscribeRequest):
    """
    Subscribe a user to the newsletter
    
    - Adds subscriber to Firebase
    - Sends welcome email via Brevo
    - Returns subscription token
    - Now supports time-based preferences (Morning/Afternoon/Evening/Weekly/Monthly)
    """
    try:
        firebase = get_firebase_service()
        brevo = get_brevo_service()
        
        # Generate unique token
        token = brevo.generate_unsubscribe_token(request.email)
        
        # Add subscriber to Firebase with preference
        subscriber_data = {
            "email": request.email,
            "name": request.name,
            "subscribed": True,
            "token": token,
            "subscribedAt": datetime.now().isoformat(),
            "topics": request.topics,
            "preference": request.preference  # NEW: Store newsletter preference
        }
        
        success = firebase.add_subscriber(request.email, subscriber_data)
        
        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to save subscriber to database"
            )
        
        # Send welcome email (could be enhanced to mention preference)
        email_sent = brevo.send_welcome_email(
            email=request.email,
            name=request.name,
            token=token
        )
        
        if not email_sent:
            # Subscriber added but email failed
            return SubscribeResponse(
                success=True,
                message=f"Subscribed to {request.preference} newsletter! Check your email for confirmation.",
                token=token
            )
        
        return SubscribeResponse(
            success=True,
            message=f"Successfully subscribed to {request.preference} newsletter! Check your email for confirmation.",
            token=token
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in subscribe endpoint: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Subscription failed: {str(e)}"
        )


@router.get("/unsubscribe", response_model=UnsubscribeResponse)
async def unsubscribe(
    token: str = Query(..., description="Unsubscribe token from email"),
    preference: Optional[str] = Query(None, description="Specific newsletter to unsubscribe from")
):
    """
    Unsubscribe user via email link
    Supports Granular Unsubscribe (e.g., 'Morning' only)
    """
    try:
        firebase = get_firebase_service()
        brevo = get_brevo_service()
        
        # Find subscriber by token
        subscriber = firebase.get_subscriber_by_token(token)
        
        if not subscriber:
            raise HTTPException(
                status_code=404,
                detail="Invalid or expired unsubscribe link"
            )
        
        email = subscriber.get('email')
        name = subscriber.get('name', 'Subscriber')
        
        success = False
        message = ""
        
        if preference:
            # GRANULAR UNSUBSCRIBE
            success = firebase.update_subscription_status(email, preference, False)
            message = f"You have been unsubscribed from the {preference} newsletter."
        else:
            # GLOBAL UNSUBSCRIBE
            success = firebase.update_subscriber_status(email, subscribed=False)
            message = "You have been globally unsubscribed from all SegmentoPulse newsletters."
        
        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to update subscription preference"
            )
        
        # Send confirmation email (Global or Specific)
        # Note: We might want slightly different emails for specific vs global, 
        # but for now reusing the standard one is fine or we can customize.
        if not preference:
             brevo.send_unsubscribe_confirmation(email, name)
        
        return UnsubscribeResponse(
            success=True,
            message=message,
            email=email
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in unsubscribe endpoint: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Unsubscribe failed: {str(e)}"
        )


@router.post("/unsubscribe", response_model=UnsubscribeResponse)
async def unsubscribe_post(email: EmailStr):
    """
    Unsubscribe via email address (for forms)
    """
    try:
        firebase = get_firebase_service()
        brevo = get_brevo_service()
        
        # Get subscriber
        subscriber = firebase.get_subscriber(email)
        
        if not subscriber:
            raise HTTPException(
                status_code=404,
                detail="Email not found in subscriber list"
            )
        
        name = subscriber.get('name', 'Subscriber')
        
        # Update status
        success = firebase.update_subscriber_status(email, subscribed=False)
        
        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to unsubscribe"
            )
        
        # Send confirmation
        brevo.send_unsubscribe_confirmation(email, name)
        
        return UnsubscribeResponse(
            success=True,
            message="Successfully unsubscribed",
            email=email
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in unsubscribe_post endpoint: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Unsubscribe failed: {str(e)}"
        )


@router.get("/subscribers/count")
async def get_subscriber_count():
    """Get total number of active subscribers"""
    try:
        firebase = get_firebase_service()
        subscribers = firebase.get_all_subscribers()
        
        active_count = sum(1 for s in subscribers if s.get('subscribed', True))
        
        return {
            "total": len(subscribers),
            "active": active_count,
            "unsubscribed": len(subscribers) - active_count
        }
        
    except Exception as e:
        print(f"Error getting subscriber count: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to get subscriber count"
        )


@router.post("/newsletter/send")
async def send_newsletter(
    subject: str,
    category: str = "ai"
):
    """
    Send newsletter to all subscribers
    
    - Fetches latest news from specified category
    - Sends to all active subscribers
    - Returns send statistics
    """
    try:
        firebase = get_firebase_service()
        brevo = get_brevo_service()
        
        # Get all active subscribers
        subscribers = firebase.get_all_subscribers()
        active_subscribers = [s for s in subscribers if s.get('subscribed', True)]
        
        if not active_subscribers:
            return {
                "success": False,
                "message": "No active subscribers found",
                "sent": 0,
                "failed": 0
            }
        
        # Fetch latest news articles (you can customize this)
        from app.services.news_aggregator import news_aggregator
        articles = await news_aggregator.fetch_by_category(category)
        
        # Send newsletter
        result = brevo.send_newsletter(
            subject=subject,
            articles=articles,
            subscribers=active_subscribers
        )
        
        return {
            "success": True,
            "message": f"Newsletter sent to {result['sent']} subscribers",
            **result
        }
        
    except Exception as e:
        print(f"Error sending newsletter: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to send newsletter: {str(e)}"
        )
