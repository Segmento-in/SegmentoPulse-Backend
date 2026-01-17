from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from app.config import settings
from app.routes import news, search, analytics, subscription, admin

# Import scheduler functions
from app.services.scheduler import start_scheduler, shutdown_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager
    
    Handles startup and shutdown events for background tasks:
    - Startup: Initialize and start APScheduler
    - Shutdown: Gracefully stop all background jobs
    """
    # Startup: Start background scheduler
    print("=" * 60)
    print("🚀 Starting Segmento Pulse Backend...")
    start_scheduler()
    print("=" * 60)
    
    yield  # Application runs here
    
    # Shutdown: Stop background scheduler
    print("=" * 60)
    print("👋 Shutting down Segmento Pulse Backend...")
    shutdown_scheduler()
    print("=" * 60)


app = FastAPI(
    title="Segmento Pulse API",
    description="Real-Time Technology Intelligence Platform API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan  # Phase 3: Background scheduler lifecycle
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(news.router, prefix="/api/news", tags=["News"])
app.include_router(search.router, prefix="/api/search", tags=["Search"])
app.include_router(analytics.router, prefix="/api/analytics", tags=["Analytics"])
app.include_router(subscription.router, tags=["Subscription"])
app.include_router(admin.router, prefix="/api/admin", tags=["Admin"])

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Segmento Pulse API",
        "version": "1.0.0",
        "status": "operational",
        "docs": "/docs",
        "features": {
            "phase1": "Redis caching (600s TTL)",
            "phase2": "Appwrite database (L2 cache)",
            "phase3": "Background workers (auto-fetch + cleanup)"
        }
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.ENVIRONMENT == "development"
    )
