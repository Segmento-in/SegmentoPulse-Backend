from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator
from typing import List, Union, Optional

class Settings(BaseSettings):
    """Application settings"""
    
    # Environment
    ENVIRONMENT: str = "development"
    
    # Logging
    LOG_LEVEL: str = "INFO"  # INFO for development, WARNING for production
    
    # Server
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    
    # CORS - Supports both production and local development
    CORS_ORIGINS: List[str] = [
        "https://segmento.in",      # Production frontend
        "http://localhost:3000",    # Local dev frontend
        "http://127.0.0.1:3000"     # Alternative local dev
    ]
    
    # News API
    NEWS_API_KEY: str = ""
    
    # Multi-Provider News APIs
    GNEWS_API_KEY: str = ""
    NEWSAPI_API_KEY: str = ""
    NEWSDATA_API_KEY: str = ""
    
    # Provider priority (will try in order until successful)
    NEWS_PROVIDER_PRIORITY: List[str] = ["gnews", "newsapi", "newsdata", "google_rss"]
    
    # Firebase
    FIREBASE_DATABASE_URL: str = ""
    FIREBASE_PROJECT_ID: str = ""
    FIREBASE_CREDENTIALS_PATH: str = "./firebase-credentials.json"
    FIREBASE_CREDENTIALS: Optional[str] = None  # Support for raw JSON content (e.g. HF Spaces)
    
    # Redis
    REDIS_URL: str = "redis://localhost:6379"
    REDIS_PASSWORD: str = ""
    
    # Redis Control (Hotfix: Soft-disable when Redis not available)
    ENABLE_REDIS: bool = False  # Set to True when Redis server is running
    
    # Upstash Redis (REST API) - Free Tier Optimized
    # Prefer env vars for production, fallback to defaults for development
    UPSTASH_REDIS_REST_URL: str = ""  # Set in production secrets
    UPSTASH_REDIS_REST_TOKEN: str = ""  # Set in production secrets
    ENABLE_UPSTASH_CACHE: bool = True  # Use Upstash instead of local Redis
    
    # Cache
    CACHE_TTL: int = 600  # seconds (10 minutes) - Phase 1 optimization
    
    # Brevo Email Configuration
    BREVO_API_KEY: str = ""
    BREVO_SENDER_EMAIL: str = "info@segmento.in"
    BREVO_SENDER_NAME: str = "SegmentoPulse"
    
    # Frontend URL (for unsubscribe links)
    FRONTEND_URL: str = "https://segmento.in"
    
    # AI Services
    GROQ_API_KEY: str = ""
    
    # Appwrite Database
    APPWRITE_ENDPOINT: str = "https://nyc.cloud.appwrite.io/v1"
    APPWRITE_PROJECT_ID: str = "6968b8e300371c58c21a"
    APPWRITE_API_KEY: str = "standard_ea4de288498a3c1dba1bd02dcc3a58e86abd68f5f10cbf1e4f5365f5e184b55dbbb54ba82f9a6476a5b415566b774ad4d50cf32ac7336e9660698a40929113b576c7dead7d845e9f8c9d6b871ddb9b05223bc347f5abde15573742a3e0b4064fbf653e1c1feda2d027bd5c08d4d49068e3d781dafddd2ae010d9eaed395e60d0"
    APPWRITE_DATABASE_ID: str = "segmento_db"
    APPWRITE_COLLECTION_ID: str = "articles"  # Regular articles
    APPWRITE_SUBSCRIBERS_COLLECTION_ID: str = "subscribers"
    APPWRITE_AUDIO_BUCKET_ID: str = "audio-summaries"
    # New Collection IDs
    APPWRITE_AI_COLLECTION_ID: str ="6985d84600311fce57c2"
    APPWRITE_DATA_COLLECTION_ID: str ="69845bcf00095c406439"
    APPWRITE_CLOUD_COLLECTION_ID: str ="cloud_articles"
    APPWRITE_MAGAZINE_COLLECTION_ID: str = "6798e285002a24aa3d63"
    APPWRITE_MEDIUM_COLLECTION_ID: str = "679a0ec3001889753820"
    APPWRITE_RESEARCH_COLLECTION_ID: str = "research_papers_v2"
    # Admin Alerting (Optional - Discord/Slack webhook URL)
    ADMIN_WEBHOOK_URL: Optional[str] = None
    
    @field_validator('CORS_ORIGINS', 'NEWS_PROVIDER_PRIORITY', mode='before')
    @classmethod
    def parse_comma_separated(cls, v: Union[str, List[str]]) -> List[str]:
        """Parse comma-separated string into list (for HF Spaces secrets)"""
        if isinstance(v, str):
            return [item.strip() for item in v.split(',') if item.strip()]
        return v
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore"
    )

settings = Settings()
