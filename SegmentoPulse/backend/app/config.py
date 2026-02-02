from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator
from typing import List, Union, Optional

class Settings(BaseSettings):
    """Application settings"""
    
    # Environment
    ENVIRONMENT: str = "development"
    
    # Server
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    
    # CORS
    CORS_ORIGINS: List[str] = ["https://segmento.in"]
    
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
    
    # Cache
    CACHE_TTL: int = 600  # seconds (10 minutes) - Phase 1 optimization
    
    # Brevo Email Configuration
    BREVO_API_KEY: str = ""
    BREVO_SENDER_EMAIL: str = "info@segmento.in"
    BREVO_SENDER_NAME: str = "SegmentoPulse"
    
    # Frontend URL (for unsubscribe links)
    FRONTEND_URL: str = "https://segmento.in"
    
    # Appwrite Database
    APPWRITE_ENDPOINT: str = "https://nyc.cloud.appwrite.io/v1"
    APPWRITE_PROJECT_ID: str = ""
    APPWRITE_API_KEY: str = ""
    APPWRITE_DATABASE_ID: str = "segmento_db"
    APPWRITE_COLLECTION_ID: str = "articles"  # Regular articles
    APPWRITE_CLOUD_COLLECTION_ID: str = ""  # Phase 3: Cloud news (to be created)
    
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
        case_sensitive=True
    )

settings = Settings()
