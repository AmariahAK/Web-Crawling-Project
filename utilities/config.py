"""Configuration management using Pydantic Settings."""
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # MongoDB Configuration
    mongodb_uri: str = "mongodb://localhost:27017"
    database_name: str = "books_crawler"
    
    # API Configuration
    api_keys: str = "dev_key_123"
    rate_limit_per_hour: int = 100
    
    # Crawler Configuration
    crawler_concurrency: int = 10
    crawler_max_retries: int = 3
    crawler_timeout_seconds: int = 30
    base_url: str = "https://books.toscrape.com"
    
    # Scheduler Configuration
    scheduler_interval_hours: int = 24
    enable_email_alerts: bool = False
    alert_email: str = ""
    
    # Logging Configuration
    log_level: str = "INFO"
    log_format: str = "json"
    
    # Application Settings
    environment: str = "development"
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False
    )
    
    @property
    def api_keys_list(self) -> List[str]:
        """Parse API keys from comma-separated string."""
        return [key.strip() for key in self.api_keys.split(",") if key.strip()]


# Global settings instance
settings = Settings()
