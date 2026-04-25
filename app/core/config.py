"""
Application configuration settings
"""
from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings with environment variable support"""
    
    # App Settings
    app_name: str = "Retail Customer Segmentation API"
    app_version: str = "1.0.0"
    debug: bool = False
    
    # Server Settings
    host: str = "0.0.0.0"
    port: int = 8000
    
    # Database Settings
    database_url: str = "sqlite:///./segmentation.db"
    
    # Data Paths
    raw_data_path: str = "data/raw"
    processed_data_path: str = "data/processed"
    
    # API Settings
    api_v1_prefix: str = "/api/v1"
    
    # Clustering Settings
    n_clusters: int = 4
    random_state: int = 42
    
    # Features for Segmentation
    features_for_clustering: list = ["Recency", "Frequency", "Monetary"]
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()
