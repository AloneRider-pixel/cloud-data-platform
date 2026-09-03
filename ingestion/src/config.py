"""
Centralized configuration for the ingestion layer.
Loads settings from environment variables with sensible defaults.
"""
import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Config:
    """Application configuration loaded from environment."""

    # Application
    app_name: str = os.getenv("APP_NAME", "cloud-data-platform")
    app_env: str = os.getenv("APP_ENV", "development")
    debug: bool = os.getenv("DEBUG", "true").lower() == "true"

    # PostgreSQL
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "data_platform")
    postgres_user: str = os.getenv("POSTGRES_USER", "data_engineer")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "data_password")

    @property
    def database_url(self) -> str:
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    # AWS / S3 (MinIO compatible)
    aws_access_key: str = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    aws_secret_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")
    s3_bucket: str = os.getenv("S3_BUCKET", "raw-data-lake")
    s3_endpoint: Optional[str] = os.getenv("S3_ENDPOINT")

    # Mock API
    mock_api_base_url: str = os.getenv("MOCK_API_BASE_URL", "http://localhost:8001")
    mock_api_key: str = os.getenv("MOCK_API_KEY", "mock-api-key-123")

    # Pipeline Settings
    batch_size: int = int(os.getenv("BATCH_SIZE", "10000"))
    max_retries: int = int(os.getenv("MAX_RETRY_COUNT", "3"))
    retry_delay: int = int(os.getenv("RETRY_DELAY_SECONDS", "60"))
    data_freshness_sla_hours: int = int(os.getenv("DATA_FRESHNESS_SLA_HOURS", "1"))

    # Data Paths
    raw_prefix: str = "raw/"
    clean_prefix: str = "clean/"
    analytics_prefix: str = "analytics/"


config = Config()
