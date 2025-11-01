
"""
Configuration Management for Crypto ML Finance Pipeline
"""

import os
from pathlib import Path
from typing import Optional
from pydantic import BaseSettings, Field
from functools import lru_cache

class Settings(BaseSettings):
    """Application settings with environment variable support"""

    # Application
    app_name: str = "Crypto ML Finance Pipeline"
    version: str = "1.0.0"
    debug: bool = Field(False, env="DEBUG")

    # Database
    database_host: str = Field("localhost", env="DATABASE_HOST")
    database_port: int = Field(5432, env="DATABASE_PORT")
    database_name: str = Field("crypto_ml", env="DATABASE_NAME")
    database_user: Optional[str] = Field(None, env="DATABASE_USER")
    database_password: Optional[str] = Field(None, env="DATABASE_PASSWORD")

    # Binance API
    binance_api_key: Optional[str] = Field(None, env="BINANCE_API_KEY")
    binance_api_secret: Optional[str] = Field(None, env="BINANCE_API_SECRET")

    # Dask Configuration
    dask_workers: int = Field(10, env="DASK_WORKERS")
    dask_threads_per_worker: int = Field(1, env="DASK_THREADS_PER_WORKER")
    dask_memory_limit: str = Field("6.4GB", env="DASK_MEMORY_LIMIT")

    # Pipeline Configuration
    max_file_size_gb: float = Field(10.0, env="MAX_FILE_SIZE_GB")
    download_workers: int = Field(5, env="DOWNLOAD_WORKERS")

    # Paths
    output_path: Path = Field(Path("./output"), env="OUTPUT_PATH")
    datasets_path: Path = Field(Path("./data"), env="DATASETS_PATH")

    # Feature Engineering
    initial_volume_threshold: int = Field(1000000, env="INITIAL_VOLUME_THRESHOLD")
    alpha_volume: float = Field(0.1, env="ALPHA_VOLUME")
    alpha_imbalance: float = Field(0.7, env="ALPHA_IMBALANCE")

    # Logging
    log_level: str = Field("INFO", env="LOG_LEVEL")
    log_file: str = Field("pipeline.log", env="LOG_FILE")

    class Config:
        env_file = ".env"
        case_sensitive = False

    @property
    def database_url(self) -> str:
        """Construct database URL"""
        if self.database_user and self.database_password:
            return f"postgresql://{self.database_user}:{self.database_password}@{self.database_host}:{self.database_port}/{self.database_name}"
        return f"postgresql://{self.database_host}:{self.database_port}/{self.database_name}"

@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()

# Create settings instance
settings = get_settings()
