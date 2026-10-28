"""
Configuration module for Cloud Data Engineering Pipeline.
Supports local execution (DuckDB, local filesystem) and Google Cloud Platform BigQuery execution
using Application Default Credentials (gcloud auth application-default login).
"""

import os
from pathlib import Path
from dataclasses import dataclass
from dotenv import load_dotenv

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Load .env file automatically from project root if present
load_dotenv(PROJECT_ROOT / ".env")

@dataclass(frozen=True)
class PipelineConfig:
    # Execution mode and target: 'duckdb' or 'bigquery'
    DEFAULT_TARGET: str = os.getenv("PIPELINE_TARGET", "duckdb").lower()

    # Local Storage and Warehouse Paths
    SAMPLE_DATA_CSV: Path = PROJECT_ROOT / "sample_data" / "retail_transactions.csv"
    SAMPLE_DATA_JSON: Path = PROJECT_ROOT / "sample_data" / "retail_transactions.json"
    DUCKDB_PATH: Path = PROJECT_ROOT / "warehouse" / "retail_analytics.duckdb"
    REJECTED_DATA_PATH: Path = PROJECT_ROOT / "warehouse" / "rejected_records.csv"
    CLEAN_PARQUET_PATH: Path = PROJECT_ROOT / "warehouse" / "clean_transactions.parquet"
    LOGS_DIR: Path = PROJECT_ROOT / "logs"

    # GCP Settings (Primary env vars with backward-compatible fallbacks)
    # Uses Application Default Credentials (ADC) via: gcloud auth application-default login
    GCP_PROJECT: str = os.getenv("GCP_PROJECT") or os.getenv("GCP_PROJECT_ID") or ""
    BQ_DATASET: str = os.getenv("BQ_DATASET") or os.getenv("BIGQUERY_DATASET") or "retail_analytics"
    BQ_TABLE: str = os.getenv("BQ_TABLE") or os.getenv("BIGQUERY_TABLE") or "retail_transactions"
    BQ_REJECTED_TABLE: str = os.getenv("BQ_REJECTED_TABLE") or os.getenv("BIGQUERY_REJECTED_TABLE") or "rejected_transactions"
    BQ_LOCATION: str = os.getenv("BQ_LOCATION") or os.getenv("GCP_REGION") or "US"
    GCS_BUCKET: str = os.getenv("GCS_BUCKET") or os.getenv("GCS_BUCKET_NAME") or ""
    GCS_INPUT_PREFIX: str = os.getenv("GCS_INPUT_PREFIX", "raw_transactions/")

    # Business Logic Constants
    DEFAULT_TAX_RATE: float = 0.08  # 8% standard retail tax
    SUPPORTED_REGIONS: tuple = ("North", "South", "East", "West", "Central", "Online")
    SUPPORTED_CATEGORIES: tuple = (
        "Electronics", "Apparel", "Home & Kitchen", "Books", 
        "Health & Beauty", "Sports & Outdoors", "Groceries"
    )

    def is_gcp_configured(self) -> bool:
        """Checks if GCP project is set and Application Default Credentials (ADC) are valid."""
        if not self.GCP_PROJECT:
            return False
        try:
            import google.auth
            credentials, _ = google.auth.default()
            return credentials is not None
        except Exception:
            return False

# Global default config instance
CONFIG = PipelineConfig()

# Ensure local runtime folders exist
os.makedirs(CONFIG.DUCKDB_PATH.parent, exist_ok=True)
os.makedirs(CONFIG.LOGS_DIR, exist_ok=True)
