"""
Configuration module for Cloud Data Engineering Pipeline.
Supports local execution (DuckDB, local filesystem) and optional Google Cloud Platform execution
(Google Cloud Storage, Cloud Dataflow, BigQuery).
"""

import os
from pathlib import Path
from dataclasses import dataclass

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent

@dataclass(frozen=True)
class PipelineConfig:
    # Execution mode: 'LOCAL' or 'GCP'
    EXECUTION_MODE: str = os.getenv("PIPELINE_EXECUTION_MODE", "LOCAL").upper()

    # Local Storage and Warehouse Paths
    SAMPLE_DATA_CSV: Path = PROJECT_ROOT / "sample_data" / "retail_transactions.csv"
    SAMPLE_DATA_JSON: Path = PROJECT_ROOT / "sample_data" / "retail_transactions.json"
    DUCKDB_PATH: Path = PROJECT_ROOT / "warehouse" / "retail_analytics.duckdb"
    REJECTED_DATA_PATH: Path = PROJECT_ROOT / "warehouse" / "rejected_records.csv"
    CLEAN_PARQUET_PATH: Path = PROJECT_ROOT / "warehouse" / "clean_transactions.parquet"
    LOGS_DIR: Path = PROJECT_ROOT / "logs"

    # GCP Specific Settings (Optional, populated via .env or Cloud Environment)
    GCP_PROJECT_ID: str = os.getenv("GCP_PROJECT_ID", "")
    GCS_BUCKET_NAME: str = os.getenv("GCS_BUCKET_NAME", "")
    GCS_INPUT_PREFIX: str = os.getenv("GCS_INPUT_PREFIX", "raw_transactions/")
    BIGQUERY_DATASET: str = os.getenv("BIGQUERY_DATASET", "retail_analytics")
    BIGQUERY_TABLE: str = os.getenv("BIGQUERY_TABLE", "retail_transactions")
    BIGQUERY_REJECTED_TABLE: str = os.getenv("BIGQUERY_REJECTED_TABLE", "rejected_transactions")
    GCP_REGION: str = os.getenv("GCP_REGION", "us-central1")
    GCP_CREDENTIALS_PATH: str = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

    # Business Logic Constants
    DEFAULT_TAX_RATE: float = 0.08  # 8% standard retail tax
    SUPPORTED_REGIONS: tuple = ("North", "South", "East", "West", "Central", "Online")
    SUPPORTED_CATEGORIES: tuple = (
        "Electronics", "Apparel", "Home & Kitchen", "Books", 
        "Health & Beauty", "Sports & Outdoors", "Groceries"
    )

    def is_gcp_configured(self) -> bool:
        """Checks if minimal GCP configuration and credentials are available."""
        return bool(
            self.GCP_PROJECT_ID and 
            (self.GCP_CREDENTIALS_PATH and os.path.exists(self.GCP_CREDENTIALS_PATH))
        )

# Global default config instance
CONFIG = PipelineConfig()

# Ensure local runtime folders exist
os.makedirs(CONFIG.DUCKDB_PATH.parent, exist_ok=True)
os.makedirs(CONFIG.LOGS_DIR, exist_ok=True)

