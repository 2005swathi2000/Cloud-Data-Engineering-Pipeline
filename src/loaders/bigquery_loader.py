"""
Optional Google BigQuery Loading Adapter.
Provides schema creation, partitioning by DATE(transaction_timestamp),
clustering by store_region and product_category, and batch loading.
Gracefully handles offline local execution without requiring GCP credentials.
"""

import logging
from typing import Dict, Any, Optional
import pandas as pd
from src.config import CONFIG

logger = logging.getLogger(__name__)

class BigQueryLoader:
    """Optional adapter for loading retail data into Google BigQuery."""

    @staticmethod
    def get_bigquery_ddl() -> str:
        """Returns standard BigQuery DDL with partitioning and clustering recommendations."""
        return f"""
-- Google Cloud BigQuery DDL for Retail Transactions Table
-- Optimized with DATE partitioning and regional/category clustering

CREATE TABLE IF NOT EXISTS `{CONFIG.GCP_PROJECT_ID or 'YOUR_PROJECT_ID'}.{CONFIG.BIGQUERY_DATASET}.{CONFIG.BIGQUERY_TABLE}`
(
    transaction_id STRING NOT NULL OPTIONS(description="Unique transaction primary key"),
    customer_id STRING NOT NULL OPTIONS(description="Customer identifier"),
    customer_masked_email STRING OPTIONS(description="PII pseudonymized customer email"),
    transaction_timestamp TIMESTAMP NOT NULL OPTIONS(description="Event timestamp"),
    transaction_date DATE NOT NULL OPTIONS(description="Derived transaction calendar date"),
    transaction_year INT64 NOT NULL,
    transaction_month INT64 NOT NULL,
    transaction_day INT64 NOT NULL,
    transaction_hour INT64 NOT NULL,
    day_of_week STRING,
    store_id STRING NOT NULL,
    store_region STRING NOT NULL,
    product_id STRING NOT NULL,
    product_name STRING NOT NULL,
    product_category STRING NOT NULL,
    unit_price NUMERIC NOT NULL,
    quantity INT64 NOT NULL,
    discount_amount NUMERIC NOT NULL,
    gross_amount NUMERIC NOT NULL,
    net_amount NUMERIC NOT NULL,
    tax_amount NUMERIC NOT NULL,
    total_amount NUMERIC NOT NULL,
    payment_method STRING NOT NULL,
    pipeline_run_id STRING NOT NULL,
    ingested_at TIMESTAMP NOT NULL
)
PARTITION BY transaction_date
CLUSTER BY store_region, product_category
OPTIONS(
    description="Partitioned and clustered retail transactions analytical warehouse table",
    require_partition_filter=false
);
"""

    @classmethod
    def load_dataframe(cls, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Loads DataFrame into BigQuery if credentials and project are configured.
        Otherwise returns clear offline simulation details without raising error.
        """
        if not CONFIG.is_gcp_configured():
            logger.info("GCP BigQuery not configured. Operating in local simulation mode.")
            return {
                "status": "OFFLINE_SIMULATION",
                "mode": "LOCAL_DUCKDB_ACTIVE",
                "message": (
                    "GCP project ID or Google credentials not detected. "
                    "Processed data is safely stored in local DuckDB warehouse. "
                    "BigQuery schema and DDL are ready for deployment."
                ),
                "ddl": cls.get_bigquery_ddl(),
                "records_pending": len(df)
            }

        try:
            from google.cloud import bigquery
            client = bigquery.Client(project=CONFIG.GCP_PROJECT_ID)
            table_ref = f"{CONFIG.GCP_PROJECT_ID}.{CONFIG.BIGQUERY_DATASET}.{CONFIG.BIGQUERY_TABLE}"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="transaction_date"
                ),
                clustering_fields=["store_region", "product_category"]
            )
            
            job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()  # Wait for load job to finish
            
            return {
                "status": "SUCCESS",
                "mode": "GCP_BIGQUERY_LIVE",
                "table": table_ref,
                "loaded_rows": len(df)
            }
        except ImportError:
            return {
                "status": "OFFLINE_SIMULATION",
                "mode": "LOCAL_DUCKDB_ACTIVE",
                "message": "google-cloud-bigquery package not installed. Running locally."
            }
        except Exception as e:
            logger.error(f"BigQuery load error: {e}", exc_info=True)
            return {
                "status": "ERROR",
                "mode": "GCP_BIGQUERY_FAILED",
                "error": str(e)
            }

