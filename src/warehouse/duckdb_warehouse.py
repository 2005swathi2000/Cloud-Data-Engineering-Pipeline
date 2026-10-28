"""
DuckDB Analytical Data Warehouse Manager.
Provides local columnar OLAP storage, schema creation, idempotent upserts,
quarantine table management, and SQL analytical querying.
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional
import duckdb
import pandas as pd
from src.config import CONFIG

logger = logging.getLogger(__name__)

class DuckDBWarehouse:
    """Manages the local DuckDB analytical warehouse."""

    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = str(db_path or CONFIG.DUCKDB_PATH)
        self._init_schemas()

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Returns a connection to the persistent DuckDB file."""
        return duckdb.connect(self.db_path)

    def _init_schemas(self):
        """Initializes tables for analytical records, quarantined records, and audit logs."""
        conn = self.get_connection()
        try:
            # 1. Main analytical table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS retail_transactions (
                    transaction_id VARCHAR PRIMARY KEY,
                    customer_id VARCHAR NOT NULL,
                    customer_masked_email VARCHAR,
                    transaction_timestamp TIMESTAMP NOT NULL,
                    transaction_date DATE NOT NULL,
                    transaction_year INTEGER NOT NULL,
                    transaction_month INTEGER NOT NULL,
                    transaction_day INTEGER NOT NULL,
                    transaction_hour INTEGER NOT NULL,
                    day_of_week VARCHAR,
                    store_id VARCHAR NOT NULL,
                    store_region VARCHAR NOT NULL,
                    product_id VARCHAR NOT NULL,
                    product_name VARCHAR NOT NULL,
                    product_category VARCHAR NOT NULL,
                    unit_price DOUBLE NOT NULL,
                    quantity INTEGER NOT NULL,
                    discount_amount DOUBLE NOT NULL,
                    gross_amount DOUBLE NOT NULL,
                    net_amount DOUBLE NOT NULL,
                    tax_amount DOUBLE NOT NULL,
                    total_amount DOUBLE NOT NULL,
                    payment_method VARCHAR NOT NULL,
                    pipeline_run_id VARCHAR NOT NULL,
                    ingested_at TIMESTAMP NOT NULL
                );
            """)

            # 2. Quarantined rejected records table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS rejected_transactions (
                    rejection_id VARCHAR DEFAULT (uuid()::VARCHAR),
                    transaction_id VARCHAR,
                    customer_id VARCHAR,
                    rejection_reason VARCHAR NOT NULL,
                    rejected_at TIMESTAMP NOT NULL,
                    raw_payload VARCHAR,
                    pipeline_run_id VARCHAR NOT NULL
                );
            """)

            # 3. Pipeline audit run history table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_audit_runs (
                    run_id VARCHAR PRIMARY KEY,
                    execution_timestamp TIMESTAMP NOT NULL,
                    execution_mode VARCHAR NOT NULL,
                    source_name VARCHAR NOT NULL,
                    total_records INTEGER NOT NULL,
                    accepted_records INTEGER NOT NULL,
                    rejected_records INTEGER NOT NULL,
                    duplicate_records INTEGER NOT NULL,
                    data_quality_score DOUBLE NOT NULL,
                    duration_seconds DOUBLE NOT NULL,
                    status VARCHAR NOT NULL
                );
            """)
            logger.info("DuckDB warehouse schemas verified.")
        finally:
            conn.close()

    def load_clean_transactions(self, df: pd.DataFrame) -> int:
        """
        Loads clean transactions into DuckDB with idempotent deduplication.
        If a transaction_id already exists, updates it with the latest run data.
        """
        if df.empty:
            return 0

        conn = self.get_connection()
        try:
            # Register DataFrame as temporary view
            conn.register("temp_clean_incoming", df)

            # Idempotent upsert via DELETE + INSERT
            conn.execute("""
                DELETE FROM retail_transactions
                WHERE transaction_id IN (SELECT transaction_id FROM temp_clean_incoming);
            """)

            conn.execute("""
                INSERT INTO retail_transactions
                SELECT 
                    transaction_id, customer_id, customer_masked_email,
                    transaction_timestamp::TIMESTAMP, transaction_date::DATE,
                    transaction_year, transaction_month, transaction_day, transaction_hour, day_of_week,
                    store_id, store_region, product_id, product_name, product_category,
                    unit_price, quantity, discount_amount, gross_amount, net_amount,
                    tax_amount, total_amount, payment_method, pipeline_run_id, ingested_at::TIMESTAMP
                FROM temp_clean_incoming;
            """)
            loaded_count = len(df)
            logger.info(f"Loaded {loaded_count} records into DuckDB retail_transactions.")
            return loaded_count
        finally:
            conn.close()

    def load_rejected_transactions(self, rejected_df: pd.DataFrame, run_id: str) -> int:
        """Loads quarantined records with failure reasons into rejected_transactions."""
        if rejected_df.empty:
            return 0

        conn = self.get_connection()
        try:
            rej = rejected_df.copy()
            rej["pipeline_run_id"] = run_id
            rej["raw_payload"] = rej.apply(lambda r: str(r.to_dict()), axis=1)

            # Ensure minimal columns present
            if "transaction_id" not in rej.columns:
                rej["transaction_id"] = "UNKNOWN"
            if "customer_id" not in rej.columns:
                rej["customer_id"] = "UNKNOWN"

            conn.register("temp_rejected_incoming", rej)
            conn.execute("""
                INSERT INTO rejected_transactions (
                    transaction_id, customer_id, rejection_reason, rejected_at, raw_payload, pipeline_run_id
                )
                SELECT 
                    transaction_id, customer_id, rejection_reason, rejected_at::TIMESTAMP, raw_payload, pipeline_run_id
                FROM temp_rejected_incoming;
            """)
            return len(rejected_df)
        finally:
            conn.close()

    def record_audit_run(self, audit_info: Dict[str, Any]):
        """Records pipeline run metrics in audit log."""
        conn = self.get_connection()
        try:
            conn.execute("""
                INSERT OR REPLACE INTO pipeline_audit_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (
                audit_info["run_id"],
                audit_info["execution_timestamp"],
                audit_info.get("execution_mode", "LOCAL"),
                audit_info.get("source_name", "unknown"),
                audit_info.get("total_records", 0),
                audit_info.get("accepted_records", 0),
                audit_info.get("rejected_records", 0),
                audit_info.get("duplicate_records", 0),
                audit_info.get("data_quality_score", 0.0),
                audit_info.get("duration_seconds", 0.0),
                audit_info.get("status", "SUCCESS")
            ))
        finally:
            conn.close()

    def execute_query(self, query: str) -> pd.DataFrame:
        """Executes arbitrary SQL query and returns results as Pandas DataFrame."""
        conn = self.get_connection()
        try:
            return conn.execute(query).fetchdf()
        finally:
            conn.close()

