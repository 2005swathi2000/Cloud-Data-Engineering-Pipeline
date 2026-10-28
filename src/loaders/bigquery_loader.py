"""
Google Cloud BigQuery Loading Adapter.
Implements partitioned and clustered warehouse schema creation,
staging-table MERGE idempotency with automatic 1-hour expiration,
and quarantine routing for rejected transactions.
Compatible with Google Cloud Free Tier & BigQuery Sandbox.
"""

import logging
import uuid
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Tuple
import pandas as pd

from src.config import CONFIG

logger = logging.getLogger(__name__)

class BigQueryLoader:
    """Manages idempotent batch loading and quarantine storage in Google BigQuery."""

    @classmethod
    def get_client(cls):
        """Initializes BigQuery client with explicit project and location."""
        from google.cloud import bigquery
        project = CONFIG.GCP_PROJECT
        if not project:
            raise ValueError(
                "GCP_PROJECT is not set. Please define GCP_PROJECT in your .env or environment."
            )
        return bigquery.Client(project=project, location=CONFIG.BQ_LOCATION)

    @classmethod
    def get_bigquery_ddl(cls) -> str:
        """Returns standard BigQuery DDL for the analytical warehouse table."""
        project = CONFIG.GCP_PROJECT or "YOUR_PROJECT_ID"
        dataset = CONFIG.BQ_DATASET
        table = CONFIG.BQ_TABLE
        return f"""
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.{table}`
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
    def get_quarantine_ddl(cls) -> str:
        """Returns BigQuery DDL for the quarantined rejected records table."""
        project = CONFIG.GCP_PROJECT or "YOUR_PROJECT_ID"
        dataset = CONFIG.BQ_DATASET
        table = CONFIG.BQ_REJECTED_TABLE
        return f"""
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.{table}`
(
    rejection_id STRING NOT NULL OPTIONS(description="Unique UUID for quarantined entry"),
    transaction_id STRING OPTIONS(description="Transaction ID if parseable"),
    customer_id STRING OPTIONS(description="Customer ID if parseable"),
    rejection_reason STRING NOT NULL OPTIONS(description="Pipe-separated machine-readable error codes"),
    rejected_at TIMESTAMP NOT NULL OPTIONS(description="Quarantine audit timestamp"),
    raw_payload STRING OPTIONS(description="Full serialized raw row payload"),
    pipeline_run_id STRING NOT NULL OPTIONS(description="Pipeline run identifier")
)
PARTITION BY DATE(rejected_at)
OPTIONS(
    description="Quarantine storage for malformed and invalid retail transactions",
    require_partition_filter=false
);
"""

    @classmethod
    def generate_merge_sql(cls, staging_table_id: str, target_table_id: str) -> str:
        """
        Generates production MERGE DML statement for idempotent loading.
        Matches on transaction_id: updates on match, inserts on mismatch.
        """
        return f"""
MERGE INTO `{target_table_id}` T
USING `{staging_table_id}` S
ON T.transaction_id = S.transaction_id
WHEN MATCHED THEN
    UPDATE SET
        customer_id = S.customer_id,
        customer_masked_email = S.customer_masked_email,
        transaction_timestamp = S.transaction_timestamp,
        transaction_date = S.transaction_date,
        transaction_year = S.transaction_year,
        transaction_month = S.transaction_month,
        transaction_day = S.transaction_day,
        transaction_hour = S.transaction_hour,
        day_of_week = S.day_of_week,
        store_id = S.store_id,
        store_region = S.store_region,
        product_id = S.product_id,
        product_name = S.product_name,
        product_category = S.product_category,
        unit_price = S.unit_price,
        quantity = S.quantity,
        discount_amount = S.discount_amount,
        gross_amount = S.gross_amount,
        net_amount = S.net_amount,
        tax_amount = S.tax_amount,
        total_amount = S.total_amount,
        payment_method = S.payment_method,
        pipeline_run_id = S.pipeline_run_id,
        ingested_at = S.ingested_at
WHEN NOT MATCHED THEN
    INSERT (
        transaction_id, customer_id, customer_masked_email,
        transaction_timestamp, transaction_date, transaction_year,
        transaction_month, transaction_day, transaction_hour, day_of_week,
        store_id, store_region, product_id, product_name, product_category,
        unit_price, quantity, discount_amount, gross_amount, net_amount,
        tax_amount, total_amount, payment_method, pipeline_run_id, ingested_at
    )
    VALUES (
        S.transaction_id, S.customer_id, S.customer_masked_email,
        S.transaction_timestamp, S.transaction_date, S.transaction_year,
        S.transaction_month, S.transaction_day, S.transaction_hour, S.day_of_week,
        S.store_id, S.store_region, S.product_id, S.product_name, S.product_category,
        S.unit_price, S.quantity, S.discount_amount, S.gross_amount, S.net_amount,
        S.tax_amount, S.total_amount, S.payment_method, S.pipeline_run_id, S.ingested_at
    );
"""

    @classmethod
    def generate_quarantine_merge_sql(cls, staging_rejected_id: str, quarantine_table_id: str) -> str:
        """
        Generates production MERGE DML statement for idempotent loading into quarantine table.
        Matches on rejection_id (stable composite hash): updates on match, inserts on mismatch.
        """
        return f"""
MERGE INTO `{quarantine_table_id}` T
USING `{staging_rejected_id}` S
ON T.rejection_id = S.rejection_id
WHEN MATCHED THEN
    UPDATE SET
        transaction_id = S.transaction_id,
        customer_id = S.customer_id,
        rejection_reason = S.rejection_reason,
        rejected_at = S.rejected_at,
        raw_payload = S.raw_payload,
        pipeline_run_id = S.pipeline_run_id
WHEN NOT MATCHED THEN
    INSERT (
        rejection_id, transaction_id, customer_id,
        rejection_reason, rejected_at, raw_payload, pipeline_run_id
    )
    VALUES (
        S.rejection_id, S.transaction_id, S.customer_id,
        S.rejection_reason, S.rejected_at, S.raw_payload, S.pipeline_run_id
    );
"""

    @staticmethod
    def compute_rejection_id(transaction_id: str, rejection_reason: str, raw_payload: Any) -> str:
        """Computes deterministic 24-character SHA-256 hash key for idempotent quarantine deduplication."""
        if isinstance(raw_payload, dict):
            clean_dict = {
                k: v for k, v in raw_payload.items()
                if k not in ("pipeline_run_id", "rejected_at", "rejection_id")
            }
            payload_str = str(sorted(clean_dict.items()))
        else:
            payload_str = str(raw_payload)
        raw_hash = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()[:12]
        key_src = f"{transaction_id}|{rejection_reason}|{raw_hash}"
        return hashlib.sha256(key_src.encode("utf-8")).hexdigest()[:24]

    @classmethod
    def ensure_dataset_and_tables(cls, client) -> None:
        """Creates dataset and target tables with explicit location if not already existing."""
        from google.cloud import bigquery
        
        # 1. Dataset
        dataset_ref = bigquery.DatasetReference(CONFIG.GCP_PROJECT, CONFIG.BQ_DATASET)
        try:
            dataset = client.get_dataset(dataset_ref)
            logger.info(f"BigQuery dataset `{CONFIG.BQ_DATASET}` located in `{dataset.location}`.")
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = CONFIG.BQ_LOCATION
            dataset = client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Created BigQuery dataset `{CONFIG.BQ_DATASET}` in location `{CONFIG.BQ_LOCATION}`.")

        # 2. Main analytical table DDL
        client.query(cls.get_bigquery_ddl(), location=CONFIG.BQ_LOCATION).result()

        # 3. Quarantine table DDL
        client.query(cls.get_quarantine_ddl(), location=CONFIG.BQ_LOCATION).result()

    @classmethod
    def execute_dry_run(cls, clean_df: pd.DataFrame, rejected_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Executes a dry-run check.
        Prints the exact MERGE SQL, target tables, explicit location, and planned row counts
        without modifying anything in BigQuery.
        """
        dummy_run_id = f"DRYRUN_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        staging_id = f"{CONFIG.GCP_PROJECT or 'YOUR_PROJECT'}.{CONFIG.BQ_DATASET}.staging_retail_{dummy_run_id}"
        staging_rej_id = f"{CONFIG.GCP_PROJECT or 'YOUR_PROJECT'}.{CONFIG.BQ_DATASET}.staging_rejected_{dummy_run_id}"
        target_id = f"{CONFIG.GCP_PROJECT or 'YOUR_PROJECT'}.{CONFIG.BQ_DATASET}.{CONFIG.BQ_TABLE}"
        quarantine_id = f"{CONFIG.GCP_PROJECT or 'YOUR_PROJECT'}.{CONFIG.BQ_DATASET}.{CONFIG.BQ_REJECTED_TABLE}"
        
        merge_sql = cls.generate_merge_sql(staging_table_id=staging_id, target_table_id=target_id)
        quarantine_merge_sql = cls.generate_quarantine_merge_sql(staging_rejected_id=staging_rej_id, quarantine_table_id=quarantine_id)
        
        print("\n" + "=" * 80)
        print("BIGQUERY SINK -- DRY-RUN INSPECTION REPORT")
        print("=" * 80)
        print(f"Target Mode:               BIGQUERY (Sandbox/Free Tier Compatible)")
        print(f"GCP Project:               {CONFIG.GCP_PROJECT or '<UNCONFIGURED - Needs GCP_PROJECT in .env>'}")
        print(f"Dataset Location:          {CONFIG.BQ_LOCATION}")
        print(f"Target Analytical Table:   {target_id}")
        print(f"Quarantine Storage Table:  {quarantine_id}")
        print(f"Retail Staging Template:   {staging_id} (Expires after 1 hour)")
        print(f"Reject Staging Template:   {staging_rej_id} (Expires after 1 hour)")
        print(f"Clean Records to Load:     {len(clean_df):,}")
        print(f"Rejected Rows to Isolate:  {len(rejected_df):,}")
        print("-" * 80)
        print("IDEMPOTENT RETAIL TRANSACTIONS MERGE DML:")
        print(merge_sql.strip())
        print("-" * 80)
        print("IDEMPOTENT QUARANTINE MERGE DML (STABLE REJECTION_ID HASH):")
        print(quarantine_merge_sql.strip())
        print("=" * 80 + "\n")

        return {
            "mode": "DRY_RUN",
            "status": "READY",
            "gcp_project": CONFIG.GCP_PROJECT,
            "dataset_location": CONFIG.BQ_LOCATION,
            "target_table": target_id,
            "quarantine_table": quarantine_id,
            "clean_records_count": len(clean_df),
            "rejected_records_count": len(rejected_df),
            "merge_sql": merge_sql,
            "quarantine_merge_sql": quarantine_merge_sql
        }

    @classmethod
    def load_batch_idempotent(
        cls,
        clean_df: pd.DataFrame,
        rejected_df: pd.DataFrame,
        run_id: str,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Loads clean records to a temporary staging table, performs a MERGE into the target table,
        drops the staging table in a finally block, and inserts rejected records into the quarantine table.
        """
        if dry_run:
            return cls.execute_dry_run(clean_df, rejected_df)

        if not CONFIG.is_gcp_configured():
            raise RuntimeError(
                f"BigQuery credentials or project not configured. "
                f"GCP_PROJECT='{CONFIG.GCP_PROJECT}'. Set GCP_PROJECT and credentials in .env."
            )

        from google.cloud import bigquery
        from google.api_core.exceptions import GoogleAPICallError

        client = cls.get_client()
        cls.ensure_dataset_and_tables(client)

        target_table_id = f"{CONFIG.GCP_PROJECT}.{CONFIG.BQ_DATASET}.{CONFIG.BQ_TABLE}"
        quarantine_table_id = f"{CONFIG.GCP_PROJECT}.{CONFIG.BQ_DATASET}.{CONFIG.BQ_REJECTED_TABLE}"
        
        # Clean run_id for staging table names (alphanumeric and underscores only)
        clean_run_token = "".join(c if c.isalnum() else "_" for c in run_id)
        staging_table_id = f"{CONFIG.GCP_PROJECT}.{CONFIG.BQ_DATASET}.staging_retail_{clean_run_token}_{uuid.uuid4().hex[:6]}"
        staging_rejected_id = f"{CONFIG.GCP_PROJECT}.{CONFIG.BQ_DATASET}.staging_rejected_{clean_run_token}_{uuid.uuid4().hex[:6]}"

        staging_dropped = False
        staging_rejected_dropped = False
        merge_executed = False

        # 1. Staging table configuration with explicit location and 1-hour expiration
        try:
            if not clean_df.empty:
                logger.info(f"Creating temporary staging table: {staging_table_id}")
                staging_table = bigquery.Table(staging_table_id)
                staging_table.expires = datetime.now(timezone.utc) + timedelta(hours=1)
                staging_table.description = "Temporary staging table for idempotent batch MERGE load"
                client.create_table(staging_table)

                # Format DataFrame datatypes for BigQuery
                df_to_load = clean_df.copy()
                df_to_load["transaction_timestamp"] = pd.to_datetime(df_to_load["transaction_timestamp"])
                df_to_load["transaction_date"] = pd.to_datetime(df_to_load["transaction_date"]).dt.date
                df_to_load["ingested_at"] = pd.to_datetime(df_to_load["ingested_at"])

                # Load clean records into staging table
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                )
                load_job = client.load_table_from_dataframe(df_to_load, staging_table_id, job_config=job_config)
                load_job.result()  # Wait for load job to finish
                logger.info(f"Loaded {len(df_to_load)} records into staging table `{staging_table_id}`.")

                # Execute Idempotent MERGE
                merge_sql = cls.generate_merge_sql(staging_table_id=staging_table_id, target_table_id=target_table_id)
                logger.info(f"Executing BigQuery MERGE statement on `{target_table_id}`...")
                query_job = client.query(merge_sql, location=CONFIG.BQ_LOCATION)
                query_job.result()
                merge_executed = True
                logger.info("MERGE DML statement completed successfully.")

            # 2. Idempotent quarantine load using stable composite rejection_id and MERGE
            if not rejected_df.empty:
                logger.info(f"Creating temporary quarantine staging table: {staging_rejected_id}")
                staging_rejected_table = bigquery.Table(staging_rejected_id)
                staging_rejected_table.expires = datetime.now(timezone.utc) + timedelta(hours=1)
                staging_rejected_table.description = "Temporary staging table for idempotent quarantine MERGE"
                client.create_table(staging_rejected_table)

                rej_records = []
                for _, row in rejected_df.iterrows():
                    txn_id = str(row.get("transaction_id", "UNKNOWN"))
                    reason = str(row.get("rejection_reason", "ERR_UNKNOWN"))
                    payload_str = str(row.to_dict())
                    stable_rejection_id = cls.compute_rejection_id(txn_id, reason, payload_str)

                    rej_records.append({
                        "rejection_id": stable_rejection_id,
                        "transaction_id": txn_id,
                        "customer_id": str(row.get("customer_id", "UNKNOWN")),
                        "rejection_reason": reason,
                        "rejected_at": datetime.now(timezone.utc).isoformat(),
                        "raw_payload": payload_str,
                        "pipeline_run_id": run_id
                    })
                rej_df_formatted = pd.DataFrame(rej_records)
                rej_df_formatted["rejected_at"] = pd.to_datetime(rej_df_formatted["rejected_at"])

                rej_job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                )
                rej_job = client.load_table_from_dataframe(rej_df_formatted, staging_rejected_id, job_config=rej_job_config)
                rej_job.result()
                logger.info(f"Loaded {len(rej_df_formatted)} records into quarantine staging table `{staging_rejected_id}`.")

                # Execute Idempotent Quarantine MERGE
                quarantine_merge_sql = cls.generate_quarantine_merge_sql(
                    staging_rejected_id=staging_rejected_id,
                    quarantine_table_id=quarantine_table_id
                )
                logger.info(f"Executing BigQuery Quarantine MERGE statement on `{quarantine_table_id}`...")
                q_query_job = client.query(quarantine_merge_sql, location=CONFIG.BQ_LOCATION)
                q_query_job.result()
                logger.info("Quarantine MERGE DML statement completed successfully.")

        except GoogleAPICallError as api_err:
            err_msg = (
                f"BigQuery Execution Failed! "
                f"Code: {api_err.code}, Message: {api_err.message}. "
                f"Check for BigQuery Sandbox DML restrictions or billing quota."
            )
            logger.error(err_msg, exc_info=True)
            raise RuntimeError(err_msg) from api_err

        except Exception as e:
            logger.error(f"BigQuery loader error: {str(e)}", exc_info=True)
            raise

        finally:
            # Guaranteed staging cleanup in finally block
            try:
                client.delete_table(staging_table_id, not_found_ok=True)
                staging_dropped = True
                logger.info(f"Cleaned up staging table: `{staging_table_id}`.")
            except Exception as drop_err:
                logger.warning(f"Could not drop staging table `{staging_table_id}`: {drop_err}")

            try:
                client.delete_table(staging_rejected_id, not_found_ok=True)
                staging_rejected_dropped = True
                logger.info(f"Cleaned up quarantine staging table: `{staging_rejected_id}`.")
            except Exception as drop_err:
                logger.warning(f"Could not drop quarantine staging table `{staging_rejected_id}`: {drop_err}")

        # Fetch actual live row counts from BigQuery
        counts = cls.get_table_counts(client)

        return {
            "status": "SUCCESS",
            "mode": "GCP_BIGQUERY_LIVE",
            "target_table": target_table_id,
            "quarantine_table": quarantine_table_id,
            "target_table_row_count": counts["target_rows"],
            "quarantine_table_row_count": counts["quarantine_rows"],
            "staging_dropped_cleanly": staging_dropped and staging_rejected_dropped,
            "merge_executed": merge_executed
        }

    @classmethod
    def get_table_counts(cls, client) -> Dict[str, int]:
        """Queries actual current row counts in BigQuery target and quarantine tables."""
        target_id = f"{CONFIG.GCP_PROJECT}.{CONFIG.BQ_DATASET}.{CONFIG.BQ_TABLE}"
        quarantine_id = f"{CONFIG.GCP_PROJECT}.{CONFIG.BQ_DATASET}.{CONFIG.BQ_REJECTED_TABLE}"
        
        target_rows = 0
        quarantine_rows = 0

        try:
            res_target = client.query(f"SELECT COUNT(*) AS cnt FROM `{target_id}`;", location=CONFIG.BQ_LOCATION).result()
            for r in res_target:
                target_rows = int(r["cnt"])
        except Exception as e:
            logger.warning(f"Could not read target row count from `{target_id}`: {e}")

        try:
            res_q = client.query(f"SELECT COUNT(*) AS cnt FROM `{quarantine_id}`;", location=CONFIG.BQ_LOCATION).result()
            for r in res_q:
                quarantine_rows = int(r["cnt"])
        except Exception as e:
            logger.warning(f"Could not read quarantine row count from `{quarantine_id}`: {e}")

        return {"target_rows": target_rows, "quarantine_rows": quarantine_rows}
