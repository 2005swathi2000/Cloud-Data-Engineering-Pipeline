"""
Batch Pipeline Orchestration Engine.
Coordinates ingestion, validation, quarantine, transformation, local DuckDB loading,
audit logging, and optional Google BigQuery export.
"""

import time
import uuid
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Union, Dict, Any, Optional
import pandas as pd

from src.config import CONFIG
from src.ingestion.reader import DataIngestionReader
from src.quality.validator import DataValidator
from src.quality.governance import GovernanceManager
from src.transformation.transformer import DataTransformer
from src.warehouse.duckdb_warehouse import DuckDBWarehouse
from src.loaders.bigquery_loader import BigQueryLoader

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

class RetailBatchPipeline:
    """Executes the complete retail sales data engineering workflow."""

    def __init__(self, warehouse: Optional[DuckDBWarehouse] = None):
        self.warehouse = warehouse or DuckDBWarehouse()

    def run(self, source: Optional[Union[str, Path, Any]] = None, file_type: str = "csv") -> Dict[str, Any]:
        """
        Executes end-to-end batch data pipeline run.

        Args:
            source: Path to file, buffer, or None (defaults to built-in sample data).
            file_type: 'csv' or 'json'.

        Returns:
            Dict containing complete execution report, data quality metrics, and table row counts.
        """
        start_time = time.time()
        run_id = f"RUN-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
        source_target = source or (CONFIG.SAMPLE_DATA_CSV if file_type == "csv" else CONFIG.SAMPLE_DATA_JSON)
        source_name = str(source_target)

        logger.info(f"Starting Retail Batch Pipeline. Run ID: {run_id}, Source: {source_name}")

        try:
            # 1. Ingestion
            raw_df = DataIngestionReader.read_source(source_target, file_type=file_type)
            total_received = len(raw_df)

            # 2. Validation & Quarantine
            valid_df, rejected_df, val_summary = DataValidator.validate_batch(raw_df)

            # 3. Governance & Data Quality Score
            dq_metrics = GovernanceManager.compute_data_quality_score(raw_df, valid_df, val_summary)

            # 4. Transformation & Normalization
            clean_df = DataTransformer.transform_all(valid_df, run_id=run_id)

            # 5. Local Analytical Warehouse Load (DuckDB)
            loaded_clean = self.warehouse.load_clean_transactions(clean_df)
            loaded_rejected = self.warehouse.load_rejected_transactions(rejected_df, run_id=run_id)

            # 6. Save persistent export snapshots
            if not rejected_df.empty:
                rejected_df.to_csv(CONFIG.REJECTED_DATA_PATH, index=False)
            if not clean_df.empty:
                clean_df.to_parquet(CONFIG.CLEAN_PARQUET_PATH, index=False)

            # 7. Optional GCP BigQuery Loading
            bq_report = BigQueryLoader.load_dataframe(clean_df)

            # 8. Record Pipeline Run Audit
            duration = round(time.time() - start_time, 3)
            audit_record = {
                "run_id": run_id,
                "execution_timestamp": datetime.now(timezone.utc).isoformat(),
                "execution_mode": CONFIG.EXECUTION_MODE,
                "source_name": Path(source_name).name if isinstance(source_name, str) else "buffer_stream",
                "total_records": total_received,
                "accepted_records": loaded_clean,
                "rejected_records": len(rejected_df),
                "duplicate_records": val_summary.get("duplicate_count", 0),
                "data_quality_score": dq_metrics.get("overall_score", 0.0),
                "duration_seconds": duration,
                "status": "SUCCESS"
            }
            self.warehouse.record_audit_run(audit_record)

            # 9. Build Lineage Manifest
            lineage = GovernanceManager.build_lineage_manifest(
                run_id=run_id,
                source_info=str(source_name),
                dest_table="duckdb.retail_transactions",
                row_counts=val_summary
            )

            report = {
                "run_id": run_id,
                "status": "SUCCESS",
                "duration_seconds": duration,
                "total_received": total_received,
                "accepted_count": loaded_clean,
                "rejected_count": len(rejected_df),
                "duplicate_count": val_summary.get("duplicate_count", 0),
                "data_quality": dq_metrics,
                "rejection_reasons": val_summary.get("rejection_reasons", {}),
                "gcp_status": bq_report,
                "lineage": lineage,
                "sample_clean_records": clean_df.head(5).to_dict(orient="records") if not clean_df.empty else [],
                "sample_rejected_records": rejected_df.head(5).to_dict(orient="records") if not rejected_df.empty else []
            }

            logger.info(
                f"Pipeline {run_id} finished successfully in {duration}s. "
                f"Accepted: {loaded_clean}, Rejected: {len(rejected_df)}, DQ Score: {dq_metrics['overall_score']}%"
            )
            return report

        except Exception as e:
            duration = round(time.time() - start_time, 3)
            logger.error(f"Pipeline run {run_id} failed: {e}", exc_info=True)
            fail_record = {
                "run_id": run_id,
                "execution_timestamp": datetime.now(timezone.utc).isoformat(),
                "execution_mode": CONFIG.EXECUTION_MODE,
                "source_name": str(source_name),
                "total_records": 0,
                "accepted_records": 0,
                "rejected_records": 0,
                "duplicate_records": 0,
                "data_quality_score": 0.0,
                "duration_seconds": duration,
                "status": f"FAILED: {str(e)}"
            }
            try:
                self.warehouse.record_audit_run(fail_record)
            except Exception:
                pass
            raise

if __name__ == "__main__":
    pipeline = RetailBatchPipeline()
    res = pipeline.run()
    print("Batch pipeline execution completed:")
    print(f"Run ID: {res['run_id']}")
    print(f"Accepted: {res['accepted_count']}, Rejected: {res['rejected_count']}")
    print(f"Data Quality Score: {res['data_quality']['overall_score']}%")

