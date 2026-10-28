"""
Batch Pipeline Orchestration Engine.
Coordinates ingestion, validation, quarantine, transformation,
local DuckDB loading, or Google BigQuery loading with staging MERGE idempotency.
"""

import argparse
import sys
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

    def run(
        self,
        source: Optional[Union[str, Path, Any]] = None,
        file_type: str = "csv",
        target: str = "duckdb",
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Executes end-to-end batch data pipeline run.

        Args:
            source: Path to file, buffer, or None (defaults to built-in sample data).
            file_type: 'csv' or 'json'.
            target: 'duckdb' (local warehouse) or 'bigquery' (Google Cloud BigQuery).
            dry_run: If True, inspects plans and prints BigQuery MERGE SQL without executing.

        Returns:
            Dict containing complete execution report, data quality metrics, and row counts.
        """
        start_time = time.time()
        run_id = f"RUN-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
        source_target = source or (CONFIG.SAMPLE_DATA_CSV if file_type == "csv" else CONFIG.SAMPLE_DATA_JSON)
        source_name = str(source_target)

        target_mode = target.lower()
        logger.info(f"Starting Retail Batch Pipeline. Run ID: {run_id}, Target: {target_mode}, Dry-Run: {dry_run}")

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

            target_report = {}

            # 5. Route to Target: BigQuery or DuckDB
            if target_mode == "bigquery":
                # BigQuery Load (Dry-Run or Live Idempotent MERGE)
                target_report = BigQueryLoader.load_batch_idempotent(
                    clean_df=clean_df,
                    rejected_df=rejected_df,
                    run_id=run_id,
                    dry_run=dry_run
                )
                loaded_clean = len(clean_df) if not dry_run else 0
                loaded_rejected = len(rejected_df) if not dry_run else 0

            else:
                # Local DuckDB Load (Default)
                loaded_clean = self.warehouse.load_clean_transactions(clean_df)
                loaded_rejected = self.warehouse.load_rejected_transactions(rejected_df, run_id=run_id)

                # Save local file snapshots
                if not rejected_df.empty:
                    rejected_df.to_csv(CONFIG.REJECTED_DATA_PATH, index=False)
                if not clean_df.empty:
                    clean_df.to_parquet(CONFIG.CLEAN_PARQUET_PATH, index=False)

                target_report = {
                    "mode": "LOCAL_DUCKDB",
                    "status": "SUCCESS",
                    "clean_rows_loaded": loaded_clean,
                    "rejected_rows_quarantined": loaded_rejected
                }

            # 6. Record Pipeline Run Audit
            duration = round(time.time() - start_time, 3)
            audit_record = {
                "run_id": run_id,
                "execution_timestamp": datetime.now(timezone.utc).isoformat(),
                "execution_mode": target_mode.upper(),
                "source_name": Path(source_name).name if isinstance(source_name, str) else "buffer_stream",
                "total_records": total_received,
                "accepted_records": len(clean_df),
                "rejected_records": len(rejected_df),
                "duplicate_records": val_summary.get("duplicate_count", 0),
                "data_quality_score": dq_metrics.get("overall_score", 0.0),
                "duration_seconds": duration,
                "status": "SUCCESS" if not dry_run else "DRY_RUN_SUCCESS"
            }
            self.warehouse.record_audit_run(audit_record)

            # 7. Lineage Manifest
            lineage = GovernanceManager.build_lineage_manifest(
                run_id=run_id,
                source_info=str(source_name),
                dest_table=f"{target_mode}.retail_transactions",
                row_counts=val_summary
            )

            report = {
                "run_id": run_id,
                "status": "SUCCESS" if not dry_run else "DRY_RUN",
                "target": target_mode,
                "duration_seconds": duration,
                "total_received": total_received,
                "accepted_count": len(clean_df),
                "rejected_count": len(rejected_df),
                "duplicate_count": val_summary.get("duplicate_count", 0),
                "data_quality": dq_metrics,
                "rejection_reasons": val_summary.get("rejection_reasons", {}),
                "target_report": target_report,
                "lineage": lineage,
                "sample_clean_records": clean_df.head(5).to_dict(orient="records") if not clean_df.empty else [],
                "sample_rejected_records": rejected_df.head(5).to_dict(orient="records") if not rejected_df.empty else []
            }

            logger.info(
                f"Pipeline {run_id} finished ({target_mode}). "
                f"Accepted: {len(clean_df)}, Rejected: {len(rejected_df)}, DQ Score: {dq_metrics['overall_score']}%"
            )
            return report

        except Exception as e:
            duration = round(time.time() - start_time, 3)
            logger.error(f"Pipeline run {run_id} failed: {e}", exc_info=True)
            raise

def main():
    """CLI entry point supporting --target and --dry-run flags."""
    parser = argparse.ArgumentParser(description="Retail Sales Cloud Data Engineering Pipeline")
    parser.add_argument(
        "--target",
        choices=["duckdb", "bigquery"],
        default=CONFIG.DEFAULT_TARGET,
        help="Storage and analytics target: 'duckdb' (local) or 'bigquery' (Google Cloud Platform)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned operations and BigQuery MERGE SQL without executing."
    )
    parser.add_argument(
        "--source",
        default=None,
        help="Custom path to CSV or JSON retail transaction file."
    )
    parser.add_argument(
        "--format",
        choices=["csv", "json"],
        default="csv",
        help="Input format (default: csv)"
    )

    args = parser.parse_args()

    pipeline = RetailBatchPipeline()
    report = pipeline.run(
        source=args.source,
        file_type=args.format,
        target=args.target,
        dry_run=args.dry_run
    )

    print("\n" + "=" * 50)
    print("PIPELINE EXECUTION SUMMARY")
    print("=" * 50)
    print(f"Run ID:            {report['run_id']}")
    print(f"Target:            {report['target'].upper()}")
    print(f"Status:            {report['status']}")
    print(f"Records Received:  {report['total_received']:,}")
    print(f"Accepted Clean:    {report['accepted_count']:,}")
    print(f"Quarantined Rows:  {report['rejected_count']:,}")
    print(f"Duplicate IDs:     {report['duplicate_count']:,}")
    print(f"DQ Conformance:    {report['data_quality']['overall_score']}%")
    print(f"Duration:          {report['duration_seconds']}s")
    print("=" * 50)

if __name__ == "__main__":
    main()
