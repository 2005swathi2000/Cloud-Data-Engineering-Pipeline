"""
Integration tests for the end-to-end retail batch data pipeline and warehouse loading.
"""

import tempfile
from pathlib import Path
import pytest
from src.warehouse.duckdb_warehouse import DuckDBWarehouse
from src.pipeline.batch_pipeline import RetailBatchPipeline
from src.config import CONFIG

@pytest.fixture
def temp_warehouse():
    temp_dir = tempfile.mkdtemp()
    temp_path = Path(temp_dir) / "test_retail.duckdb"
    warehouse = DuckDBWarehouse(db_path=temp_path)
    yield warehouse
    try:
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
    except Exception:
        pass

def test_pipeline_end_to_end_execution(temp_warehouse):
    pipeline = RetailBatchPipeline(warehouse=temp_warehouse)
    report = pipeline.run(source=CONFIG.SAMPLE_DATA_CSV, file_type="csv")

    assert report["status"] == "SUCCESS"
    assert report["total_received"] > 0
    assert report["accepted_count"] > 0
    assert report["rejected_count"] > 0
    assert report["data_quality"]["overall_score"] > 80.0

    # Verify tables in warehouse
    df_clean = temp_warehouse.execute_query("SELECT COUNT(*) AS cnt FROM retail_transactions;")
    assert df_clean.iloc[0]["cnt"] == report["accepted_count"]

    df_rejected = temp_warehouse.execute_query("SELECT COUNT(*) AS cnt FROM rejected_transactions;")
    assert df_rejected.iloc[0]["cnt"] == report["rejected_count"]

    df_audit = temp_warehouse.execute_query("SELECT COUNT(*) AS cnt FROM pipeline_audit_runs;")
    assert df_audit.iloc[0]["cnt"] == 1

def test_pipeline_idempotent_rerun(temp_warehouse):
    """Running the same batch twice should update existing records without doubling table rows in both target and quarantine tables."""
    pipeline = RetailBatchPipeline(warehouse=temp_warehouse)
    
    # Run 1
    report1 = pipeline.run(source=CONFIG.SAMPLE_DATA_CSV, file_type="csv")
    clean_cnt1 = temp_warehouse.execute_query("SELECT COUNT(*) AS cnt FROM retail_transactions;").iloc[0]["cnt"]
    rej_cnt1 = temp_warehouse.execute_query("SELECT COUNT(*) AS cnt FROM rejected_transactions;").iloc[0]["cnt"]

    # Run 2
    report2 = pipeline.run(source=CONFIG.SAMPLE_DATA_CSV, file_type="csv")
    clean_cnt2 = temp_warehouse.execute_query("SELECT COUNT(*) AS cnt FROM retail_transactions;").iloc[0]["cnt"]
    rej_cnt2 = temp_warehouse.execute_query("SELECT COUNT(*) AS cnt FROM rejected_transactions;").iloc[0]["cnt"]

    # Clean records idempotency
    assert clean_cnt1 == clean_cnt2
    assert clean_cnt1 == report1["accepted_count"]

    # Quarantined records idempotency (stable composite key hash deduplication)
    assert rej_cnt1 == rej_cnt2
    assert rej_cnt1 == report1["rejected_count"]

def test_quarantine_stable_key_hash():
    """Verify deterministic 24-character SHA-256 stable key generation for quarantine deduplication."""
    from src.loaders.bigquery_loader import BigQueryLoader

    key1 = BigQueryLoader.compute_rejection_id(
        transaction_id="TXN_12345",
        rejection_reason="ERR_NULL_PRICE|ERR_NEGATIVE_AMOUNT",
        raw_payload="{'store_id': 'STR_001', 'unit_price': None}"
    )
    key2 = BigQueryLoader.compute_rejection_id(
        transaction_id="TXN_12345",
        rejection_reason="ERR_NULL_PRICE|ERR_NEGATIVE_AMOUNT",
        raw_payload="{'store_id': 'STR_001', 'unit_price': None}"
    )
    key3 = BigQueryLoader.compute_rejection_id(
        transaction_id="TXN_99999",
        rejection_reason="ERR_INVALID_EMAIL",
        raw_payload="{'customer_email': 'bad_email'}"
    )

    assert key1 == key2, "Stable key must be completely deterministic for identical inputs"
    assert key1 != key3, "Different transaction data must produce different keys"
    assert len(key1) == 24, "Key should be 24-character hash prefix"

def test_bigquery_dry_run_generation():
    """Verify BigQueryLoader dry-run executes cleanly and outputs valid DML MERGE statements."""
    import pandas as pd
    from src.loaders.bigquery_loader import BigQueryLoader

    sample_clean = pd.DataFrame([{
        "transaction_id": "TXN_TEST_1",
        "customer_id": "CUST_1",
        "customer_masked_email": "c***@example.com",
        "transaction_timestamp": "2026-01-01 12:00:00",
        "transaction_date": "2026-01-01",
        "transaction_year": 2026,
        "transaction_month": 1,
        "transaction_day": 1,
        "transaction_hour": 12,
        "day_of_week": "Thursday",
        "store_id": "STR_1",
        "store_region": "North",
        "product_id": "PRD_1",
        "product_name": "Test Item",
        "product_category": "Electronics",
        "unit_price": 100.0,
        "quantity": 1,
        "discount_amount": 0.0,
        "gross_amount": 100.0,
        "net_amount": 100.0,
        "tax_amount": 8.0,
        "total_amount": 108.0,
        "payment_method": "Credit Card",
        "pipeline_run_id": "TEST_RUN",
        "ingested_at": "2026-01-01 12:00:00"
    }])

    sample_rejected = pd.DataFrame([{
        "transaction_id": "TXN_FAIL_1",
        "customer_id": "CUST_BAD",
        "rejection_reason": "ERR_NULL_UNIT_PRICE",
        "raw_payload": "{'bad': True}"
    }])

    dry_report = BigQueryLoader.execute_dry_run(sample_clean, sample_rejected)

    assert dry_report["mode"] == "DRY_RUN"
    assert dry_report["status"] == "READY"
    assert "MERGE INTO" in dry_report["merge_sql"]
    assert "ON T.transaction_id = S.transaction_id" in dry_report["merge_sql"]
    assert "MERGE INTO" in dry_report["quarantine_merge_sql"]
    assert "ON T.rejection_id = S.rejection_id" in dry_report["quarantine_merge_sql"]
    assert dry_report["clean_records_count"] == 1
    assert dry_report["rejected_records_count"] == 1
