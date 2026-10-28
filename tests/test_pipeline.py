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
    """Running the same batch twice should update existing records without doubling table rows."""
    pipeline = RetailBatchPipeline(warehouse=temp_warehouse)
    
    # Run 1
    report1 = pipeline.run(source=CONFIG.SAMPLE_DATA_CSV, file_type="csv")
    count1 = temp_warehouse.execute_query("SELECT COUNT(*) AS cnt FROM retail_transactions;").iloc[0]["cnt"]

    # Run 2
    report2 = pipeline.run(source=CONFIG.SAMPLE_DATA_CSV, file_type="csv")
    count2 = temp_warehouse.execute_query("SELECT COUNT(*) AS cnt FROM retail_transactions;").iloc[0]["cnt"]

    assert count1 == count2
    assert count1 == report1["accepted_count"]
