"""
Integration tests verifying that all 8 SQL analytics queries execute cleanly on the warehouse.
"""

from pathlib import Path
import pytest
from src.warehouse.duckdb_warehouse import DuckDBWarehouse
from src.pipeline.batch_pipeline import RetailBatchPipeline
from src.config import CONFIG, PROJECT_ROOT

@pytest.fixture(scope="module")
def populated_warehouse():
    warehouse = DuckDBWarehouse(db_path=CONFIG.DUCKDB_PATH)
    pipeline = RetailBatchPipeline(warehouse=warehouse)
    pipeline.run(source=CONFIG.SAMPLE_DATA_CSV, file_type="csv")
    return warehouse

SQL_FILES = [
    "01_total_revenue_summary.sql",
    "02_revenue_by_category.sql",
    "03_revenue_trends.sql",
    "04_top_products.sql",
    "05_customer_summaries.sql",
    "06_regional_performance.sql",
    "07_duplicate_anomaly_checks.sql",
    "08_data_quality_monitoring.sql"
]

@pytest.mark.parametrize("sql_file", SQL_FILES)
def test_sql_query_execution(populated_warehouse, sql_file):
    query_path = PROJECT_ROOT / "sql" / sql_file
    assert query_path.exists(), f"Query file missing: {query_path}"

    with open(query_path, "r", encoding="utf-8") as f:
        query_text = f.read()

    # Filter out empty comments and execute
    clean_query = "\n".join([line for line in query_text.splitlines() if not line.strip().startswith("--")])
    if clean_query.strip():
        result_df = populated_warehouse.execute_query(clean_query)
        assert result_df is not None
        assert len(result_df.columns) > 0

