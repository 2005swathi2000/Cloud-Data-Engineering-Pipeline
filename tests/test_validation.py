"""
Unit tests for data validation, schema enforcement, and quarantine routing.
"""

import pandas as pd
import pytest
from src.quality.validator import DataValidator

@pytest.fixture
def sample_valid_row():
    return {
        "transaction_id": "TXN-99901",
        "customer_id": "CUST-9001",
        "customer_email": "jane.doe@example.com",
        "transaction_timestamp": "2026-08-15T14:30:00",
        "store_id": "STORE-E30",
        "store_region": "East",
        "product_id": "PROD-101",
        "product_name": "Apple MacBook Pro 14",
        "product_category": "Electronics",
        "unit_price": "1999.00",
        "quantity": "2",
        "discount_amount": "50.00",
        "payment_method": "Credit Card"
    }

def test_validate_valid_record(sample_valid_row):
    df = pd.DataFrame([sample_valid_row])
    valid_df, rejected_df, summary = DataValidator.validate_batch(df)

    assert len(valid_df) == 1
    assert len(rejected_df) == 0
    assert summary["accepted_count"] == 1
    assert summary["rejected_count"] == 0
    assert valid_df.iloc[0]["unit_price"] == 1999.00
    assert valid_df.iloc[0]["quantity"] == 2

def test_validate_negative_quantity(sample_valid_row):
    bad_row = sample_valid_row.copy()
    bad_row["quantity"] = "-5"
    df = pd.DataFrame([bad_row])
    valid_df, rejected_df, summary = DataValidator.validate_batch(df)

    assert len(valid_df) == 0
    assert len(rejected_df) == 1
    assert "ERR_NON_POSITIVE_QUANTITY" in rejected_df.iloc[0]["rejection_reason"]

def test_validate_discount_exceeds_gross(sample_valid_row):
    bad_row = sample_valid_row.copy()
    bad_row["unit_price"] = "10.00"
    bad_row["quantity"] = "1"
    bad_row["discount_amount"] = "50.00"  # Discount > Gross
    df = pd.DataFrame([bad_row])
    valid_df, rejected_df, summary = DataValidator.validate_batch(df)

    assert len(valid_df) == 0
    assert len(rejected_df) == 1
    assert "ERR_DISCOUNT_EXCEEDS_GROSS" in rejected_df.iloc[0]["rejection_reason"]

def test_validate_duplicate_txn_id(sample_valid_row):
    row1 = sample_valid_row.copy()
    row2 = sample_valid_row.copy()
    df = pd.DataFrame([row1, row2])
    valid_df, rejected_df, summary = DataValidator.validate_batch(df)

    assert len(valid_df) == 1
    assert len(rejected_df) == 1
    assert summary["duplicate_count"] == 1
    assert "ERR_DUPLICATE_TXN_ID" in rejected_df.iloc[0]["rejection_reason"]

def test_validate_missing_mandatory_schema():
    broken_df = pd.DataFrame([{"foo": "bar"}])
    valid_df, rejected_df, summary = DataValidator.validate_batch(broken_df)

    assert len(valid_df) == 0
    assert len(rejected_df) == 1
    assert "ERR_SCHEMA_MISSING_COLUMNS" in rejected_df.iloc[0]["rejection_reason"]

