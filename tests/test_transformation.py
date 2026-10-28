"""
Unit tests for transformation logic, financial arithmetic, and PII masking.
"""

import pandas as pd
from src.transformation.transformer import DataTransformer
from src.quality.governance import GovernanceManager

def test_financial_calculations():
    df = pd.DataFrame([{
        "unit_price": 100.0,
        "quantity": 2,
        "discount_amount": 20.0
    }])
    transformed = DataTransformer.compute_financials(df, tax_rate=0.08)

    # Gross = 200.00
    assert transformed.iloc[0]["gross_amount"] == 200.00
    # Net = 200.00 - 20.00 = 180.00
    assert transformed.iloc[0]["net_amount"] == 180.00
    # Tax = 180.00 * 0.08 = 14.40
    assert transformed.iloc[0]["tax_amount"] == 14.40
    # Total = 180.00 + 14.40 = 194.40
    assert transformed.iloc[0]["total_amount"] == 194.40

def test_date_normalization():
    df = pd.DataFrame([{
        "transaction_timestamp": "2026-08-15T14:45:00"
    }])
    transformed = DataTransformer.normalize_dates(df)

    assert transformed.iloc[0]["transaction_date"] == "2026-08-15"
    assert transformed.iloc[0]["transaction_year"] == 2026
    assert transformed.iloc[0]["transaction_month"] == 8
    assert transformed.iloc[0]["transaction_day"] == 15
    assert transformed.iloc[0]["transaction_hour"] == 14
    assert transformed.iloc[0]["day_of_week"] == "Saturday"

def test_pii_email_masking():
    email = "charlie.brown@peanuts.com"
    masked = GovernanceManager.mask_email(email)

    assert "@peanuts.com" in masked
    assert masked.startswith("c***n@")
    assert "charlie.brown" not in masked

