"""
Data Transformation and Normalization Module.
Modular, testable pure functions for financial calculations,
calendar derivations, and PII protection.
"""

import logging
from datetime import datetime, timezone
import pandas as pd
from src.config import CONFIG
from src.quality.governance import GovernanceManager

logger = logging.getLogger(__name__)

class DataTransformer:
    """Transforms validated retail transaction records into analytical format."""

    @staticmethod
    def compute_financials(df: pd.DataFrame, tax_rate: float = CONFIG.DEFAULT_TAX_RATE) -> pd.DataFrame:
        """
        Computes gross_amount, net_amount, tax_amount, and total_amount.
        
        Formulas:
            gross_amount = unit_price * quantity
            net_amount = gross_amount - discount_amount
            tax_amount = net_amount * tax_rate
            total_amount = net_amount + tax_amount
        """
        out = df.copy()
        out["gross_amount"] = (out["unit_price"] * out["quantity"]).round(2)
        out["net_amount"] = (out["gross_amount"] - out["discount_amount"]).round(2)
        # Ensure net amount cannot be negative
        out["net_amount"] = out["net_amount"].clip(lower=0.0)
        out["tax_amount"] = (out["net_amount"] * tax_rate).round(2)
        out["total_amount"] = (out["net_amount"] + out["tax_amount"]).round(2)
        return out

    @staticmethod
    def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
        """Derives standard analytical calendar dimensions."""
        out = df.copy()
        ts = pd.to_datetime(out["transaction_timestamp"])
        out["transaction_date"] = ts.dt.date.astype(str)
        out["transaction_year"] = ts.dt.year.astype(int)
        out["transaction_month"] = ts.dt.month.astype(int)
        out["transaction_day"] = ts.dt.day.astype(int)
        out["transaction_hour"] = ts.dt.hour.astype(int)
        out["day_of_week"] = ts.dt.day_name()
        return out

    @staticmethod
    def mask_pii(df: pd.DataFrame) -> pd.DataFrame:
        """Pseudonymizes customer email addresses for governance compliance."""
        out = df.copy()
        if "customer_email" in out.columns:
            out["customer_masked_email"] = out["customer_email"].apply(GovernanceManager.mask_email)
        else:
            out["customer_masked_email"] = "anonymous@masked.local"
        return out

    @staticmethod
    def enrich_metadata(df: pd.DataFrame, run_id: str) -> pd.DataFrame:
        """Stamps run UUID and warehouse ingestion timestamp."""
        out = df.copy()
        out["pipeline_run_id"] = run_id
        out["ingested_at"] = datetime.now(timezone.utc).isoformat()
        return out

    @classmethod
    def transform_all(cls, valid_df: pd.DataFrame, run_id: str) -> pd.DataFrame:
        """Applies the full sequence of data transformations."""
        if valid_df.empty:
            return pd.DataFrame()
            
        logger.info(f"Transforming {len(valid_df)} valid records for run {run_id}.")
        transformed = cls.compute_financials(valid_df)
        transformed = cls.normalize_dates(transformed)
        transformed = cls.mask_pii(transformed)
        transformed = cls.enrich_metadata(transformed, run_id)
        
        # Select and arrange clean analytical columns
        ordered_cols = [
            "transaction_id", "customer_id", "customer_masked_email",
            "transaction_timestamp", "transaction_date", "transaction_year",
            "transaction_month", "transaction_day", "transaction_hour", "day_of_week",
            "store_id", "store_region", "product_id", "product_name", "product_category",
            "unit_price", "quantity", "discount_amount", "gross_amount", "net_amount",
            "tax_amount", "total_amount", "payment_method", "pipeline_run_id", "ingested_at"
        ]
        available_cols = [c for c in ordered_cols if c in transformed.columns]
        return transformed[available_cols]

