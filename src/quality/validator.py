"""
Data Validation and Quarantine Engine.
Performs schema conformance checks, null detection, type casting,
business rule validations, and duplicate isolation.
"""

import logging
from datetime import datetime, timezone
from typing import Tuple, List, Dict, Any
import pandas as pd
import numpy as np

from src.config import CONFIG

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    "transaction_id",
    "customer_id",
    "customer_email",
    "transaction_timestamp",
    "store_id",
    "store_region",
    "product_id",
    "product_name",
    "product_category",
    "unit_price",
    "quantity",
    "discount_amount",
    "payment_method"
]

class DataValidator:
    """Validates raw retail transactions and splits into valid and quarantined sets."""

    @classmethod
    def validate_batch(cls, raw_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
        """
        Validates raw DataFrame of records.

        Returns:
            Tuple of:
                valid_df: Cleaned and typed records.
                rejected_df: Quarantined records with 'rejection_reason' and 'rejected_at'.
                summary: Validation metrics dictionary.
        """
        logger.info(f"Starting batch validation for {len(raw_df)} records.")
        total_records = len(raw_df)
        rejection_reasons: List[str] = []
        valid_indices: List[int] = []
        rejected_rows: List[Dict[str, Any]] = []

        # 1. Check for required schema columns
        missing_cols = [c for c in REQUIRED_COLUMNS if c not in raw_df.columns]
        if missing_cols:
            error_msg = f"ERR_SCHEMA_MISSING_COLUMNS: {missing_cols}"
            logger.error(error_msg)
            # All records fail if fundamental columns are missing
            rejected = raw_df.copy()
            rejected["rejection_reason"] = error_msg
            rejected["rejected_at"] = datetime.now(timezone.utc).isoformat()
            return pd.DataFrame(), rejected, {
                "total": total_records,
                "accepted": 0,
                "rejected": total_records,
                "duplicates": 0,
                "reason_breakdown": {error_msg: total_records}
            }

        # Track seen transaction IDs within batch to detect duplicates
        seen_txn_ids = set()
        duplicate_count = 0

        # Row-by-row comprehensive verification
        for idx, row in raw_df.iterrows():
            errors = []
            
            # --- Check Transaction ID ---
            txn_id = str(row.get("transaction_id", "")).strip()
            if not txn_id or txn_id.lower() in ("nan", "none", "null"):
                errors.append("ERR_NULL_TRANSACTION_ID")
            elif txn_id in seen_txn_ids:
                errors.append(f"ERR_DUPLICATE_TXN_ID: {txn_id}")
                duplicate_count += 1
            else:
                seen_txn_ids.add(txn_id)

            # --- Check Customer ID ---
            cust_id = str(row.get("customer_id", "")).strip()
            if not cust_id or cust_id.lower() in ("nan", "none", "null"):
                errors.append("ERR_NULL_CUSTOMER_ID")

            # --- Check Timestamp ---
            raw_ts = str(row.get("transaction_timestamp", "")).strip()
            ts_parsed = None
            try:
                ts_parsed = pd.to_datetime(raw_ts)
                if pd.isna(ts_parsed):
                    errors.append("ERR_INVALID_TIMESTAMP")
            except Exception:
                errors.append("ERR_MALFORMED_TIMESTAMP")

            # --- Check Unit Price ---
            price_val = None
            try:
                price_val = float(row.get("unit_price", 0))
                if price_val <= 0:
                    errors.append(f"ERR_NON_POSITIVE_PRICE: {price_val}")
            except (ValueError, TypeError):
                errors.append("ERR_INVALID_NUMERIC_PRICE")

            # --- Check Quantity ---
            qty_val = None
            try:
                qty_val = int(float(row.get("quantity", 0)))
                if qty_val <= 0:
                    errors.append(f"ERR_NON_POSITIVE_QUANTITY: {qty_val}")
            except (ValueError, TypeError):
                errors.append("ERR_INVALID_NUMERIC_QUANTITY")

            # --- Check Discount and Business Rules ---
            disc_val = 0.0
            try:
                disc_val = float(row.get("discount_amount", 0))
                if disc_val < 0:
                    errors.append(f"ERR_NEGATIVE_DISCOUNT: {disc_val}")
                elif price_val is not None and qty_val is not None and price_val > 0 and qty_val > 0:
                    gross_calc = round(price_val * qty_val, 2)
                    if disc_val > gross_calc:
                        errors.append(f"ERR_DISCOUNT_EXCEEDS_GROSS: {disc_val} > {gross_calc}")
            except (ValueError, TypeError):
                errors.append("ERR_INVALID_NUMERIC_DISCOUNT")

            # --- Check Region ---
            region = str(row.get("store_region", "")).strip()
            if region not in CONFIG.SUPPORTED_REGIONS:
                errors.append(f"ERR_UNSUPPORTED_REGION: {region}")

            # Routing decision
            if errors:
                rejected_record = row.to_dict()
                rejected_record["rejection_reason"] = " | ".join(errors)
                rejected_record["rejected_at"] = datetime.now(timezone.utc).isoformat()
                rejected_rows.append(rejected_record)
            else:
                valid_indices.append(idx)

        # Build DataFrames
        valid_df = raw_df.loc[valid_indices].copy() if valid_indices else pd.DataFrame(columns=raw_df.columns)
        
        # Cast valid DataFrame types appropriately
        if not valid_df.empty:
            valid_df["unit_price"] = valid_df["unit_price"].astype(float)
            valid_df["quantity"] = valid_df["quantity"].astype(float).astype(int)
            valid_df["discount_amount"] = valid_df["discount_amount"].astype(float)
            valid_df["transaction_timestamp"] = pd.to_datetime(valid_df["transaction_timestamp"])

        rejected_df = pd.DataFrame(rejected_rows) if rejected_rows else pd.DataFrame(columns=list(raw_df.columns) + ["rejection_reason", "rejected_at"])

        # Reason breakdown summary
        reason_counts = {}
        for r in rejected_rows:
            for err in r["rejection_reason"].split(" | "):
                key = err.split(":")[0]
                reason_counts[key] = reason_counts.get(key, 0) + 1

        summary = {
            "total_received": total_records,
            "accepted_count": len(valid_df),
            "rejected_count": len(rejected_df),
            "duplicate_count": duplicate_count,
            "acceptance_rate_pct": round((len(valid_df) / total_records * 100), 2) if total_records > 0 else 0.0,
            "rejection_reasons": reason_counts
        }

        logger.info(
            f"Validation complete. Accepted: {summary['accepted_count']}, "
            f"Rejected: {summary['rejected_count']}, Duplicates: {duplicate_count}"
        )
        return valid_df, rejected_df, summary
