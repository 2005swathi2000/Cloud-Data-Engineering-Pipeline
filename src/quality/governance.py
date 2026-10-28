"""
Data Governance, Lineage, and Quality Metrics Module.
Calculates transparent Data Quality Scores, handles PII pseudonymization,
and generates pipeline lineage audit metadata.
"""

import hashlib
import re
from typing import Dict, Any
import pandas as pd

class GovernanceManager:
    """Manages governance standards, data quality scoring, and PII protection."""

    QUALITY_DISCLAIMER = (
        "Note: The Data Quality Score is a mathematical metric of structural and "
        "business-rule conformance against defined constraints. It does not "
        "guarantee real-world ground-truth accuracy of the external transaction source."
    )

    @classmethod
    def mask_email(cls, email: Any) -> str:
        """
        Pseudonymizes customer email address for PII minimization.
        Example: 'jane.doe@example.com' -> 'j***e@example.com'
        """
        if not email or pd.isna(email) or not isinstance(email, str):
            return "anonymous@masked.local"
        
        email = email.strip()
        if "@" not in email:
            # Fallback hash
            return hashlib.sha256(email.encode("utf-8")).hexdigest()[:12] + "@masked.local"

        parts = email.split("@", 1)
        user, domain = parts[0], parts[1]
        if len(user) <= 2:
            masked_user = user[0] + "*"
        else:
            masked_user = user[0] + "***" + user[-1]
            
        return f"{masked_user}@{domain}"

    @classmethod
    def compute_data_quality_score(cls, raw_df: pd.DataFrame, valid_df: pd.DataFrame, summary: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculates an explicit, weighted Data Quality Score (0 - 100%).
        
        Dimensions:
        - Completeness (25%): Absence of nulls in required fields
        - Validity (35%): Adherence to datatypes and ranges
        - Uniqueness (20%): Proportion of distinct transaction IDs
        - Consistency (20%): Conformance to retail business rules (e.g. discount <= gross)
        """
        total = len(raw_df)
        if total == 0:
            return {
                "overall_score": 0.0,
                "completeness_score": 0.0,
                "validity_score": 0.0,
                "uniqueness_score": 0.0,
                "consistency_score": 0.0,
                "disclaimer": cls.QUALITY_DISCLAIMER
            }

        # 1. Uniqueness
        dup_count = summary.get("duplicate_count", 0)
        uniqueness_ratio = max(0.0, (total - dup_count) / total)

        # 2. Completeness (check nulls in key identifiers)
        null_counts = raw_df[["transaction_id", "customer_id", "unit_price", "quantity"]].isna().sum().sum()
        max_possible_nulls = total * 4
        completeness_ratio = max(0.0, 1.0 - (null_counts / max_possible_nulls))

        # 3. Validity (accepted rows passing type and range checks)
        validity_ratio = len(valid_df) / total

        # 4. Consistency (records where discount <= gross)
        reasons = summary.get("rejection_reasons", {})
        discount_violations = reasons.get("ERR_DISCOUNT_EXCEEDS_GROSS", 0)
        consistency_ratio = max(0.0, (total - discount_violations) / total)

        # Weighted calculation
        overall_score = round(
            (completeness_ratio * 25.0) +
            (validity_ratio * 35.0) +
            (uniqueness_ratio * 20.0) +
            (consistency_ratio * 20.0),
            2
        )

        return {
            "overall_score": overall_score,
            "completeness_score": round(completeness_ratio * 100, 2),
            "validity_score": round(validity_ratio * 100, 2),
            "uniqueness_score": round(uniqueness_ratio * 100, 2),
            "consistency_score": round(consistency_ratio * 100, 2),
            "disclaimer": cls.QUALITY_DISCLAIMER
        }

    @classmethod
    def build_lineage_manifest(cls, run_id: str, source_info: str, dest_table: str, row_counts: Dict[str, int]) -> Dict[str, Any]:
        """Builds audit-ready data lineage metadata graph."""
        return {
            "pipeline_run_id": run_id,
            "lineage": [
                {
                    "step": 1,
                    "stage": "Ingestion",
                    "source": source_info,
                    "target": "Raw Ingestion Buffer",
                    "records_in": row_counts.get("total_received", 0)
                },
                {
                    "step": 2,
                    "stage": "Validation & Quarantine",
                    "source": "Raw Ingestion Buffer",
                    "target": ["Valid In-Memory Stream", "Rejected Quarantine Storage"],
                    "records_valid": row_counts.get("accepted_count", 0),
                    "records_rejected": row_counts.get("rejected_count", 0)
                },
                {
                    "step": 3,
                    "stage": "Transformation & Normalization",
                    "source": "Valid In-Memory Stream",
                    "target": "Analytical Enriched Frame",
                    "transforms": ["Gross/Net/Tax/Total", "Calendar Normalization", "PII Email Masking"]
                },
                {
                    "step": 4,
                    "stage": "Warehouse Loading",
                    "source": "Analytical Enriched Frame",
                    "target": dest_table,
                    "records_loaded": row_counts.get("accepted_count", 0)
                }
            ],
            "governance_controls": {
                "pii_masked": True,
                "idempotency_guaranteed": True,
                "quarantine_preserved": True
            }
        }

