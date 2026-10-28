"""
Apache Beam Pipeline for Retail Data Ingestion & Transformation.
Designed to run locally with DirectRunner or at scale on Google Cloud Dataflow.
Implements branch processing (TaggedOutput) to quarantine malformed and invalid records.
"""

import argparse
import csv
import io
import json
import logging
from datetime import datetime
from typing import Dict, Any, Tuple

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

# Tag for quarantined records
TAG_REJECTED = "rejected"

class ParseAndValidateDoFn(beam.DoFn):
    """
    Parses incoming raw CSV lines, validates fields, and routes valid
    records to main output and invalid records to TAG_REJECTED.
    """

    def process(self, element: str):
        # Skip header or empty lines
        if not element or element.startswith("transaction_id"):
            return

        try:
            reader = csv.reader(io.StringIO(element))
            row = next(reader)
        except Exception as e:
            yield beam.pvalue.TaggedOutput(
                TAG_REJECTED,
                {"raw_line": element, "rejection_reason": f"ERR_CSV_PARSE_FAILED: {str(e)}"}
            )
            return

        expected_columns = 13
        if len(row) < expected_columns:
            yield beam.pvalue.TaggedOutput(
                TAG_REJECTED,
                {"raw_line": element, "rejection_reason": f"ERR_INSUFFICIENT_COLUMNS: {len(row)} < {expected_columns}"}
            )
            return

        txn_id = row[0].strip()
        cust_id = row[1].strip()
        cust_email = row[2].strip()
        timestamp = row[3].strip()
        store_id = row[4].strip()
        store_region = row[5].strip()
        prod_id = row[6].strip()
        prod_name = row[7].strip()
        prod_category = row[8].strip()

        errors = []
        if not txn_id:
            errors.append("ERR_NULL_TRANSACTION_ID")
        if not cust_id:
            errors.append("ERR_NULL_CUSTOMER_ID")

        try:
            price = float(row[9])
            if price <= 0:
                errors.append("ERR_NON_POSITIVE_PRICE")
        except ValueError:
            price = 0.0
            errors.append("ERR_INVALID_NUMERIC_PRICE")

        try:
            qty = int(float(row[10]))
            if qty <= 0:
                errors.append("ERR_NON_POSITIVE_QUANTITY")
        except ValueError:
            qty = 0
            errors.append("ERR_INVALID_NUMERIC_QUANTITY")

        try:
            disc = float(row[11])
            if disc < 0:
                errors.append("ERR_NEGATIVE_DISCOUNT")
            elif price > 0 and qty > 0 and disc > (price * qty):
                errors.append("ERR_DISCOUNT_EXCEEDS_GROSS")
        except ValueError:
            disc = 0.0
            errors.append("ERR_INVALID_NUMERIC_DISCOUNT")

        payment_method = row[12].strip()

        if errors:
            yield beam.pvalue.TaggedOutput(
                TAG_REJECTED,
                {
                    "transaction_id": txn_id,
                    "customer_id": cust_id,
                    "rejection_reason": " | ".join(errors),
                    "rejected_at": datetime.utcnow().isoformat()
                }
            )
        else:
            yield {
                "transaction_id": txn_id,
                "customer_id": cust_id,
                "customer_email": cust_email,
                "transaction_timestamp": timestamp,
                "store_id": store_id,
                "store_region": store_region,
                "product_id": prod_id,
                "product_name": prod_name,
                "product_category": prod_category,
                "unit_price": price,
                "quantity": qty,
                "discount_amount": disc,
                "payment_method": payment_method
            }

class TransformFinancialsAndPIIDoFn(beam.DoFn):
    """Computes financial totals, calendar derivations, and pseudonymizes PII."""

    def process(self, record: Dict[str, Any]):
        price = record["unit_price"]
        qty = record["quantity"]
        disc = record["discount_amount"]

        gross = round(price * qty, 2)
        net = max(0.0, round(gross - disc, 2))
        tax = round(net * 0.08, 2)
        total = round(net + tax, 2)

        # Mask email
        raw_email = record.get("customer_email", "")
        if "@" in raw_email:
            parts = raw_email.split("@", 1)
            masked_user = parts[0][0] + "***" + (parts[0][-1] if len(parts[0]) > 1 else "")
            masked_email = f"{masked_user}@{parts[1]}"
        else:
            masked_email = "anonymous@masked.local"

        # Date normalization
        ts_str = record["transaction_timestamp"]
        try:
            dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            date_str = dt.strftime("%Y-%m-%d")
            year, month, day, hour = dt.year, dt.month, dt.day, dt.hour
        except Exception:
            date_str = ts_str[:10]
            year, month, day, hour = 2026, 8, 1, 12

        record["gross_amount"] = gross
        record["net_amount"] = net
        record["tax_amount"] = tax
        record["total_amount"] = total
        record["customer_masked_email"] = masked_email
        record["transaction_date"] = date_str
        record["transaction_year"] = year
        record["transaction_month"] = month
        record["transaction_day"] = day
        record["transaction_hour"] = hour
        record["pipeline_engine"] = "APACHE_BEAM"
        record["processed_at"] = datetime.utcnow().isoformat()

        yield record

def run_beam_pipeline(argv=None):
    """Builds and executes the Apache Beam pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="sample_data/retail_transactions.csv", help="Input file path (local or gs://)")
    parser.add_argument("--output_clean", default="warehouse/beam_output_clean", help="Clean output prefix")
    parser.add_argument("--output_rejected", default="warehouse/beam_output_rejected", help="Quarantine output prefix")
    parser.add_argument("--bq_table", default="", help="Optional BigQuery table (PROJECT:DATASET.TABLE)")
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    logging.info(f"Starting Apache Beam pipeline. Input: {known_args.input}")

    with beam.Pipeline(options=pipeline_options) as p:
        # 1. Read input lines
        lines = p | "ReadTransactions" >> beam.io.ReadFromText(known_args.input)

        # 2. Branch: Validate and partition into clean vs rejected
        validation_results = (
            lines
            | "ParseAndValidate" >> beam.ParDo(ParseAndValidateDoFn()).with_outputs(
                TAG_REJECTED, main="valid"
            )
        )

        valid_records = validation_results["valid"]
        rejected_records = validation_results[TAG_REJECTED]

        # 3. Transform valid records
        transformed_records = (
            valid_records
            | "EnrichTransformations" >> beam.ParDo(TransformFinancialsAndPIIDoFn())
        )

        # 4. Sink valid records to JSON / Text
        (
            transformed_records
            | "FormatValidJSON" >> beam.Map(json.dumps)
            | "WriteValidOutput" >> beam.io.WriteToText(
                known_args.output_clean, file_name_suffix=".jsonl", shard_name_template=""
            )
        )

        # 5. Sink rejected records to Quarantine JSON / Text
        (
            rejected_records
            | "FormatRejectedJSON" >> beam.Map(json.dumps)
            | "WriteRejectedOutput" >> beam.io.WriteToText(
                known_args.output_rejected, file_name_suffix=".jsonl", shard_name_template=""
            )
        )

        # 6. Optional BigQuery sink if specified
        if known_args.bq_table:
            (
                transformed_records
                | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                    table=known_args.bq_table,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )

    logging.info("Apache Beam pipeline completed successfully.")

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_beam_pipeline()

