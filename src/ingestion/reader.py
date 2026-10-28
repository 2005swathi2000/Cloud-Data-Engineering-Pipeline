"""
Data Ingestion Module.
Reads raw retail sales transaction data from CSV or JSON files, streams, or buffers.
"""

import json
import logging
from pathlib import Path
from typing import Union, BinaryIO
import pandas as pd

logger = logging.getLogger(__name__)

class DataIngestionReader:
    """Reads CSV or JSON transaction records into a unified Pandas DataFrame."""

    @staticmethod
    def read_source(source: Union[str, Path, BinaryIO], file_type: str = "csv") -> pd.DataFrame:
        """
        Ingests data from a file path, string path, or file-like buffer.

        Args:
            source: File path or byte buffer (e.g. from Streamlit upload).
            file_type: 'csv' or 'json'.

        Returns:
            pd.DataFrame containing the raw ingestion payload.
        """
        logger.info(f"Ingesting source data. Type: {file_type}")
        try:
            if file_type.lower() == "csv":
                df = pd.read_csv(source, dtype=str)  # Read as string initially for robust validation
            elif file_type.lower() == "json":
                if isinstance(source, (str, Path)):
                    with open(source, "r", encoding="utf-8") as f:
                        data = json.load(f)
                else:
                    data = json.load(source)
                df = pd.DataFrame(data).astype(str)
            else:
                raise ValueError(f"Unsupported file format: {file_type}. Expected 'csv' or 'json'.")

            # Clean column whitespace
            df.columns = [c.strip() for c in df.columns]
            logger.info(f"Ingested {len(df)} raw records with columns: {list(df.columns)}")
            return df
        except Exception as e:
            logger.error(f"Failed to ingest source data: {e}", exc_info=True)
            raise RuntimeError(f"Ingestion error: {e}")

