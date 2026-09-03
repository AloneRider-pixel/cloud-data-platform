"""
CSV File Extractor.
Handles batch ingestion of CSV files with schema validation
and incremental processing.
"""
import csv
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Generator, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class CSVExtractor:
    """
    Extracts data from CSV files with:
    - Schema validation
    - Chunked reading for large files
    - Date partitioning
    - Encoding detection
    """

    def __init__(
        self,
        chunk_size: int = 10000,
        encoding: str = "utf-8",
        date_column: Optional[str] = None,
    ):
        self.chunk_size = chunk_size
        self.encoding = encoding
        self.date_column = date_column

    def extract_file(
        self,
        file_path: str,
        expected_columns: Optional[List[str]] = None,
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Extract data from a CSV file in chunks.
        
        Yields DataFrames of chunk_size rows.
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        logger.info(f"Extracting CSV: {file_path}")

        # Read in chunks
        for chunk_num, chunk in enumerate(
            pd.read_csv(
                file_path,
                encoding=self.encoding,
                chunksize=self.chunk_size,
                parse_dates=[self.date_column] if self.date_column else None,
                dtype=str,  # Read everything as string first, cast later
            )
        ):
            # Validate schema if expected columns provided
            if expected_columns:
                self._validate_schema(chunk, expected_columns, file_path)

            # Add metadata columns
            chunk["_source_file"] = path.name
            chunk["_ingestion_timestamp"] = datetime.utcnow().isoformat()
            chunk["_chunk_number"] = chunk_num

            logger.info(
                f"Chunk {chunk_num}: {len(chunk)} rows from {path.name}"
            )
            yield chunk

    def extract_directory(
        self,
        directory: str,
        pattern: str = "*.csv",
        expected_columns: Optional[List[str]] = None,
    ) -> Generator[Dict, None, None]:
        """
        Extract all CSV files matching pattern from a directory.
        
        Yields dicts with file info and data generator.
        """
        path = Path(directory)
        files = sorted(path.glob(pattern))

        logger.info(f"Found {len(files)} CSV files in {directory}")

        for file_path in files:
            yield {
                "file_path": str(file_path),
                "file_name": file_path.name,
                "file_size": file_path.stat().st_size,
                "modified": datetime.fromtimestamp(file_path.stat().st_mtime),
                "data": self.extract_file(str(file_path), expected_columns),
            }

    def _validate_schema(
        self,
        df: pd.DataFrame,
        expected_columns: List[str],
        file_path: str,
    ):
        """Validate DataFrame columns match expected schema."""
        actual_columns = set(df.columns)
        expected_set = set(expected_columns)

        missing = expected_set - actual_columns
        extra = actual_columns - expected_set

        if missing:
            logger.warning(
                f"Schema mismatch in {file_path}: "
                f"missing columns: {missing}"
            )
            raise ValueError(
                f"Missing required columns in {file_path}: {missing}"
            )

        if extra:
            logger.info(
                f"Extra columns in {file_path} (will be ignored): {extra}"
            )

    def extract_incremental(
        self,
        file_path: str,
        watermark_column: str,
        last_watermark: str,
    ) -> pd.DataFrame:
        """
        Extract only records newer than the watermark.
        Used for incremental loading of CSV data.
        """
        df = pd.read_csv(
            file_path,
            encoding=self.encoding,
            parse_dates=[watermark_column],
            dtype=str,
        )

        # Filter to only new records
        df[watermark_column] = pd.to_datetime(df[watermark_column])
        last_wm = pd.to_datetime(last_watermark)
        incremental_df = df[df[watermark_column] > last_wm]

        logger.info(
            f"Incremental extraction: {len(incremental_df)} new records "
            f"(watermark: {last_watermark})"
        )
        return incremental_df

    def get_file_metadata(self, file_path: str) -> Dict:
        """Get metadata about a CSV file."""
        path = Path(file_path)
        # Read first few rows to get column info
        sample = pd.read_csv(file_path, nrows=5)
        
        return {
            "file_path": str(path),
            "file_name": path.name,
            "file_size_bytes": path.stat().st_size,
            "row_count_estimate": sum(1 for _ in open(file_path)) - 1,  # Subtract header
            "columns": list(sample.columns),
            "column_types": {col: str(dtype) for col, dtype in sample.dtypes.items()},
            "modified": datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
        }
