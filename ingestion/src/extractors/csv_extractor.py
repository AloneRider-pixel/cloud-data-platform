"""
CSV File Extractor.
Handles batch ingestion of CSV files with schema validation
and incremental processing.
"""
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Generator, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class CSVExtractor:
    """Extracts data from CSV files with chunking and validation."""

    def __init__(self, chunk_size: int = 10000, encoding: str = "utf-8", date_column: Optional[str] = None):
        self.chunk_size = chunk_size
        self.encoding = encoding
        self.date_column = date_column

    def extract_file(self, file_path: str, expected_columns: Optional[List[str]] = None) -> Generator[pd.DataFrame, None, None]:
        """Extract data from a CSV file in chunks."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        logger.info("Extracting CSV: %s", file_path)
        for chunk_num, chunk in enumerate(
            pd.read_csv(
                file_path,
                encoding=self.encoding,
                chunksize=self.chunk_size,
                parse_dates=[self.date_column] if self.date_column else None,
                dtype=str,
            )
        ):
            if expected_columns:
                self._validate_schema(chunk, expected_columns, file_path)

            chunk["_source_file"] = path.name
            chunk["_ingestion_timestamp"] = datetime.utcnow().isoformat()
            chunk["_chunk_number"] = chunk_num
            logger.info("Chunk %s: %s rows from %s", chunk_num, len(chunk), path.name)
            yield chunk

    def extract_directory(self, directory: str, pattern: str = "*.csv", expected_columns: Optional[List[str]] = None) -> Generator[Dict, None, None]:
        """Extract all CSV files matching pattern from a directory."""
        path = Path(directory)
        files = sorted(path.glob(pattern))
        logger.info("Found %s CSV files in %s", len(files), directory)
        for file_path in files:
            yield {
                "file_path": str(file_path),
                "file_name": file_path.name,
                "file_size": file_path.stat().st_size,
                "modified": datetime.fromtimestamp(file_path.stat().st_mtime),
                "data": self.extract_file(str(file_path), expected_columns),
            }

    def _validate_schema(self, df: pd.DataFrame, expected_columns: List[str], file_path: str):
        """Validate DataFrame columns match expected schema."""
        actual_columns = set(df.columns)
        expected_set = set(expected_columns)
        missing = expected_set - actual_columns
        extra = actual_columns - expected_set

        if missing:
            logger.warning("Schema mismatch in %s: missing columns: %s", file_path, missing)
            raise ValueError(f"Missing required columns in {file_path}: {missing}")
        if extra:
            logger.info("Extra columns in %s (will be ignored): %s", file_path, extra)

    def extract_incremental(self, file_path: str, watermark_column: str, last_watermark: str) -> pd.DataFrame:
        """Extract only records newer than the watermark."""
        df = pd.read_csv(file_path, encoding=self.encoding, parse_dates=[watermark_column], dtype=str)
        df[watermark_column] = pd.to_datetime(df[watermark_column])
        last_wm = pd.to_datetime(last_watermark)
        incremental_df = df[df[watermark_column] > last_wm]
        logger.info("Incremental extraction: %s new records (watermark: %s)", len(incremental_df), last_watermark)
        return incremental_df

    def get_file_metadata(self, file_path: str) -> Dict:
        """Get metadata about a CSV file."""
        path = Path(file_path)
        sample = pd.read_csv(file_path, nrows=5)
        with open(file_path, encoding=self.encoding) as handle:
            row_count = max(sum(1 for _ in handle) - 1, 0)

        return {
            "file_path": str(path),
            "file_name": path.name,
            "file_size_bytes": path.stat().st_size,
            "row_count_estimate": row_count,
            "columns": list(sample.columns),
            "column_types": {col: str(dtype) for col, dtype in sample.dtypes.items()},
            "modified": datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
        }
