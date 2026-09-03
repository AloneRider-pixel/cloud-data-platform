"""
S3 Data Loader.
Handles writing data to S3/MinIO in Parquet format with partitioning.
"""
import io
import logging
import uuid
from datetime import datetime
from typing import Dict, List, Optional

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config as BotoConfig

from src.config import config

logger = logging.getLogger(__name__)


class S3Loader:
    """
    Loads data into S3/MinIO with:
    - Parquet format for columnar efficiency
    - Date-based partitioning (year/month/day)
    - Idempotent writes (safe to re-run)
    - Metadata tracking
    """

    def __init__(
        self,
        bucket: str = None,
        endpoint_url: str = None,
        aws_access_key: str = None,
        aws_secret_key: str = None,
    ):
        self.bucket = bucket or config.s3_bucket
        
        boto_config = BotoConfig(
            retries={"max_attempts": 3, "mode": "adaptive"},
            s3={"addressing_style": "path-style"},
        )

        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url or config.s3_endpoint,
            aws_access_key_id=aws_access_key or config.aws_access_key,
            aws_secret_access_key=aws_secret_key or config.aws_secret_key,
            config=boto_config,
        )

    def load_dataframe(
        self,
        df: pd.DataFrame,
        prefix: str,
        partition_column: Optional[str] = None,
        filename: Optional[str] = None,
        compression: str = "snappy",
        metadata: Optional[Dict] = None,
    ) -> str:
        """
        Load a DataFrame to S3 as Parquet.
        
        Args:
            df: DataFrame to write
            prefix: S3 key prefix (e.g., "raw/orders/")
            partition_column: Column to partition by (date column)
            filename: Override auto-generated filename
            compression: Parquet compression codec
            metadata: Additional metadata to embed
        
        Returns:
            S3 key of the written file
        """
        if df.empty:
            logger.warning("Empty DataFrame, skipping S3 write")
            return ""

        # Generate partition path
        if partition_column and partition_column in df.columns:
            date_val = pd.to_datetime(df[partition_column].iloc[0])
            partition_path = (
                f"year={date_val.year}/"
                f"month={date_val.month:02d}/"
                f"day={date_val.day:02d}/"
            )
        else:
            now = datetime.utcnow()
            partition_path = (
                f"year={now.year}/"
                f"month={now.month:02d}/"
                f"day={now.day:02d}/"
            )

        # Generate filename
        if not filename:
            filename = f"{uuid.uuid4().hex}.parquet"

        s3_key = f"{prefix}{partition_path}{filename}"

        # Write Parquet to buffer
        buffer = io.BytesIO()
        
        # Convert to Arrow table with metadata
        table = pa.Table.from_pandas(df)
        
        # Add custom metadata
        custom_metadata = {
            "ingestion_timestamp": datetime.utcnow().isoformat(),
            "row_count": str(len(df)),
            "source": prefix,
        }
        if metadata:
            custom_metadata.update(metadata)
        
        existing_metadata = table.schema.metadata or {}
        existing_metadata.update({k.encode(): v.encode() for k, v in custom_metadata.items()})
        table = table.replace_schema_metadata(existing_metadata)
        
        pq.write_table(
            table,
            buffer,
            compression=compression,
            coerce_timestamps="us",
        )
        buffer.seek(0)

        # Upload to S3
        self.client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
            Metadata=custom_metadata,
        )

        file_size = buffer.getbuffer().nbytes
        logger.info(
            f"Written to S3: s3://{self.bucket}/{s3_key} "
            f"({len(df)} rows, {file_size / 1024:.1f} KB)"
        )

        return s3_key

    def load_incremental(
        self,
        df: pd.DataFrame,
        prefix: str,
        watermark_column: str,
        partition_column: Optional[str] = None,
    ) -> str:
        """
        Load incremental data to S3.
        Ensures idempotency by using deterministic filenames.
        """
        if df.empty:
            logger.info("No new records to load")
            return ""

        # Generate deterministic filename based on data range
        if watermark_column in df.columns:
            min_ts = pd.to_datetime(df[watermark_column]).min()
            max_ts = pd.to_datetime(df[watermark_column]).max()
            filename = f"incremental_{min_ts.strftime('%Y%m%d_%H%M%S')}_{max_ts.strftime('%Y%m%d_%H%M%S')}.parquet"
        else:
            filename = f"incremental_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"

        return self.load_dataframe(
            df=df,
            prefix=prefix,
            partition_column=partition_column,
            filename=filename,
            metadata={"load_type": "incremental"},
        )

    def list_files(self, prefix: str, max_keys: int = 100) -> List[Dict]:
        """List files in an S3 prefix."""
        response = self.client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=prefix,
            MaxKeys=max_keys,
        )

        files = []
        for obj in response.get("Contents", []):
            files.append({
                "key": obj["Key"],
                "size": obj["Size"],
                "last_modified": obj["LastModified"].isoformat(),
            })

        return files

    def read_parquet(self, s3_key: str) -> pd.DataFrame:
        """Read a Parquet file from S3."""
        response = self.client.get_object(
            Bucket=self.bucket,
            Key=s3_key,
        )
        buffer = io.BytesIO(response["Body"].read())
        return pd.read_parquet(buffer)
