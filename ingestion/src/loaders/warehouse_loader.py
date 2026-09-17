"""
PostgreSQL / Snowflake Data Loader.
Handles writing transformed data to the analytical warehouse.
"""
import logging
import uuid
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.config import config

logger = logging.getLogger(__name__)


class WarehouseLoader:
    """Load data into PostgreSQL/Snowflake analytical warehouses."""

    def __init__(self, database_url: str = None, schema: str = "raw"):
        self.database_url = database_url or config.database_url
        self.schema = schema
        self.engine: Engine = create_engine(
            self.database_url,
            pool_size=5,
            max_overflow=10,
        )

    def load_full_refresh(self, df: pd.DataFrame, table_name: str, schema: str = None, if_exists: str = "replace") -> int:
        """Full refresh load - replaces entire table."""
        target_schema = schema or self.schema
        row_count = len(df)

        logger.info("Full refresh: %s.%s (%s rows)", target_schema, table_name, row_count)

        df.to_sql(
            name=table_name,
            con=self.engine,
            schema=target_schema,
            if_exists=if_exists,
            index=False,
            chunksize=10000,
            method="multi",
        )

        logger.info("Loaded %s rows into %s.%s", row_count, target_schema, table_name)
        return row_count

    def load_incremental(self, df: pd.DataFrame, table_name: str, primary_key: str, schema: str = None) -> int:
        """Incremental upsert using INSERT ... ON CONFLICT UPDATE."""
        if df.empty:
            logger.info("No records to upsert")
            return 0

        target_schema = schema or self.schema
        full_table = f"{target_schema}.{table_name}"
        self._ensure_table_exists(df, table_name, target_schema)

        columns = list(df.columns)
        col_list = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])
        update_set = ", ".join(
            [f"{col} = EXCLUDED.{col}" for col in columns if col != primary_key]
        )

        upsert_sql = f"""
            INSERT INTO {full_table} ({col_list})
            VALUES ({placeholders})
            ON CONFLICT ({primary_key})
            DO UPDATE SET {update_set}
        """

        total_rows = 0
        batch_size = 5000

        for start in range(0, len(df), batch_size):
            batch = df.iloc[start:start + batch_size]
            records = batch.to_dict("records")

            with self.engine.connect() as conn:
                conn.execute(text(upsert_sql), records)
                conn.commit()

            total_rows += len(batch)
            logger.info("Upserted batch: %s/%s rows", total_rows, len(df))

        logger.info("Incremental load complete: %s rows upserted into %s", total_rows, full_table)
        return total_rows

    def load_scd_type2(self, df: pd.DataFrame, table_name: str, natural_key: str, schema: str = None) -> int:
        """Slowly Changing Dimension Type 2 load."""
        target_schema = schema or self.schema
        full_table = f"{target_schema}.{table_name}"

        if df.empty:
            return 0

        df = df.copy()
        df["_scd_valid_from"] = datetime.utcnow()
        df["_scd_valid_to"] = None
        df["_scd_is_current"] = True
        df["_surrogate_key"] = [uuid.uuid4().hex for _ in range(len(df))]

        with self.engine.connect() as conn:
            natural_keys = df[natural_key].unique().tolist()
            expire_sql = f"""
                UPDATE {full_table}
                SET _scd_valid_to = NOW(),
                    _scd_is_current = FALSE
                WHERE {natural_key} = ANY(:keys)
                AND _scd_is_current = TRUE
            """
            conn.execute(text(expire_sql), {"keys": natural_keys})
            conn.commit()

        df.to_sql(
            name=table_name,
            con=self.engine,
            schema=target_schema,
            if_exists="append",
            index=False,
            chunksize=5000,
        )

        logger.info("SCD Type 2 load: %s new versions into %s", len(df), full_table)
        return len(df)

    def _ensure_table_exists(self, df: pd.DataFrame, table_name: str, schema: str):
        """Create table if it doesn't exist."""
        with self.engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            conn.commit()

        empty_df = df.head(0)
        empty_df.to_sql(
            name=table_name,
            con=self.engine,
            schema=schema,
            if_exists="fail" if not self._table_exists(table_name, schema) else "append",
            index=False,
        )

    def _table_exists(self, table_name: str, schema: str) -> bool:
        """Check if a table exists."""
        with self.engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT EXISTS ("
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = :schema AND table_name = :table"
                    ")"
                ),
                {"schema": schema, "table": table_name},
            )
            return result.scalar()

    def update_watermark(self, pipeline_name: str, table_name: str, watermark_value: str):
        """Update the pipeline watermark for tracking incremental loads."""
        with self.engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS metadata"))
            conn.execute(
                text("""
                    INSERT INTO metadata.watermarks (pipeline_name, table_name, watermark_value, updated_at)
                    VALUES (:pipeline, :table, :watermark, NOW())
                    ON CONFLICT (pipeline_name, table_name)
                    DO UPDATE SET watermark_value = EXCLUDED.watermark_value, updated_at = NOW()
                """),
                {
                    "pipeline": pipeline_name,
                    "table": table_name,
                    "watermark": watermark_value,
                },
            )
            conn.commit()

    def get_watermark(self, pipeline_name: str, table_name: str) -> Optional[str]:
        """Get the last watermark value for a pipeline/table."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT watermark_value FROM metadata.watermarks
                        WHERE pipeline_name = :pipeline AND table_name = :table
                    """),
                    {"pipeline": pipeline_name, "table": table_name},
                )
                row = result.fetchone()
                return row[0] if row else None
        except Exception:
            return None
