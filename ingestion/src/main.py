"""
Main entry point for the ingestion service.
Orchestrates data extraction from all sources and loading to S3/warehouse.
"""
import logging

import pandas as pd

from src.config import config
from src.extractors.api_extractor import APIExtractor
from src.loaders.s3_loader import S3Loader
from src.loaders.warehouse_loader import WarehouseLoader
from src.validators.schema_validator import DataQualityChecker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def enforce_quality_gate(checks: list[tuple[str, bool]]) -> None:
    """Raise before downstream writes when any required validation fails."""
    failed_checks = [name for name, passed in checks if not passed]
    if failed_checks:
        raise RuntimeError(
            "Data quality gate failed; downstream writes were blocked: "
            + ", ".join(failed_checks)
        )

def run_full_ingestion():
    """Run complete ingestion pipeline for all entities."""
    logger.info("=" * 60)
    logger.info("Starting full ingestion pipeline")
    logger.info("=" * 60)

    # ─── Extract from API ───
    logger.info("\n📡 Extracting from API...")
    extractor = APIExtractor()

    all_orders = []
    for batch in extractor.extract_with_offset_pagination("/orders", page_size=500):
        all_orders.extend(batch)
    logger.info("  Orders: %s records", len(all_orders))

    all_products = []
    for batch in extractor.extract_with_offset_pagination("/products", page_size=500):
        all_products.extend(batch)
    logger.info("  Products: %s records", len(all_products))

    all_customers = []
    for batch in extractor.extract_with_offset_pagination("/customers", page_size=500):
        all_customers.extend(batch)
    logger.info("  Customers: %s records", len(all_customers))

    # ─── Validate ───
    logger.info("\n🔍 Validating data...")
    checker = DataQualityChecker()

    orders_df = pd.DataFrame(all_orders)
    products_df = pd.DataFrame(all_products)
    customers_df = pd.DataFrame(all_customers)

    # Quality checks are a hard gate: a failed validation must stop the pipeline
    # before data is written downstream.
    checks = [
        ("orders row count", checker.check_row_count(orders_df, min_rows=1)),
        ("products row count", checker.check_row_count(products_df, min_rows=1)),
        ("customers row count", checker.check_row_count(customers_df, min_rows=1)),
    ]

    if "order_id" in orders_df.columns:
        checks.append(("orders primary-key uniqueness", checker.check_unique(orders_df, ["order_id"])))
    if "product_id" in products_df.columns:
        checks.append(("products primary-key uniqueness", checker.check_unique(products_df, ["product_id"])))

    summary = checker.get_summary()
    logger.info("  Quality gate: %s/%s checks passed", summary["passed"], summary["total_checks"])

    failed_checks = [name for name, passed in checks if not passed]
    if failed_checks:
        raise RuntimeError(
            "Data quality gate failed; downstream writes were blocked: "
            + ", ".join(failed_checks)
        )

    # ─── Load to S3 ───
    logger.info("\n📦 Loading to S3 (Raw Layer)...")
    s3_loader = S3Loader()

    for entity, df in [("orders", orders_df), ("products", products_df), ("customers", customers_df)]:
        if not df.empty:
            key = s3_loader.load_dataframe(
                df=df,
                prefix=f"raw/{entity}/",
                metadata={"entity": entity, "load_type": "full_refresh"},
            )
            logger.info("  %s: %s rows → s3://%s/%s", entity, len(df), config.s3_bucket, key)

    # ─── Load to Warehouse ───
    logger.info("\n🏗️ Loading to Warehouse...")
    try:
        wh_loader = WarehouseLoader(schema="raw")

        for entity, df in [("orders", orders_df), ("products", products_df), ("customers", customers_df)]:
            if not df.empty:
                rows = wh_loader.load_full_refresh(df=df, table_name=entity, schema="raw")
                logger.info("  %s: %s rows loaded to raw.%s", entity, rows, entity)
    except Exception as exc:
        logger.warning("  Warehouse load skipped (not available): %s", exc)

    logger.info("\n" + "=" * 60)
    logger.info("✅ Ingestion pipeline complete")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_full_ingestion()
