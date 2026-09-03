"""
Main entry point for the ingestion service.
Orchestrates data extraction from all sources and loading to S3/warehouse.
"""
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import config
from src.extractors.api_extractor import APIExtractor
from src.extractors.csv_extractor import CSVExtractor
from src.loaders.s3_loader import S3Loader
from src.loaders.warehouse_loader import WarehouseLoader
from src.validators.schema_validator import SchemaValidator, DataQualityChecker

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


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
    logger.info(f"  Orders: {len(all_orders)} records")
    
    all_products = []
    for batch in extractor.extract_with_offset_pagination("/products", page_size=500):
        all_products.extend(batch)
    logger.info(f"  Products: {len(all_products)} records")
    
    all_customers = []
    for batch in extractor.extract_with_offset_pagination("/customers", page_size=500):
        all_customers.extend(batch)
    logger.info(f"  Customers: {len(all_customers)} records")

    # ─── Validate ───
    logger.info("\n🔍 Validating data...")
    validator = SchemaValidator()
    checker = DataQualityChecker()
    
    orders_df = pd.DataFrame(all_orders)
    products_df = pd.DataFrame(all_products)
    customers_df = pd.DataFrame(all_customers)
    
    # Quality checks
    checker.check_row_count(orders_df, min_rows=1)
    checker.check_row_count(products_df, min_rows=1)
    checker.check_row_count(customers_df, min_rows=1)
    
    if "order_id" in orders_df.columns:
        checker.check_unique(orders_df, ["order_id"])
    if "product_id" in products_df.columns:
        checker.check_unique(products_df, ["product_id"])
    
    summary = checker.get_summary()
    logger.info(f"  Quality: {summary['passed']}/{summary['total_checks']} checks passed")

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
            logger.info(f"  {entity}: {len(df)} rows → s3://{config.s3_bucket}/{key}")

    # ─── Load to Warehouse ───
    logger.info("\n🏗️ Loading to Warehouse...")
    try:
        wh_loader = WarehouseLoader(schema="raw")
        
        for entity, df in [("orders", orders_df), ("products", products_df), ("customers", customers_df)]:
            if not df.empty:
                rows = wh_loader.load_full_refresh(df=df, table_name=entity, schema="raw")
                logger.info(f"  {entity}: {rows} rows loaded to raw.{entity}")
    except Exception as e:
        logger.warning(f"  Warehouse load skipped (not available): {e}")

    logger.info("\n" + "=" * 60)
    logger.info("✅ Ingestion pipeline complete")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_full_ingestion()
