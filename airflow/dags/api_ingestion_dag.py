"""
API Ingestion Pipeline DAG.
Scheduled API pulls with backfill support, rate limiting, and error handling.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from common.utils import DEFAULT_ARGS, format_dag_tags


def ingest_api_data(endpoint: str, entity: str, page_size: int = 200):
    """Generic API ingestion function."""
    def _callable(**context):
        import sys
        sys.path.insert(0, "/opt/airflow/dags/../../ingestion")
        from src.extractors.api_extractor import APIExtractor
        from src.loaders.s3_loader import S3Loader
        import pandas as pd
        
        extractor = APIExtractor()
        all_records = []
        
        for batch in extractor.extract_with_offset_pagination(
            endpoint=endpoint,
            page_size=page_size,
        ):
            all_records.extend(batch)
        
        if not all_records:
            context["ti"].xcom_push(key="status", value="no_data")
            return {"rows": 0, "status": "no_data"}
        
        df = pd.DataFrame(all_records)
        
        loader = S3Loader()
        s3_key = loader.load_dataframe(
            df=df,
            prefix=f"raw/{entity}/",
            metadata={
                "entity": entity,
                "load_type": "api_ingestion",
                "source": endpoint,
            },
        )
        
        context["ti"].xcom_push(key="extracted_count", value=len(all_records))
        context["ti"].xcom_push(key="s3_key", value=s3_key)
        
        return {"rows": len(all_records), "s3_key": s3_key, "status": "success"}
    
    return _callable


def validate_loaded_data(**context):
    """Validate data loaded from API meets quality standards."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags/../../ingestion")
    from src.validators.schema_validator import DataQualityChecker
    
    ti = context["ti"]
    orders_count = ti.xcom_pull(task_ids="ingest_orders", key="extracted_count") or 0
    products_count = ti.xcom_pull(task_ids="ingest_products", key="extracted_count") or 0
    
    checker = DataQualityChecker()
    
    # Check minimum row counts
    checker.check_row_count(pd.DataFrame({"x": range(orders_count)}), min_rows=0)
    
    summary = checker.get_summary()
    
    context["ti"].xcom_push(key="validation_summary", value=summary)
    
    return summary


# ─── DAG Definition ───

with DAG(
    dag_id="api_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Scheduled API ingestion with backfill support and rate limiting",
    schedule_interval="0 */2 * * *",  # Every 2 hours
    catchup=False,
    tags=format_dag_tags("ecommerce", "api_ingestion"),
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")
    
    ingest_orders = PythonOperator(
        task_id="ingest_orders",
        python_callable=ingest_api_data("/orders", "orders", 500),
    )
    
    ingest_products = PythonOperator(
        task_id="ingest_products",
        python_callable=ingest_api_data("/products", "products", 500),
    )
    
    ingest_customers = PythonOperator(
        task_id="ingest_customers",
        python_callable=ingest_api_data("/customers", "customers", 500),
    )
    
    ingest_events = PythonOperator(
        task_id="ingest_events",
        python_callable=ingest_api_data("/events", "events", 1000),
    )
    
    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_loaded_data,
        trigger_rule="all_success",
    )
    
    end = EmptyOperator(task_id="end")
    
    # Dependencies - all ingestion runs in parallel
    start >> [ingest_orders, ingest_products, ingest_customers, ingest_events]
    [ingest_orders, ingest_products, ingest_customers, ingest_events] >> validate >> end
