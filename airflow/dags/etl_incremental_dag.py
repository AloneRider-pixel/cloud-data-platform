"""
Incremental ETL Pipeline DAG.
Watermark-based delta processing for large datasets.
Only processes new/changed records since last run.

Flow:
  Check Watermark → Extract Delta → Validate → Load to S3 → Load to Warehouse → Update Watermark
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from common.utils import DEFAULT_ARGS, format_dag_tags


def check_watermark(**context):
    """Check the last watermark to determine extraction range."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags/../../ingestion")
    from src.loaders.warehouse_loader import WarehouseLoader
    
    loader = WarehouseLoader()
    last_watermark = loader.get_watermark(
        pipeline_name="incremental_orders",
        table_name="orders",
    )
    
    if not last_watermark:
        # First run - look back 7 days
        last_watermark = (datetime.utcnow() - timedelta(days=7)).isoformat()
    
    current_watermark = datetime.utcnow().isoformat()
    
    context["ti"].xcom_push(key="start_watermark", value=last_watermark)
    context["ti"].xcom_push(key="end_watermark", value=current_watermark)
    
    return {
        "start_watermark": last_watermark,
        "end_watermark": current_watermark,
    }


def extract_incremental_orders(**context):
    """Extract only orders newer than the watermark."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags/../../ingestion")
    from src.extractors.api_extractor import APIExtractor
    
    ti = context["ti"]
    start_wm = ti.xcom_pull(task_ids="check_watermark", key="start_watermark")
    end_wm = ti.xcom_pull(task_ids="check_watermark", key="end_watermark")
    
    extractor = APIExtractor()
    all_records = []
    
    for batch in extractor.extract_with_date_filter(
        endpoint="/orders",
        start_date=datetime.fromisoformat(start_wm.replace("Z", "+00:00")),
        end_date=datetime.fromisoformat(end_wm.replace("Z", "+00:00")),
        page_size=1000,
    ):
        all_records.extend(batch)
    
    context["ti"].xcom_push(key="extracted_count", value=len(all_records))
    
    if not all_records:
        return {"rows": 0, "status": "no_new_data"}
    
    # Load to S3 as incremental
    import pandas as pd
    df = pd.DataFrame(all_records)
    
    from src.loaders.s3_loader import S3Loader
    loader = S3Loader()
    s3_key = loader.load_incremental(
        df=df,
        prefix="raw/orders/",
        watermark_column="created_at" if "created_at" in df.columns else "updated_at",
    )
    
    return {"rows": len(all_records), "s3_key": s3_key, "status": "loaded"}


def load_to_warehouse(**context):
    """Load incremental data to PostgreSQL warehouse."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags/../../ingestion")
    from src.loaders.warehouse_loader import WarehouseLoader
    
    ti = context["ti"]
    extracted = ti.xcom_pull(task_ids="extract_incremental_orders", key="extracted_count")
    
    if not extracted or extracted == 0:
        return {"status": "skipped", "reason": "no_new_data"}
    
    # Read back from S3 and load to warehouse
    from src.loaders.s3_loader import S3Loader
    s3_loader = S3Loader()
    files = s3_loader.list_files(prefix="raw/orders/", max_keys=1)
    
    if not files:
        return {"status": "skipped", "reason": "no_s3_files"}
    
    df = s3_loader.read_parquet(files[0]["key"])
    
    # Upsert to warehouse
    wh_loader = WarehouseLoader()
    row_count = wh_loader.load_incremental(
        df=df,
        table_name="orders",
        primary_key="order_id",
        schema="raw",
    )
    
    return {"rows_loaded": row_count, "status": "loaded"}


def update_watermark(**context):
    """Update the pipeline watermark after successful load."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags/../../ingestion")
    from src.loaders.warehouse_loader import WarehouseLoader
    
    ti = context["ti"]
    end_wm = ti.xcom_pull(task_ids="check_watermark", key="end_watermark")
    
    loader = WarehouseLoader()
    loader.update_watermark(
        pipeline_name="incremental_orders",
        table_name="orders",
        watermark_value=end_wm,
    )
    
    return {"new_watermark": end_wm, "status": "updated"}


# ─── DAG Definition ───

with DAG(
    dag_id="etl_incremental_pipeline",
    default_args=DEFAULT_ARGS,
    description="Incremental ETL: Watermark-based delta processing for large datasets",
    schedule_interval="*/30 * * * *",  # Every 30 minutes
    catchup=False,
    tags=format_dag_tags("ecommerce", "incremental"),
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")
    
    check_wm = PythonOperator(
        task_id="check_watermark",
        python_callable=check_watermark,
    )
    
    extract = PythonOperator(
        task_id="extract_incremental_orders",
        python_callable=extract_incremental_orders,
    )
    
    load_wh = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_to_warehouse,
    )
    
    update_wm = PythonOperator(
        task_id="update_watermark",
        python_callable=update_watermark,
    )
    
    end = EmptyOperator(task_id="end")
    
    # Dependencies
    start >> check_wm >> extract >> load_wh >> update_wm >> end
