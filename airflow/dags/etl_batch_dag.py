"""
Batch ETL Pipeline DAG.
Full-refresh pipeline for the core e-commerce entities:
Orders → Products → Customers → Revenue Analytics

Flow:
  Extract → Validate → Load to S3 (Raw) → Transform (dbt) → Quality Check → Notify
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from common.utils import DEFAULT_ARGS, format_dag_tags


def extract_orders(**context):
    """Extract orders from source database/API."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags/../../ingestion")
    from src.extractors.api_extractor import APIExtractor
    
    extractor = APIExtractor()
    all_records = []
    
    for batch in extractor.extract_with_offset_pagination(
        endpoint="/orders",
        page_size=500,
        data_key="data",
    ):
        all_records.extend(batch)
    
    context["ti"].xcom_push(key="extracted_count", value=len(all_records))
    
    # Save to S3
    import pandas as pd
    df = pd.DataFrame(all_records)
    
    from src.loaders.s3_loader import S3Loader
    loader = S3Loader()
    s3_key = loader.load_dataframe(
        df=df,
        prefix="raw/orders/",
        partition_column="created_at" if "created_at" in df.columns else None,
        metadata={"entity": "orders", "load_type": "batch"},
    )
    
    context["ti"].xcom_push(key="s3_key", value=s3_key)
    return {"rows": len(all_records), "s3_key": s3_key}


def extract_products(**context):
    """Extract products from source."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags/../../ingestion")
    from src.extractors.api_extractor import APIExtractor
    
    extractor = APIExtractor()
    all_records = []
    
    for batch in extractor.extract_with_offset_pagination(
        endpoint="/products",
        page_size=500,
    ):
        all_records.extend(batch)
    
    import pandas as pd
    df = pd.DataFrame(all_records)
    
    from src.loaders.s3_loader import S3Loader
    loader = S3Loader()
    s3_key = loader.load_dataframe(
        df=df,
        prefix="raw/products/",
        metadata={"entity": "products", "load_type": "batch"},
    )
    
    context["ti"].xcom_push(key="extracted_count", value=len(all_records))
    return {"rows": len(all_records), "s3_key": s3_key}


def extract_customers(**context):
    """Extract customers from source."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags/../../ingestion")
    from src.extractors.api_extractor import APIExtractor
    
    extractor = APIExtractor()
    all_records = []
    
    for batch in extractor.extract_with_offset_pagination(
        endpoint="/customers",
        page_size=500,
    ):
        all_records.extend(batch)
    
    import pandas as pd
    df = pd.DataFrame(all_records)
    
    from src.loaders.s3_loader import S3Loader
    loader = S3Loader()
    s3_key = loader.load_dataframe(
        df=df,
        prefix="raw/customers/",
        metadata={"entity": "customers", "load_type": "batch"},
    )
    
    context["ti"].xcom_push(key="extracted_count", value=len(all_records))
    return {"rows": len(all_records), "s3_key": s3_key}


def validate_extraction(**context):
    """Validate extracted data meets quality thresholds."""
    ti = context["ti"]
    
    orders_count = ti.xcom_pull(task_ids="extract_orders", key="extracted_count")
    products_count = ti.xcom_pull(task_ids="extract_products", key="extracted_count")
    customers_count = ti.xcom_pull(task_ids="extract_customers", key="extracted_count")
    
    # Minimum thresholds
    assert orders_count and orders_count > 0, f"No orders extracted (count={orders_count})"
    assert products_count and products_count > 0, f"No products extracted (count={products_count})"
    
    total = (orders_count or 0) + (products_count or 0) + (customers_count or 0)
    
    context["ti"].xcom_push(key="validation", value={
        "orders": orders_count,
        "products": products_count,
        "customers": customers_count,
        "total": total,
        "status": "passed",
    })
    
    return {"total_extracted": total, "status": "passed"}


# ─── DAG Definition ───

with DAG(
    dag_id="etl_batch_pipeline",
    default_args=DEFAULT_ARGS,
    description="Batch ETL pipeline: Extract → Validate → Load → Transform → Quality",
    schedule_interval="@daily",
    catchup=False,
    tags=format_dag_tags("ecommerce", "batch"),
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")
    
    # ─── Extract Phase ───
    extract_orders_task = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_orders,
    )
    
    extract_products_task = PythonOperator(
        task_id="extract_products",
        python_callable=extract_products,
    )
    
    extract_customers_task = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_customers,
    )
    
    # ─── Validate Phase ───
    validate_task = PythonOperator(
        task_id="validate_extraction",
        python_callable=validate_extraction,
        trigger_rule="all_success",
    )
    
    # ─── Transform Phase (dbt) ───
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/dags/../../dbt && dbt run --models staging --profiles-dir /root/.dbt",
    )
    
    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command="cd /opt/airflow/dags/../../dbt && dbt run --models intermediate --profiles-dir /root/.dbt",
    )
    
    dbt_run_analytics = BashOperator(
        task_id="dbt_run_analytics",
        bash_command="cd /opt/airflow/dags/../../dbt && dbt run --models analytics --profiles-dir /root/.dbt",
    )
    
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dags/../../dbt && dbt test --profiles-dir /root/.dbt",
        retries=2,
    )
    
    # ─── Data Quality Phase ───
    dq_check = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id="data_quality_checks",
        wait_for_completion=False,
    )
    
    end = EmptyOperator(task_id="end")
    
    # ─── Dependencies ───
    start >> [extract_orders_task, extract_products_task, extract_customers_task]
    [extract_orders_task, extract_products_task, extract_customers_task] >> validate_task
    validate_task >> dbt_run_staging
    dbt_run_staging >> dbt_run_intermediate
    dbt_run_intermediate >> dbt_run_analytics
    dbt_run_analytics >> dbt_test
    dbt_test >> dq_check
    dq_check >> end
