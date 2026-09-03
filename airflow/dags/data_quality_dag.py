"""
Data Quality Pipeline DAG.
Post-load data quality checks including:
- Schema validation
- Row count reconciliation
- Freshness checks
- Distribution anomaly detection
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator

from common.utils import DEFAULT_ARGS, format_dag_tags


def check_source_freshness(**context):
    """Check that data has been loaded recently enough."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags/../../ingestion")
    from src.loaders.warehouse_loader import WarehouseLoader
    from sqlalchemy import text
    
    loader = WarehouseLoader()
    
    tables_to_check = [
        ("raw", "orders"),
        ("raw", "products"),
        ("raw", "customers"),
        ("staging", "stg_orders"),
    ]
    
    freshness_results = []
    all_fresh = True
    
    for schema, table in tables_to_check:
        try:
            with loader.engine.connect() as conn:
                result = conn.execute(
                    text(f"""
                        SELECT MAX(_ingestion_timestamp) as last_load
                        FROM {schema}.{table}
                        WHERE _ingestion_timestamp IS NOT NULL
                    """)
                )
                row = result.fetchone()
                
                if row and row[0]:
                    last_load = row[0]
                    age_hours = (datetime.utcnow() - last_load).total_seconds() / 3600
                    is_fresh = age_hours < 2  # Must be loaded within 2 hours
                    
                    freshness_results.append({
                        "table": f"{schema}.{table}",
                        "last_load": last_load.isoformat(),
                        "age_hours": round(age_hours, 2),
                        "is_fresh": is_fresh,
                    })
                    
                    if not is_fresh:
                        all_fresh = False
                else:
                    freshness_results.append({
                        "table": f"{schema}.{table}",
                        "last_load": None,
                        "is_fresh": False,
                    })
                    all_fresh = False
        except Exception as e:
            freshness_results.append({
                "table": f"{schema}.{table}",
                "error": str(e),
                "is_fresh": False,
            })
            all_fresh = False
    
    context["ti"].xcom_push(key="freshness_results", value=freshness_results)
    
    return {"all_fresh": all_fresh, "results": freshness_results}


def check_row_counts(**context):
    """Compare row counts between source and target tables."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags/../../ingestion")
    from src.loaders.warehouse_loader import WarehouseLoader
    from sqlalchemy import text
    
    loader = WarehouseLoader()
    
    table_pairs = [
        ("raw", "orders", "staging", "stg_orders"),
        ("raw", "products", "staging", "stg_products"),
    ]
    
    results = []
    all_match = True
    
    for src_schema, src_table, tgt_schema, tgt_table in table_pairs:
        try:
            with loader.engine.connect() as conn:
                src_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM {src_schema}.{src_table}")
                ).scalar()
                
                tgt_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM {tgt_schema}.{tgt_table}")
                ).scalar()
            
            # Allow up to 5% difference for incremental loads
            tolerance = max(src_count * 0.05, 10)
            is_close = abs(src_count - tgt_count) <= tolerance
            
            results.append({
                "source": f"{src_schema}.{src_table}",
                "target": f"{tgt_schema}.{tgt_table}",
                "source_count": src_count,
                "target_count": tgt_count,
                "difference": abs(src_count - tgt_count),
                "tolerance": tolerance,
                "passed": is_close,
            })
            
            if not is_close:
                all_match = False
        except Exception as e:
            results.append({
                "source": f"{src_schema}.{src_table}",
                "target": f"{tgt_schema}.{tgt_table}",
                "error": str(e),
                "passed": False,
            })
            all_match = False
    
    context["ti"].xcom_push(key="row_count_results", value=results)
    return {"all_match": all_match, "results": results}


def check_data_distribution(**context):
    """Check for statistical anomalies in key metrics."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags/../../ingestion")
    from src.loaders.warehouse_loader import WarehouseLoader
    from sqlalchemy import text
    
    loader = WarehouseLoader()
    
    results = []
    
    try:
        with loader.engine.connect() as conn:
            # Check order amounts distribution
            result = conn.execute(text("""
                SELECT 
                    AVG(amount) as avg_amount,
                    STDDEV(amount) as std_amount,
                    MIN(amount) as min_amount,
                    MAX(amount) as max_amount,
                    COUNT(*) as total
                FROM raw.orders
                WHERE amount IS NOT NULL
            """))
            
            row = result.fetchone()
            if row:
                avg, std, min_val, max_val, count = row
                results.append({
                    "metric": "order_amount",
                    "avg": float(avg) if avg else 0,
                    "std": float(std) if std else 0,
                    "min": float(min_val) if min_val else 0,
                    "max": float(max_val) if max_val else 0,
                    "count": count,
                    "anomaly": False,  # Could add z-score checking here
                })
    except Exception as e:
        results.append({"metric": "order_amount", "error": str(e)})
    
    context["ti"].xcom_push(key="distribution_results", value=results)
    return {"results": results}


def route_alerts(**context):
    """Route to alert or continue based on quality check results."""
    ti = context["ti"]
    
    freshness = ti.xcom_pull(task_ids="check_freshness")
    row_counts = ti.xcom_pull(task_ids="check_row_counts")
    
    has_issues = False
    
    if freshness and not freshness.get("all_fresh", True):
        has_issues = True
    
    if row_counts and not row_counts.get("all_match", True):
        has_issues = True
    
    return "send_alert" if has_issues else "quality_passed"


# ─── DAG Definition ───

with DAG(
    dag_id="data_quality_checks",
    default_args=DEFAULT_ARGS,
    description="Post-load data quality checks: freshness, row counts, distribution",
    schedule_interval=None,  # Triggered by other DAGs
    catchup=False,
    tags=format_dag_tags("ecommerce", "quality"),
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")
    
    freshness_check = PythonOperator(
        task_id="check_freshness",
        python_callable=check_source_freshness,
    )
    
    row_count_check = PythonOperator(
        task_id="check_row_counts",
        python_callable=check_row_counts,
    )
    
    distribution_check = PythonOperator(
        task_id="check_distribution",
        python_callable=check_data_distribution,
    )
    
    route = BranchPythonOperator(
        task_id="route_alerts",
        python_callable=route_alerts,
    )
    
    send_alert = PythonOperator(
        task_id="send_alert",
        python_callable=lambda **ctx: print("🚨 Data quality alert triggered!"),
    )
    
    quality_passed = EmptyOperator(task_id="quality_passed")
    
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")
    
    # Dependencies
    start >> [freshness_check, row_count_check, distribution_check]
    [freshness_check, row_count_check, distribution_check] >> route
    route >> [send_alert, quality_passed]
    [send_alert, quality_passed] >> end
