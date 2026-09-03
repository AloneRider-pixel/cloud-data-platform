"""Common Airflow utilities shared across DAGs."""
from datetime import datetime, timedelta


# Default DAG arguments
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    "start_date": datetime(2024, 1, 1),
}


def get_s3_path(layer: str, entity: str, partition: str = None) -> str:
    """Generate standardized S3 path."""
    base = f"s3://raw-data-lake/{layer}/{entity}/"
    if partition:
        return f"{base}{partition}/"
    return base


def format_dag_tags(domain: str, pipeline_type: str) -> list:
    """Generate consistent DAG tags."""
    return [
        f"domain:{domain}",
        f"type:{pipeline_type}",
        "team:data-engineering",
        f"env:production",
    ]
