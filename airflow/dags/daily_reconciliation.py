from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "vishal",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="daily_reconciliation",
    default_args=default_args,
    description="Daily batch aggregation and reconciliation of streaming vs batch transaction metrics",
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 29),
    catchup=False,
    tags=["reconciliation", "spark", "iceberg"],
) as dag:
    run_batch_aggregation = BashOperator(
        task_id="run_batch_aggregation",
        bash_command='echo "Running batch aggregation: reading bronze, computing daily per-merchant aggregates, writing to transactions_batch_agg"',
    )

    run_reconciliation = BashOperator(
        task_id="run_reconciliation",
        bash_command='echo "Running reconciliation: joining stream and batch aggregates, computing drift, writing reconciliation_report"',
    )

    run_batch_aggregation >> run_reconciliation