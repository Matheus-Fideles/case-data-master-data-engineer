"""DAG Silver — Estabelecimento (CNES)."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG

from _common.spark_k8s import make_spark_operator

with DAG(
    dag_id="dag_silver_estabelecimento",
    schedule_interval="30 5 * * 1",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "cnes", "estabelecimento"],
    default_args={"retries": 2, "retry_delay": __import__("datetime").timedelta(minutes=5)},
) as dag:

    snapshot_date = "{{ data_interval_start.strftime('%Y%m%d') }}"
    make_spark_operator(
        task_id="silver_estabelecimento_spark",
        template_name="silver-estabelecimento.yaml",
        substitutions={"SNAPSHOT_DATE": snapshot_date},
        dag=dag,
    )
