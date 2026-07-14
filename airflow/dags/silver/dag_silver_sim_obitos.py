"""DAG Silver — SIM Óbitos."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG

from _common.spark_k8s import make_spark_operator

with DAG(
    dag_id="dag_silver_sim_obitos",
    schedule_interval="30 8 1 1 *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "sim", "obitos"],
    default_args={"retries": 2, "retry_delay": __import__("datetime").timedelta(minutes=5)},
) as dag:

    ano = "{{ (data_interval_start.year - 1) | string }}"
    make_spark_operator(
        task_id="silver_sim_obitos_spark",
        template_name="silver-sim-obitos.yaml",
        substitutions={"ANO": ano},
        dag=dag,
    )
