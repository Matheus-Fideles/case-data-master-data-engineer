"""DAG Silver — Vacinação PNI."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG

from _common.spark_k8s import make_spark_operator

with DAG(
    dag_id="dag_silver_vacinacao_pni",
    schedule_interval="30 7 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "vacinacao", "pni"],
    default_args={"retries": 2, "retry_delay": __import__("datetime").timedelta(minutes=5)},
) as dag:

    ano_mes = "{{ data_interval_start.strftime('%Y%m') }}"
    make_spark_operator(
        task_id="silver_vacinacao_pni_spark",
        template_name="silver-vacinacao-pni.yaml",
        substitutions={"ANO_MES": ano_mes},
        dag=dag,
    )
