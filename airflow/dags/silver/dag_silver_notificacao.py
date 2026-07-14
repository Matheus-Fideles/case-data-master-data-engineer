"""DAG Silver — Notificação (arboviroses).

Processa dengue, zika e chikungunya da Bronze para Silver em paralelo.
Cada agravo é um SparkApplication independente no k8s.
Depende upstream dos DAGs dag_bronze_dengue, dag_bronze_zika, dag_bronze_chikungunya.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG

from _common.spark_k8s import make_spark_operator

with DAG(
    dag_id="dag_silver_notificacao",
    schedule_interval="30 6 1 * *",  # 30 min após os DAGs Bronze de arboviroses
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "notificacao", "arboviroses"],
    default_args={"retries": 2, "retry_delay": __import__("datetime").timedelta(minutes=5)},
) as dag:

    ano_mes = "{{ data_interval_start.strftime('%Y%m') }}"

    for agravo in ("dengue", "zika", "chikungunya"):
        make_spark_operator(
            task_id=f"silver_notificacao_{agravo}",
            template_name="silver-notificacao.yaml",
            substitutions={"AGRAVO": agravo, "ANO_MES": ano_mes},
            dag=dag,
        )
