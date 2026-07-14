"""DAG Bronze — Chikungunya.

Extrai notificações de chikungunya e persiste em Delta Lake (bronze/arboviroses/chikungunya/).
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from _common.spark_k8s import make_spark_operator

with DAG(
    dag_id="dag_bronze_chikungunya",
    schedule_interval="0 6 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "arboviroses", "chikungunya"],
    default_args={"retries": 2, "retry_delay": __import__("datetime").timedelta(minutes=5)},
) as dag:

    def _extract(**context):
        from pipelines.extraction.arboviroses import run

        ano = context["data_interval_start"].year
        result = run(agravo="chikungunya", ano=ano)
        context["ti"].xcom_push(key="extraction_result", value=result)

    extract = PythonOperator(
        task_id="extract_chikungunya",
        python_callable=_extract,
        pool="extraction_pool",
    )

    ano_mes = "{{ data_interval_start.strftime('%Y%m') }}"
    submit, sensor = make_spark_operator(
        task_id="bronze_chikungunya_spark",
        template_name="bronze-chikungunya.yaml",
        substitutions={"ANO_MES": ano_mes},
        dag=dag,
    )

    extract >> submit
