"""DAG Bronze — CNES (Estabelecimentos de Saúde).

Snapshot diário dos estabelecimentos de saúde do CNES, persistido em
Delta Lake (bronze/cnes/), particionado por snapshot_date.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from _common.spark_k8s import make_spark_operator

with DAG(
    dag_id="dag_bronze_cnes",
    schedule_interval="0 5 * * 1",  # toda segunda-feira
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "cnes", "estabelecimentos"],
    default_args={"retries": 2, "retry_delay": __import__("datetime").timedelta(minutes=5)},
) as dag:

    def _extract(**context):
        from pipelines.extraction.cnes import run

        snapshot_date = context["data_interval_start"].strftime("%Y%m%d")
        result = run(snapshot_date=snapshot_date)
        context["ti"].xcom_push(key="extraction_result", value=result)

    extract = PythonOperator(
        task_id="extract_cnes",
        python_callable=_extract,
        pool="extraction_pool",
    )

    snapshot_date = "{{ data_interval_start.strftime('%Y%m%d') }}"
    submit, sensor = make_spark_operator(
        task_id="bronze_cnes_spark",
        template_name="bronze-cnes.yaml",
        substitutions={"SNAPSHOT_DATE": snapshot_date},
        dag=dag,
    )

    extract >> submit
