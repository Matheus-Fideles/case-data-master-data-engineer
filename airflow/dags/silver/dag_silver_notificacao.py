"""DAG Silver — Notification (arboviruses).

Processes dengue, zika and chikungunya from Bronze to Silver in parallel.
Each disease type is an independent SparkApplication in k8s.
Upstream dependency: dag_bronze_dengue, dag_bronze_zika, dag_bronze_chikungunya.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from _common.spark_k8s import make_spark_operator

with DAG(
    dag_id="dag_silver_notificacao",
    schedule_interval="30 6 1 * *",  # 30 min after Bronze arboviruses DAGs
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "notificacao", "arboviroses"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:

    year_month = "{{ data_interval_start.strftime('%Y%m') }}"

    disease_types = ("dengue", "zika", "chikungunya")

    for disease in disease_types:
        submit, sensor = make_spark_operator(
            task_id=f"silver_notificacao_{disease}",
            template_name="silver-notificacao.yaml",
            substitutions={"AGRAVO": disease, "ANO_MES": year_month},
            dag=dag,
        )

        def _emit_lineage(disease=disease, **context):
            from pipelines.common.lineage import Dataset, emit_complete, emit_start

            run_id = emit_start(
                job_name=f"dag_silver_notificacao.{disease}",
                inputs=[Dataset.s3(f"s3://bronze/arboviroses/{disease}/{year_month}/")],
                outputs=[Dataset.s3(f"s3://silver/notificacao/{disease}/")],
            )
            if run_id:
                emit_complete(job_name=f"dag_silver_notificacao.{disease}", run_id=run_id)

        emit_lineage = PythonOperator(
            task_id=f"emit_lineage_{disease}",
            python_callable=_emit_lineage,
            trigger_rule="all_success",
        )

        submit >> sensor >> emit_lineage
