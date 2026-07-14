"""DAG Silver — Vaccination PNI."""

from __future__ import annotations

from datetime import datetime, timedelta

from _common.spark_k8s import make_spark_operator
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dag_silver_vacinacao_pni",
    schedule_interval="30 7 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "vacinacao", "pni"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:
    year_month = "{{ data_interval_start.strftime('%Y%m') }}"
    submit, sensor = make_spark_operator(
        task_id="silver_vacinacao_pni_spark",
        template_name="silver-vacinacao-pni.yaml",
        substitutions={"ANO_MES": year_month},
        dag=dag,
    )

    def _emit_lineage(**context):
        from pipelines.common.lineage import Dataset, emit_complete, emit_start

        run_id = emit_start(
            job_name="dag_silver_vacinacao_pni",
            inputs=[Dataset.s3(f"s3://bronze/vacinacao_pni/{year_month}/")],
            outputs=[Dataset.s3("s3://silver/vacinacao_pni/")],
        )
        if run_id:
            emit_complete(job_name="dag_silver_vacinacao_pni", run_id=run_id)

    emit_lineage = PythonOperator(
        task_id="emit_lineage",
        python_callable=_emit_lineage,
        trigger_rule="all_success",
    )

    submit >> sensor >> emit_lineage
