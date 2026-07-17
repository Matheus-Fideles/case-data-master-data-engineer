"""DAG Silver — Municipality."""

from __future__ import annotations

from datetime import datetime, timedelta

from _common.spark_k8s import make_spark_operator
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dag_silver_municipio",
    schedule_interval="30 4 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "municipio"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:
    snapshot_date = "{{ data_interval_start.strftime('%Y%m%d') }}"
    submit, sensor = make_spark_operator(
        task_id="silver_municipio_spark",
        template_name="silver-municipio.yaml",
        substitutions={"SNAPSHOT_DATE": snapshot_date},
        dag=dag,
    )

    def _emit_lineage(**context):
        from apps.shared.lineage import Dataset, emit_complete, emit_start

        run_id = emit_start(
            job_name="dag_silver_municipio",
            inputs=[Dataset.s3(f"s3://bronze/municipios/{snapshot_date}/")],
            outputs=[Dataset.s3("s3://silver/municipio/")],
        )
        if run_id:
            emit_complete(job_name="dag_silver_municipio", run_id=run_id)

    emit_lineage = PythonOperator(
        task_id="emit_lineage",
        python_callable=_emit_lineage,
        trigger_rule="all_success",
    )

    submit >> sensor >> emit_lineage
