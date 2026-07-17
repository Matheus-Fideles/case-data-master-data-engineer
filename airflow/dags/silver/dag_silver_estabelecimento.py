"""DAG Silver — Establishment (CNES)."""

from __future__ import annotations

from datetime import datetime, timedelta

from _common.spark_k8s import make_spark_operator
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dag_silver_estabelecimento",
    schedule_interval="30 5 * * 1",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "cnes", "estabelecimento"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:
    snapshot_date = "{{ data_interval_start.strftime('%Y%m%d') }}"
    submit, sensor = make_spark_operator(
        task_id="silver_estabelecimento_spark",
        template_name="silver-estabelecimento.yaml",
        substitutions={"SNAPSHOT_DATE": snapshot_date},
        dag=dag,
    )

    def _emit_lineage(**context):
        from apps.shared.lineage import Dataset, emit_complete, emit_start

        run_id = emit_start(
            job_name="dag_silver_estabelecimento",
            inputs=[Dataset.s3(f"s3://bronze/cnes/{snapshot_date}/")],
            outputs=[Dataset.s3("s3://silver/estabelecimento/")],
        )
        if run_id:
            emit_complete(job_name="dag_silver_estabelecimento", run_id=run_id)

    emit_lineage = PythonOperator(
        task_id="emit_lineage",
        python_callable=_emit_lineage,
        trigger_rule="all_success",
    )

    submit >> sensor >> emit_lineage
