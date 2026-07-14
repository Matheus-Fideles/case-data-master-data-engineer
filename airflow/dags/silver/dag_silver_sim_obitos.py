"""DAG Silver — SIM Deaths."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from _common.spark_k8s import make_spark_operator

with DAG(
    dag_id="dag_silver_sim_obitos",
    schedule_interval="30 8 1 1 *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "sim", "obitos"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:

    year = "{{ (data_interval_start.year - 1) | string }}"
    submit, sensor = make_spark_operator(
        task_id="silver_sim_obitos_spark",
        template_name="silver-sim-obitos.yaml",
        substitutions={"ANO": year},
        dag=dag,
    )

    def _emit_lineage(**context):
        from pipelines.common.lineage import Dataset, emit_complete, emit_start

        run_id = emit_start(
            job_name="dag_silver_sim_obitos",
            inputs=[Dataset.s3(f"s3://bronze/sim_obitos/{year}/")],
            outputs=[Dataset.s3("s3://silver/sim_obitos/")],
        )
        if run_id:
            emit_complete(job_name="dag_silver_sim_obitos", run_id=run_id)

    emit_lineage = PythonOperator(
        task_id="emit_lineage",
        python_callable=_emit_lineage,
        trigger_rule="all_success",
    )

    submit >> sensor >> emit_lineage
