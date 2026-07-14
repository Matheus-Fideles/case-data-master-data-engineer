"""DAG Bronze — SIM (Mortality Information System).

Extracts annual deaths from SIM and stores them in Delta Lake (bronze/sim_obitos/),
partitioned by year. Runs once a year (or on demand).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from _common.spark_k8s import make_spark_operator

with DAG(
    dag_id="dag_bronze_sim_obitos",
    schedule_interval="0 8 1 1 *",  # January 1st each year
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "sim", "obitos", "mortalidade"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:

    def _extract(**context):
        from pipelines.extraction.sim_obitos import run

        year = context["data_interval_start"].year - 1  # prior year data
        result = run(ano=year)
        context["ti"].xcom_push(key="extraction_result", value=result)

    extract = PythonOperator(
        task_id="extract_sim_obitos",
        python_callable=_extract,
        pool="extraction_pool",
    )

    year = "{{ (data_interval_start.year - 1) | string }}"
    submit, sensor = make_spark_operator(
        task_id="bronze_sim_obitos_spark",
        template_name="bronze-sim-obitos.yaml",
        substitutions={"ANO": year},
        dag=dag,
    )

    def _emit_lineage(**context):
        from pipelines.common.lineage import Dataset, emit_complete, emit_start

        run_id = emit_start(
            job_name="dag_bronze_sim_obitos.extract",
            inputs=[Dataset.s3(f"datasus.gov.br/sim/{year}")],
            outputs=[Dataset.s3(f"s3://landing/sim_obitos/{year}/")],
        )
        if run_id:
            row_count = (
                context["ti"].xcom_pull(task_ids="extract_sim_obitos", key="extraction_result") or {}
            ).get("row_count", 0)
            emit_complete(
                job_name="dag_bronze_sim_obitos.extract",
                run_id=run_id,
                output_facets={"rowCount": {"rowCount": row_count}},
            )

    emit_lineage = PythonOperator(
        task_id="emit_lineage",
        python_callable=_emit_lineage,
        trigger_rule="all_success",
    )

    extract >> submit >> sensor >> emit_lineage
