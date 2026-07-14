"""DAG Bronze — Municipalities.

Monthly snapshot of municipalities (IBGE × health regions), stored in
Delta Lake (bronze/municipios/), partitioned by snapshot_date.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from _common.spark_k8s import make_spark_operator
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dag_bronze_municipios",
    schedule_interval="0 4 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "municipios", "ibge"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:

    def _extract(**context):
        from pipelines.extraction.municipios import run

        result = run()
        context["ti"].xcom_push(key="extraction_result", value=result)

    extract = PythonOperator(
        task_id="extract_municipios",
        python_callable=_extract,
        pool="extraction_pool",
    )

    snapshot_date = "{{ data_interval_start.strftime('%Y%m%d') }}"
    submit, sensor = make_spark_operator(
        task_id="bronze_municipios_spark",
        template_name="bronze-municipios.yaml",
        substitutions={"SNAPSHOT_DATE": snapshot_date},
        dag=dag,
    )

    def _emit_lineage(**context):
        from pipelines.common.lineage import Dataset, emit_complete, emit_start

        run_id = emit_start(
            job_name="dag_bronze_municipios.extract",
            inputs=[Dataset.s3("ibge.gov.br/api/v1/localidades/municipios")],
            outputs=[Dataset.s3(f"s3://landing/municipios/{snapshot_date}/")],
        )
        if run_id:
            row_count = (
                context["ti"].xcom_pull(task_ids="extract_municipios", key="extraction_result")
                or {}
            ).get("row_count", 0)
            emit_complete(
                job_name="dag_bronze_municipios.extract",
                run_id=run_id,
                output_facets={"rowCount": {"rowCount": row_count}},
            )

    emit_lineage = PythonOperator(
        task_id="emit_lineage",
        python_callable=_emit_lineage,
        trigger_rule="all_success",
    )

    extract >> submit >> sensor >> emit_lineage
