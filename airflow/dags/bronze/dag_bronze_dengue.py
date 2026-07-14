"""DAG Bronze — Dengue.

Extracts dengue notifications from the Ministry of Health API and stores them
in Delta Lake in the Bronze layer (s3://bronze/arboviroses/dengue/).

Flow:
  extract_dengue (PythonOperator)
      → bronze_dengue_spark (SparkKubernetesOperator)
      → bronze_dengue_spark_sensor (SparkKubernetesSensor)
      → emit_lineage (PythonOperator — registers in Marquez)

OpenLineage:
  The automatic Spark listener captures Delta inputs/outputs.
  The emit_lineage task registers the extraction event (landing/) that
  does not go through Spark — closes the full graph in Marquez.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from _common.spark_k8s import make_spark_operator

with DAG(
    dag_id="dag_bronze_dengue",
    schedule_interval="0 6 1 * *",  # 1st of each month at 06:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "arboviroses", "dengue"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:

    def _extract(**context):
        from pipelines.extraction.arboviroses import run

        year = context["data_interval_start"].year
        result = run(agravo="dengue", ano=year)
        context["ti"].xcom_push(key="extraction_result", value=result)
        context["ti"].xcom_push(key="output_path", value=result.get("output_path", ""))

    extract = PythonOperator(
        task_id="extract_dengue",
        python_callable=_extract,
        pool="extraction_pool",
    )

    year_month = "{{ data_interval_start.strftime('%Y%m') }}"
    submit, sensor = make_spark_operator(
        task_id="bronze_dengue_spark",
        template_name="bronze-dengue.yaml",
        substitutions={"ANO_MES": year_month},
        dag=dag,
    )

    def _emit_lineage(**context):
        from pipelines.common.lineage import Dataset, emit_complete, emit_start

        run_id = emit_start(
            job_name="dag_bronze_dengue.extract",
            inputs=[Dataset.s3("apidadosabertos.saude.gov.br/api/notif/dengue")],
            outputs=[Dataset.s3(f"s3://landing/arboviroses/dengue/{year_month}/")],
        )
        if run_id:
            row_count = (
                context["ti"].xcom_pull(task_ids="extract_dengue", key="extraction_result") or {}
            ).get("row_count", 0)
            emit_complete(
                job_name="dag_bronze_dengue.extract",
                run_id=run_id,
                output_facets={"rowCount": {"rowCount": row_count}},
            )

    emit_lineage = PythonOperator(
        task_id="emit_lineage",
        python_callable=_emit_lineage,
        trigger_rule="all_success",
    )

    extract >> submit >> sensor >> emit_lineage
