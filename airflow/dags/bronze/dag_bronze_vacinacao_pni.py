"""DAG Bronze — Vaccination PNI.

Extracts administered vaccine doses from the National Immunization Program (PNI)
and stores them in Delta Lake (bronze/vacinacao_pni/), partitioned by year_month.

patient_id arrives pre-hashed from the Ministry (codigo_paciente).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from _common.spark_k8s import make_spark_operator
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dag_bronze_vacinacao_pni",
    schedule_interval="0 7 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "vacinacao", "pni"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
) as dag:

    def _extract(**context):
        from pipelines.extraction.vacinacao_pni import run

        year = context["data_interval_start"].year
        result = run(ano=year)
        context["ti"].xcom_push(key="extraction_result", value=result)

    extract = PythonOperator(
        task_id="extract_vacinacao_pni",
        python_callable=_extract,
        pool="extraction_pool",
    )

    year_month = "{{ data_interval_start.strftime('%Y%m') }}"
    submit, sensor = make_spark_operator(
        task_id="bronze_vacinacao_pni_spark",
        template_name="bronze-vacinacao-pni.yaml",
        substitutions={"ANO_MES": year_month},
        dag=dag,
    )

    def _emit_lineage(**context):
        from pipelines.common.lineage import Dataset, emit_complete, emit_start

        run_id = emit_start(
            job_name="dag_bronze_vacinacao_pni.extract",
            inputs=[Dataset.s3(f"datasus.gov.br/pni/vacinacao/{year_month[:4]}")],
            outputs=[Dataset.s3(f"s3://landing/vacinacao_pni/{year_month}/")],
        )
        if run_id:
            row_count = (
                context["ti"].xcom_pull(task_ids="extract_vacinacao_pni", key="extraction_result")
                or {}
            ).get("row_count", 0)
            emit_complete(
                job_name="dag_bronze_vacinacao_pni.extract",
                run_id=run_id,
                output_facets={"rowCount": {"rowCount": row_count}},
            )

    emit_lineage = PythonOperator(
        task_id="emit_lineage",
        python_callable=_emit_lineage,
        trigger_rule="all_success",
    )

    extract >> submit >> sensor >> emit_lineage
