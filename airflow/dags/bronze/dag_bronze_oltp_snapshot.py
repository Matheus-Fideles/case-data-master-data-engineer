"""DAG Bronze — OLTP Snapshot (oltp.paciente → landing/ → bronze/oltp_paciente/).

Unlike other Bronze DAGs (REST API), extraction uses psycopg2 to read
directly from the OLTP Postgres. Delta ingestion follows the same pattern
via SparkKubernetesOperator.

WARNING: Produces Bronze WITH PII. Separate pool (oltp_pool) limits concurrency.
Immediate downstream: dag_silver_paciente_mascaramento (30 min later).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from _common.spark_k8s import make_spark_operator
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dag_bronze_oltp_snapshot",
    schedule_interval="0 1 * * *",  # daily at 01:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "oltp", "paciente", "pii"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
) as dag:

    def _extract(**context):
        from apps.oltp.jobs.extract_oltp_snapshot import run

        snapshot_date = context["data_interval_start"].strftime("%Y%m%d")
        result = run(snapshot_date=snapshot_date, incremental=False)
        context["ti"].xcom_push(key="extraction_result", value=result)
        return result["row_count"]

    extract = PythonOperator(
        task_id="extract_oltp_paciente",
        python_callable=_extract,
        pool="oltp_pool",
    )

    snapshot_date = "{{ data_interval_start.strftime('%Y%m%d') }}"
    submit, sensor = make_spark_operator(
        task_id="bronze_oltp_paciente_spark",
        template_name="bronze-oltp-paciente.yaml",
        substitutions={"SNAPSHOT_DATE": snapshot_date},
        dag=dag,
    )

    def _emit_lineage(**context):
        from apps.shared.lineage import Dataset, emit_complete, emit_start

        run_id = emit_start(
            job_name="dag_bronze_oltp_snapshot.extract",
            inputs=[Dataset.postgres("oltp", "paciente")],
            outputs=[Dataset.s3(f"s3://landing/oltp_paciente/{snapshot_date}/")],
        )
        if run_id:
            row_count = (
                context["ti"].xcom_pull(task_ids="extract_oltp_paciente", key="extraction_result")
                or {}
            ).get("row_count", 0)
            emit_complete(
                job_name="dag_bronze_oltp_snapshot.extract",
                run_id=run_id,
                output_facets={"rowCount": {"rowCount": row_count}},
            )

    emit_lineage = PythonOperator(
        task_id="emit_lineage",
        python_callable=_emit_lineage,
        trigger_rule="all_success",
    )

    extract >> submit >> sensor >> emit_lineage
