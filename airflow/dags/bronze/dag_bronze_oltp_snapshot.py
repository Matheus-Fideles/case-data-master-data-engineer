"""DAG Bronze — OLTP Snapshot (oltp.paciente → landing/ → bronze/oltp_paciente/).

Diferente dos outros DAGs Bronze (REST API), aqui a extração usa psycopg2
para ler diretamente do Postgres OLTP. A ingestão Delta segue o mesmo padrão
via SparkKubernetesOperator.

⚠️  Produz Bronze COM PII. Pool separado (oltp_pool) limita concorrência.
Downstream imediato: dag_silver_paciente_mascaramento (30 min depois).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from _common.spark_k8s import make_spark_operator

with DAG(
    dag_id="dag_bronze_oltp_snapshot",
    schedule_interval="0 1 * * *",   # todo dia à 01h00
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
        from pipelines.extraction.oltp_snapshot import run

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

    extract >> submit
