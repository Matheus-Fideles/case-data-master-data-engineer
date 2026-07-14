"""DAG Silver — Patient Masking (bronze/oltp_paciente → silver/paciente/).

Runs 30 min after dag_bronze_oltp_snapshot and waits via ExternalTaskSensor.

The Spark job applies LGPD masking (ADR-0004):
  - cpf        → SHA-256 + salt (deterministic for joins)
  - nome       → suppressed
  - data_nascimento → generalized to ano_nascimento (birth year)
  - cep        → truncated to 3 digits
  - email/telefone → suppressed

validate_no_pii() ensures no PII column survives before writing to Silver —
failure here aborts the DAG and blocks all downstream.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from _common.spark_k8s import make_spark_operator

with DAG(
    dag_id="dag_silver_paciente_mascaramento",
    schedule_interval="30 1 * * *",   # 30 min after Bronze OLTP (01:00)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "paciente", "mascaramento", "lgpd"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
) as dag:

    wait_bronze = ExternalTaskSensor(
        task_id="wait_bronze_oltp_snapshot",
        external_dag_id="dag_bronze_oltp_snapshot",
        external_task_id="bronze_oltp_paciente_spark_sensor",
        execution_date_fn=lambda dt: dt,
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    snapshot_date = "{{ data_interval_start.strftime('%Y%m%d') }}"
    submit, sensor = make_spark_operator(
        task_id="silver_paciente_spark",
        template_name="silver-paciente.yaml",
        substitutions={"SNAPSHOT_DATE": snapshot_date},
        dag=dag,
    )

    def _emit_lineage(**context):
        from pipelines.common.lineage import Dataset, emit_complete, emit_start

        run_id = emit_start(
            job_name="dag_silver_paciente_mascaramento",
            inputs=[Dataset.s3(f"s3://bronze/oltp_paciente/{snapshot_date}/")],
            outputs=[Dataset.s3("s3://silver/paciente/")],
        )
        if run_id:
            emit_complete(job_name="dag_silver_paciente_mascaramento", run_id=run_id)

    emit_lineage = PythonOperator(
        task_id="emit_lineage",
        python_callable=_emit_lineage,
        trigger_rule="all_success",
    )

    wait_bronze >> submit >> sensor >> emit_lineage
