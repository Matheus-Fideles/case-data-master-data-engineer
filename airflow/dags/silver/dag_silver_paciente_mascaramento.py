"""DAG Silver — Mascaramento de Pacientes (bronze/oltp_paciente → silver/paciente/).

Roda 30 min após dag_bronze_oltp_snapshot e aguarda via ExternalTaskSensor.

O job Spark aplica mascaramento LGPD (ADR-0004):
  - cpf        → SHA-256 + salt (determinístico para joins)
  - nome       → suprimido
  - data_nascimento → generalizado para ano_nascimento
  - cep        → truncado para 3 dígitos
  - email/telefone → suprimidos

validate_no_pii() garante que nenhuma coluna PII sobrevive antes de gravar
no Silver — falha aqui abortará o DAG e bloqueará todos os downstream.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from _common.spark_k8s import make_spark_operator

with DAG(
    dag_id="dag_silver_paciente_mascaramento",
    schedule_interval="30 1 * * *",   # 30 min após Bronze OLTP (01h00)
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

    wait_bronze >> submit
