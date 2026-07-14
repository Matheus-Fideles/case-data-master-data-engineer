"""DAG Bronze — Vacinação PNI.

Extrai doses aplicadas do Programa Nacional de Imunizações e persiste em
Delta Lake (bronze/vacinacao_pni/), particionado por ano_mes.

id_paciente já chega pre-hasheado pelo Ministério (codigo_paciente).
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from _common.spark_k8s import make_spark_operator

with DAG(
    dag_id="dag_bronze_vacinacao_pni",
    schedule_interval="0 7 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "vacinacao", "pni"],
    default_args={"retries": 2, "retry_delay": __import__("datetime").timedelta(minutes=5)},
) as dag:

    def _extract(**context):
        from pipelines.extraction.vacinacao_pni import run

        ano = context["data_interval_start"].year
        result = run(ano=ano)
        context["ti"].xcom_push(key="extraction_result", value=result)

    extract = PythonOperator(
        task_id="extract_vacinacao_pni",
        python_callable=_extract,
        pool="extraction_pool",
    )

    ano_mes = "{{ data_interval_start.strftime('%Y%m') }}"
    submit, sensor = make_spark_operator(
        task_id="bronze_vacinacao_pni_spark",
        template_name="bronze-vacinacao-pni.yaml",
        substitutions={"ANO_MES": ano_mes},
        dag=dag,
    )

    extract >> submit
