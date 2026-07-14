"""DAG Bronze — SIM (Sistema de Informação sobre Mortalidade).

Extrai óbitos anuais do SIM e persiste em Delta Lake (bronze/sim_obitos/),
particionado por ano_part. Roda uma vez por ano (ou sob demanda).
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from _common.spark_k8s import make_spark_operator

with DAG(
    dag_id="dag_bronze_sim_obitos",
    schedule_interval="0 8 1 1 *",  # 1º de janeiro de cada ano
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "sim", "obitos", "mortalidade"],
    default_args={"retries": 2, "retry_delay": __import__("datetime").timedelta(minutes=5)},
) as dag:

    def _extract(**context):
        from pipelines.extraction.sim_obitos import run

        ano = context["data_interval_start"].year - 1  # dados do ano anterior
        result = run(ano=ano)
        context["ti"].xcom_push(key="extraction_result", value=result)

    extract = PythonOperator(
        task_id="extract_sim_obitos",
        python_callable=_extract,
        pool="extraction_pool",
    )

    ano = "{{ (data_interval_start.year - 1) | string }}"
    submit, sensor = make_spark_operator(
        task_id="bronze_sim_obitos_spark",
        template_name="bronze-sim-obitos.yaml",
        substitutions={"ANO": ano},
        dag=dag,
    )

    extract >> submit
