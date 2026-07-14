"""DAG Gold — Fact Death (SIM).

Loads fato_obito into Postgres gold_dw. Runs annually.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG

from _common.spark_k8s import make_spark_operator

with DAG(
    dag_id="dag_gold_fato_obito",
    schedule_interval="0 10 1 1 *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "fato", "obito", "sim"],
    default_args={"retries": 2, "retry_delay": __import__("datetime").timedelta(minutes=5)},
) as dag:

    ano = "{{ (data_interval_start.year - 1) | string }}"
    make_spark_operator(
        task_id="gold_fato_obito",
        template_name="gold-fatos.yaml",
        substitutions={
            "FATO": "obito",
            "REF": ano,
            "ANO_MES": "{{ data_interval_start.strftime('%Y%m') }}",
            "ANO": ano,
        },
        dag=dag,
    )
