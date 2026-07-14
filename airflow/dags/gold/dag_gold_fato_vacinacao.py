"""DAG Gold — Fact Vaccination (PNI).

Loads fato_vacinacao into Postgres gold_dw.
"""

from __future__ import annotations

from datetime import datetime

from _common.spark_k8s import make_spark_operator
from airflow import DAG

with DAG(
    dag_id="dag_gold_fato_vacinacao",
    schedule_interval="0 10 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "fato", "vacinacao"],
    default_args={"retries": 2, "retry_delay": __import__("datetime").timedelta(minutes=5)},
) as dag:
    ano_mes = "{{ data_interval_start.strftime('%Y%m') }}"
    make_spark_operator(
        task_id="gold_fato_vacinacao",
        template_name="gold-fatos.yaml",
        substitutions={
            "FATO": "vacinacao",
            "REF": ano_mes,
            "ANO_MES": ano_mes,
            "ANO": "{{ data_interval_start.year }}",
        },
        dag=dag,
    )
