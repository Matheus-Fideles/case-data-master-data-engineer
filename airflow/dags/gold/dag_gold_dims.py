"""DAG Gold — Dimensions.

Loads dim_municipio, dim_agravo, dim_vacina and dim_tempo into Postgres gold_dw.
Runs after the corresponding Silver DAGs.
"""

from __future__ import annotations

from datetime import datetime

from _common.spark_k8s import make_spark_operator
from airflow import DAG

with DAG(
    dag_id="dag_gold_dims",
    schedule_interval="0 9 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "dimensoes", "star-schema"],
    default_args={"retries": 2, "retry_delay": __import__("datetime").timedelta(minutes=5)},
) as dag:
    snapshot_date = "{{ data_interval_start.strftime('%Y%m%d') }}"
    ano_mes = "{{ data_interval_start.strftime('%Y%m') }}"

    # dim_municipio and dim_agravo can run in parallel
    _dim_mun_submit, _dim_mun_sensor = make_spark_operator(
        task_id="gold_dim_municipio",
        template_name="gold-dims.yaml",
        substitutions={
            "DIM": "municipio",
            "REF": snapshot_date,
            "SNAPSHOT_DATE": snapshot_date,
            "ANO_MES": ano_mes,
        },
        dag=dag,
    )

    _dim_agr_submit, _dim_agr_sensor = make_spark_operator(
        task_id="gold_dim_agravo",
        template_name="gold-dims.yaml",
        substitutions={
            "DIM": "agravo",
            "REF": "static",
            "SNAPSHOT_DATE": snapshot_date,
            "ANO_MES": ano_mes,
        },
        dag=dag,
    )

    _dim_vac_submit, _dim_vac_sensor = make_spark_operator(
        task_id="gold_dim_vacina",
        template_name="gold-dims.yaml",
        substitutions={
            "DIM": "vacina",
            "REF": ano_mes,
            "SNAPSHOT_DATE": snapshot_date,
            "ANO_MES": ano_mes,
        },
        dag=dag,
    )

    _dim_tmp_submit, _dim_tmp_sensor = make_spark_operator(
        task_id="gold_dim_tempo",
        template_name="gold-dims.yaml",
        substitutions={
            "DIM": "tempo",
            "REF": ano_mes,
            "SNAPSHOT_DATE": snapshot_date,
            "ANO_MES": ano_mes,
        },
        dag=dag,
    )
