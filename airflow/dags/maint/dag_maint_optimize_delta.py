"""DAG de Manutenção — OPTIMIZE + VACUUM nas tabelas Delta Lake.

Executa semanalmente (domingo 03h00) para:
  1. Compactar small files via OPTIMIZE (melhora leitura analítica)
  2. ZORDER nas colunas de alta cardinalidade dos fatos (reduz data skipping)
  3. VACUUM para remover versões antigas e cumprir retenção LGPD

Retenção por camada (ADR-0004 / LGPD Art. 16):
  - Bronze: 7 dias (168h)  — dados brutos com PII, reter o mínimo
  - Silver: 30 dias (720h) — dados mascarados, retenção operacional
  - Gold:   30 dias (720h) — agregados, mesma política de Silver

Spec: docs/specs/airflow-dags.md — seção "dag_maint_optimize_delta"
ADR:  docs/architecture/decisions/0004-pii-masking.md
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

_DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "start_date": datetime(2024, 1, 1),
}

# Tabelas Delta por camada e suas configurações de compactação
from _common.spark_local import BRONZE_TABLES as _BRONZE_TABLES

_SILVER_TABLES = [
    "s3a://silver/notificacao/",
    "s3a://silver/sim_obitos/",
    "s3a://silver/vacinacao_pni/",
    "s3a://silver/estabelecimento/",
    "s3a://silver/municipio/",
    "s3a://silver/paciente/",
]

# Tabelas Gold Delta (exceto as que vivem no Postgres)
_GOLD_TABLES: list[tuple[str, list[str]]] = [
    # (path, colunas para ZORDER) — lista vazia = OPTIMIZE sem ZORDER
    ("s3a://gold/fato_notificacao/",  ["sk_agravo", "sk_municipio_notificacao"]),
    ("s3a://gold/fato_obito/",        ["sk_agravo", "sk_municipio_residencia"]),
    ("s3a://gold/fato_vacinacao/",    ["sk_vacina", "sk_municipio_aplicacao"]),
]

# Retenção por camada em horas
_RETAIN_BRONZE_H = int(os.environ.get("DELTA_RETAIN_BRONZE_HOURS", "168"))   # 7d
_RETAIN_SILVER_H = int(os.environ.get("DELTA_RETAIN_SILVER_HOURS", "720"))   # 30d
_RETAIN_GOLD_H   = int(os.environ.get("DELTA_RETAIN_GOLD_HOURS",   "720"))   # 30d


def _get_spark():
    from _common.spark_local import make_local_spark
    return make_local_spark("maint_optimize_delta")


def _optimize_tables(paths: list[str], label: str, **context) -> dict:
    """Executa OPTIMIZE em cada tabela Delta da lista."""
    spark = _get_spark()
    results = {}
    for path in paths:
        try:
            log.info("[%s] OPTIMIZE %s", label, path)
            df = spark.sql(f"OPTIMIZE delta.`{path}`")
            metrics = df.collect()[0].asDict() if df.count() > 0 else {}
            results[path] = {"status": "ok", "metrics": str(metrics)}
            log.info("[%s] OPTIMIZE concluído: %s → %s", label, path, metrics)
        except Exception as e:
            log.error("[%s] OPTIMIZE falhou para %s: %s", label, path, e)
            results[path] = {"status": "error", "error": str(e)}

    context["ti"].xcom_push(key=f"optimize_{label}_results", value=results)
    errors = [p for p, r in results.items() if r["status"] == "error"]
    if errors:
        raise RuntimeError(f"OPTIMIZE falhou em {len(errors)} tabela(s): {errors}")
    return results


def _optimize_bronze(**context) -> dict:
    return _optimize_tables(_BRONZE_TABLES, "bronze", **context)


def _optimize_silver(**context) -> dict:
    return _optimize_tables(_SILVER_TABLES, "silver", **context)


def _optimize_gold_zorder(**context) -> dict:
    """OPTIMIZE com ZORDER nas colunas de alta cardinalidade dos fatos Gold."""
    spark = _get_spark()
    results = {}
    for path, zorder_cols in _GOLD_TABLES:
        try:
            if zorder_cols:
                cols_str = ", ".join(zorder_cols)
                sql = f"OPTIMIZE delta.`{path}` ZORDER BY ({cols_str})"
            else:
                sql = f"OPTIMIZE delta.`{path}`"
            log.info("[gold] %s", sql)
            df = spark.sql(sql)
            metrics = df.collect()[0].asDict() if df.count() > 0 else {}
            results[path] = {"status": "ok", "zorder_cols": zorder_cols}
            log.info("[gold] OPTIMIZE+ZORDER concluído: %s", path)
        except Exception as e:
            log.error("[gold] OPTIMIZE falhou para %s: %s", path, e)
            results[path] = {"status": "error", "error": str(e)}

    context["ti"].xcom_push(key="optimize_gold_results", value=results)
    errors = [p for p, r in results.items() if r["status"] == "error"]
    if errors:
        raise RuntimeError(f"OPTIMIZE Gold falhou em {len(errors)} tabela(s): {errors}")
    return results


def _vacuum_bronze(**context) -> dict:
    """VACUUM Bronze com retenção mínima (dados brutos com PII — LGPD)."""
    spark = _get_spark()
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    results = {}
    for path in _BRONZE_TABLES:
        try:
            log.info("[vacuum/bronze] VACUUM %s RETAIN %dh", path, _RETAIN_BRONZE_H)
            spark.sql(f"VACUUM delta.`{path}` RETAIN {_RETAIN_BRONZE_H} HOURS")
            results[path] = "ok"
        except Exception as e:
            log.error("[vacuum/bronze] falhou %s: %s", path, e)
            results[path] = f"error: {e}"

    context["ti"].xcom_push(key="vacuum_bronze_results", value=results)
    return results


def _vacuum_silver_gold(**context) -> dict:
    """VACUUM Silver e Gold com retenção operacional (30 dias)."""
    spark = _get_spark()
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    results = {}

    for path in _SILVER_TABLES:
        try:
            spark.sql(f"VACUUM delta.`{path}` RETAIN {_RETAIN_SILVER_H} HOURS")
            results[path] = "ok"
        except Exception as e:
            results[path] = f"error: {e}"

    for path, _ in _GOLD_TABLES:
        try:
            spark.sql(f"VACUUM delta.`{path}` RETAIN {_RETAIN_GOLD_H} HOURS")
            results[path] = "ok"
        except Exception as e:
            results[path] = f"error: {e}"

    context["ti"].xcom_push(key="vacuum_silver_gold_results", value=results)
    return results


with DAG(
    dag_id="dag_maint_optimize_delta",
    schedule_interval="0 3 * * 0",   # domingo 03:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["maint", "delta", "optimize", "vacuum", "lgpd"],
    default_args=_DEFAULT_ARGS,
    doc_md=__doc__,
) as dag:

    optimize_bronze = PythonOperator(
        task_id="optimize_bronze",
        python_callable=_optimize_bronze,
        pool="spark_pool",
        execution_timeout=timedelta(hours=1),
    )

    optimize_silver = PythonOperator(
        task_id="optimize_silver",
        python_callable=_optimize_silver,
        pool="spark_pool",
        execution_timeout=timedelta(hours=1),
    )

    optimize_gold_zorder = PythonOperator(
        task_id="optimize_gold_zorder",
        python_callable=_optimize_gold_zorder,
        pool="spark_pool",
        execution_timeout=timedelta(hours=2),
    )

    vacuum_bronze = PythonOperator(
        task_id="vacuum_bronze",
        python_callable=_vacuum_bronze,
        pool="spark_pool",
        execution_timeout=timedelta(hours=1),
    )

    vacuum_silver_gold = PythonOperator(
        task_id="vacuum_silver_gold",
        python_callable=_vacuum_silver_gold,
        pool="spark_pool",
        execution_timeout=timedelta(hours=1),
    )

    # Bronze e Silver em paralelo → Gold → VACUUM Bronze → VACUUM Silver/Gold
    [optimize_bronze, optimize_silver] >> optimize_gold_zorder
    optimize_bronze >> vacuum_bronze
    optimize_gold_zorder >> vacuum_silver_gold
