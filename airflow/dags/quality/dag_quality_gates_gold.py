"""DAG Quality Gates — Gold DW.

Runs after each Gold load and validates Star Schema integrity:

  dim_check:   Exactly one is_current=true per NK in each dimension (SCD2)
  fato_check:  No null SK, no orphan FKs, minimum fact count
  ge_check:    Great Expectations checkpoint (optional)

Failure blocks downstream consumers and alerts.

Spec: docs/specs/airflow-dags.md — section "dag_quality_gates_gold"
ADR:  docs/architecture/decisions/0006-scd2.md
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

log = logging.getLogger(__name__)

_DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


def _pg_conn(user: str | None = None, password: str | None = None):
    import psycopg2

    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "postgres"),
        user=user or os.environ.get("POSTGRES_GOLD_USER", "gold_engineer"),
        password=password or os.environ.get("POSTGRES_GOLD_PASSWORD", "gold_engineer"),
        connect_timeout=10,
    )


def _validate_scd2_integrity(**context) -> dict:
    """Validates no overlap of current records per NK in any dimension."""
    conn = _pg_conn()
    cur = conn.cursor()
    report = {}

    dims = [
        ("dim_paciente", "id_paciente_hash"),
        ("dim_municipio", "codigo_municipio"),
        ("dim_agravo", "codigo_agravo"),
        ("dim_vacina", "codigo_vacina"),
        ("dim_estabelecimento", "co_cnes"),
    ]

    for table, natural_key in dims:
        cur.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='gold_dw' AND table_name=%s",
            (table,),
        )
        if not cur.fetchone():
            report[table] = "skipped_not_found"
            continue

        cur.execute(f"""
            SELECT {natural_key}, COUNT(*) AS cnt
            FROM gold_dw.{table}
            WHERE is_current = true
            GROUP BY {natural_key}
            HAVING COUNT(*) > 1
            LIMIT 10
        """)
        overlaps = cur.fetchall()

        cur.execute(f"""
            SELECT COUNT(*) FROM gold_dw.{table}
            WHERE dt_inicio > dt_fim
        """)
        inversions = cur.fetchone()[0]

        cur.execute(f"""
            SELECT COUNT(*) FROM gold_dw.{table}
            WHERE is_current = true AND dt_fim != '9999-12-31'
        """)
        bad_end_date = cur.fetchone()[0]

        failures = []
        if overlaps:
            failures.append(f"overlap: {len(overlaps)} NKs with multiple current rows")
        if inversions > 0:
            failures.append(f"date inversion: {inversions} rows")
        if bad_end_date > 0:
            failures.append(f"current rows with wrong dt_fim: {bad_end_date}")

        report[table] = {"status": "ok" if not failures else "failed", "failures": failures}

    cur.close()
    conn.close()

    context["ti"].xcom_push(key="scd2_report", value=report)

    failed = {
        t: r for t, r in report.items() if isinstance(r, dict) and r.get("status") == "failed"
    }
    if failed:
        raise ValueError(f"SCD2 integrity check failed:\n{failed}")

    log.info("[quality/gold] SCD2 check: OK (%d dims)", len(report))
    return report


def _validate_fato_referential_integrity(**context) -> dict:
    """Validates fact FKs against dimensions and absence of null SKs."""
    conn = _pg_conn()
    cur = conn.cursor()
    report = {}

    fact_checks = [
        {
            "table": "fato_notificacao",
            "sk_cols": ["sk_agravo", "sk_municipio_notificacao", "sk_tempo"],
            "dim_map": {
                "sk_agravo": ("dim_agravo", "sk_agravo"),
                "sk_municipio_notificacao": ("dim_municipio", "sk_municipio"),
                "sk_tempo": ("dim_tempo", "sk_tempo"),
            },
        },
        {
            "table": "fato_vacinacao",
            "sk_cols": ["sk_vacina", "sk_municipio_aplicacao"],
            "dim_map": {
                "sk_vacina": ("dim_vacina", "sk_vacina"),
                "sk_municipio_aplicacao": ("dim_municipio", "sk_municipio"),
            },
        },
        {
            "table": "fato_obito",
            "sk_cols": ["sk_agravo", "sk_municipio_residencia"],
            "dim_map": {
                "sk_agravo": ("dim_agravo", "sk_agravo"),
                "sk_municipio_residencia": ("dim_municipio", "sk_municipio"),
            },
        },
    ]

    for check in fact_checks:
        table = check["table"]

        cur.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='gold_dw' AND table_name=%s",
            (table,),
        )
        if not cur.fetchone():
            report[table] = "skipped_not_found"
            continue

        failures = []

        for sk in check["sk_cols"]:
            cur.execute(f"SELECT COUNT(*) FROM gold_dw.{table} WHERE {sk} IS NULL")
            null_count = cur.fetchone()[0]
            if null_count > 0:
                failures.append(f"{sk}: {null_count} nulls")

        for fk_col, (dim_table, dim_pk) in check["dim_map"].items():
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema='gold_dw' AND table_name=%s",
                (dim_table,),
            )
            if not cur.fetchone():
                continue
            cur.execute(f"""
                SELECT COUNT(*) FROM gold_dw.{table} f
                LEFT JOIN gold_dw.{dim_table} d ON f.{fk_col} = d.{dim_pk}
                WHERE f.{fk_col} IS NOT NULL AND d.{dim_pk} IS NULL
            """)
            orphan_count = cur.fetchone()[0]
            if orphan_count > 0:
                failures.append(f"{fk_col} → {dim_table}: {orphan_count} orphan FKs")

        report[table] = {"status": "ok" if not failures else "failed", "failures": failures}

    cur.close()
    conn.close()

    context["ti"].xcom_push(key="fato_report", value=report)

    failed = {
        t: r for t, r in report.items() if isinstance(r, dict) and r.get("status") == "failed"
    }
    if failed:
        raise ValueError(f"Fact referential integrity check failed:\n{failed}")

    log.info("[quality/gold] FK check: OK (%d facts)", len(report))
    return report


def _validate_fato_row_counts(**context) -> dict:
    """Checks that each fact table has at least 1 row (not empty after load)."""
    conn = _pg_conn()
    cur = conn.cursor()
    report = {}

    fact_tables = ["fato_notificacao", "fato_vacinacao", "fato_obito"]
    for table in fact_tables:
        cur.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='gold_dw' AND table_name=%s",
            (table,),
        )
        if not cur.fetchone():
            report[table] = "skipped"
            continue
        cur.execute(f"SELECT COUNT(*) FROM gold_dw.{table}")
        row_count = cur.fetchone()[0]
        report[table] = {"count": row_count, "status": "ok" if row_count > 0 else "empty"}

    cur.close()
    conn.close()

    context["ti"].xcom_push(key="row_count_report", value=report)

    empty = {t: r for t, r in report.items() if isinstance(r, dict) and r.get("status") == "empty"}
    if empty:
        log.warning("[quality/gold] Empty fact tables: %s", list(empty.keys()))

    log.info("[quality/gold] Row count check: %s", report)
    return report


def _run_ge_checkpoint(**context) -> str:
    """Runs Great Expectations checkpoint for Gold if available."""
    try:
        import great_expectations as gx

        ctx = gx.get_context()
        result = ctx.run_checkpoint(checkpoint_name="gold_dw_checkpoint")
        if not result.success:
            raise ValueError(f"GE checkpoint Gold failed: {result.statistics}")
        log.info("[quality/gold] GE checkpoint: OK")
        return "ge_passed"
    except ImportError:
        log.info("[quality/gold] Great Expectations not installed — skipping")
        return "ge_skipped"
    except Exception as exc:
        if "does not exist" in str(exc) or "not found" in str(exc).lower():
            log.info("[quality/gold] GE checkpoint not configured — skipping")
            return "ge_skipped"
        raise


with DAG(
    dag_id="dag_quality_gates_gold",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["quality", "gold", "scd2", "great-expectations"],
    default_args=_DEFAULT_ARGS,
    doc_md=__doc__,
) as dag:
    wait_gold_dims = ExternalTaskSensor(
        task_id="wait_gold_dims",
        external_dag_id="dag_gold_dims",
        external_task_id="gold_dims_spark_sensor",
        execution_date_fn=lambda dt: dt,
        timeout=7200,
        poke_interval=120,
        mode="reschedule",
    )

    check_scd2 = PythonOperator(
        task_id="validate_scd2_integrity",
        python_callable=_validate_scd2_integrity,
        execution_timeout=timedelta(minutes=15),
    )

    check_fk = PythonOperator(
        task_id="validate_fato_referential_integrity",
        python_callable=_validate_fato_referential_integrity,
        execution_timeout=timedelta(minutes=15),
    )

    check_counts = PythonOperator(
        task_id="validate_fato_row_counts",
        python_callable=_validate_fato_row_counts,
        execution_timeout=timedelta(minutes=5),
    )

    ge_checkpoint = PythonOperator(
        task_id="run_ge_checkpoint",
        python_callable=_run_ge_checkpoint,
        execution_timeout=timedelta(minutes=15),
    )

    wait_gold_dims >> [check_scd2, check_fk, check_counts] >> ge_checkpoint
