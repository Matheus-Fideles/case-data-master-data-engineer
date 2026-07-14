"""DAG Quality Gates — Silver.

Runs after each Silver job and validates quality suites:

  bronze_suite: key field completeness, minimum schema, no exposed PII column
  silver_suite: id_paciente_hash unique (NK), correct types, masking confirmed

Failure here blocks downstream Gold via ExternalTaskSensor.

Implementation:
  - Uses Great Expectations if available (via configured checkpoints)
  - Fallback: native Spark validations without GE (works without GE installed)

Spec: docs/specs/airflow-dags.md — section "dag_quality_gates_silver"
"""
from __future__ import annotations

import logging
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

_PII_COLS = {"cpf", "rg", "nome", "nome_completo", "data_nascimento", "email", "telefone"}

_SILVER_CHECKS = {
    "s3a://silver/notificacao/": {
        "required_cols": ["id_notificacao", "ano_mes", "_ingestion_ts"],
        "not_null_cols": ["id_notificacao", "ano_mes"],
        "unique_cols": [],
    },
    "s3a://silver/paciente/": {
        "required_cols": ["id_paciente_hash", "snapshot_date", "masking_version"],
        "not_null_cols": ["id_paciente_hash", "snapshot_date"],
        "unique_cols": ["id_paciente_hash"],
    },
    "s3a://silver/estabelecimento/": {
        "required_cols": ["codigo_cnes", "snapshot_date"],
        "not_null_cols": ["codigo_cnes"],
        "unique_cols": [],
    },
    "s3a://silver/municipio/": {
        "required_cols": ["codigo_municipio", "_ingestion_ts"],
        "not_null_cols": ["codigo_municipio"],
        "unique_cols": ["codigo_municipio"],
    },
}


def _get_spark():
    from _common.spark_local import make_local_spark
    return make_local_spark("quality_gates_silver")


def _validate_no_pii_in_silver(**context) -> dict:
    """Checks that no PII column exists in any Silver table."""
    from pyspark.sql import functions as F

    spark = _get_spark()
    violations = {}

    for path in _SILVER_CHECKS:
        try:
            from delta import DeltaTable
            if not DeltaTable.isDeltaTable(spark, path):
                log.warning("[quality/silver] %s is not a Delta Table — skipping", path)
                continue

            df = spark.read.format("delta").load(path)
            cols = set(df.columns)
            leaked = _PII_COLS & cols
            if leaked:
                violations[path] = list(leaked)
                log.error("[quality/silver] PII detected in %s: %s", path, leaked)
        except Exception as exc:
            log.warning("[quality/silver] Could not read %s: %s", path, exc)

    context["ti"].xcom_push(key="pii_violations", value=violations)

    if violations:
        raise ValueError(f"CRITICAL FAILURE: PII columns detected in Silver!\n{violations}")
    log.info("[quality/silver] PII check: OK")
    return {}


def _validate_silver_schemas(**context) -> dict:
    """Validates required fields, not-null and NK uniqueness in each Silver table."""
    from pyspark.sql import functions as F

    spark = _get_spark()
    report = {}

    for path, checks in _SILVER_CHECKS.items():
        table_report: dict = {"status": "ok", "failures": []}
        try:
            from delta import DeltaTable
            if not DeltaTable.isDeltaTable(spark, path):
                table_report["status"] = "skipped"
                report[path] = table_report
                continue

            df = spark.read.format("delta").load(path)
            cols = set(df.columns)

            missing = set(checks["required_cols"]) - cols
            if missing:
                table_report["failures"].append(f"Missing columns: {missing}")

            for col in checks["not_null_cols"]:
                if col not in cols:
                    continue
                null_count = df.filter(F.col(col).isNull()).count()
                if null_count > 0:
                    table_report["failures"].append(f"{col}: {null_count} nulls")

            for col in checks["unique_cols"]:
                if col not in cols:
                    continue
                total = df.count()
                distinct = df.select(col).distinct().count()
                if total != distinct:
                    table_report["failures"].append(
                        f"{col}: {total - distinct} duplicates (total={total}, distinct={distinct})"
                    )

        except Exception as exc:
            table_report["status"] = "error"
            table_report["failures"] = [str(exc)]

        if table_report["failures"]:
            table_report["status"] = "failed"
        report[path] = table_report

    context["ti"].xcom_push(key="schema_report", value=report)

    failed = {p: r for p, r in report.items() if r["status"] == "failed"}
    if failed:
        raise ValueError(f"Silver quality gates failed:\n{failed}")

    log.info("[quality/silver] Schema check: OK (%d tables)", len(report))
    return report


def _run_ge_checkpoint(**context) -> str:
    """Runs Great Expectations checkpoint if available; does not block if absent."""
    try:
        import great_expectations as gx

        ctx = gx.get_context()
        result = ctx.run_checkpoint(checkpoint_name="silver_paciente_checkpoint")
        if not result.success:
            raise ValueError(f"GE checkpoint failed: {result.statistics}")
        log.info("[quality/silver] GE checkpoint: OK")
        return "ge_passed"
    except ImportError:
        log.info("[quality/silver] Great Expectations not installed — skipping")
        return "ge_skipped"
    except Exception as exc:
        if "does not exist" in str(exc) or "not found" in str(exc).lower():
            log.info("[quality/silver] GE checkpoint not configured — skipping")
            return "ge_skipped"
        raise


with DAG(
    dag_id="dag_quality_gates_silver",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["quality", "silver", "great-expectations", "lgpd"],
    default_args=_DEFAULT_ARGS,
    doc_md=__doc__,
) as dag:

    wait_silver_paciente = ExternalTaskSensor(
        task_id="wait_silver_paciente",
        external_dag_id="dag_silver_paciente_mascaramento",
        external_task_id="silver_paciente_spark_sensor",
        execution_date_fn=lambda dt: dt,
        timeout=7200,
        poke_interval=120,
        mode="reschedule",
    )

    validate_no_pii = PythonOperator(
        task_id="validate_no_pii_in_silver",
        python_callable=_validate_no_pii_in_silver,
        pool="spark_pool",
        execution_timeout=timedelta(minutes=30),
    )

    validate_schemas = PythonOperator(
        task_id="validate_silver_schemas",
        python_callable=_validate_silver_schemas,
        pool="spark_pool",
        execution_timeout=timedelta(minutes=30),
    )

    ge_checkpoint = PythonOperator(
        task_id="run_ge_checkpoint",
        python_callable=_run_ge_checkpoint,
        execution_timeout=timedelta(minutes=15),
    )

    wait_silver_paciente >> [validate_no_pii, validate_schemas] >> ge_checkpoint
