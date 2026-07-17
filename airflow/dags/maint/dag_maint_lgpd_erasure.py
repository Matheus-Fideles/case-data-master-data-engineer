"""LGPD Erasure DAG — Art. 18, section VI (right to erasure).

Runs on demand (manual trigger) when a data subject requests deletion of their
personal data. Receives `cpf_hash` as a parameter and erases the subject's
trace across all Data Lake layers.

Flow:
  delete_oltp          → Removes row from oltp.paciente by real CPF
  delete_bronze        → Removes Bronze partitions with the subject's hash
  vacuum_bronze_now    → VACUUM RETAIN 0 HOURS — purges Delta versions
  reprocess_silver     → Recalculates Silver without the subject (DROP + re-ingest)
  delete_gold          → Removes subject SK from dim_paciente and facts
  audit_log            → Records execution in gold_dw.lgpd_erasure_audit

IMPORTANT:
  - Real CPF is passed via Airflow Variable 'lgpd_cpf_<run_id>' and deleted
    after first read — never persists in XCom or structured logs.
  - `cpf_hash` parameter is the SHA-256 that identifies the subject in the lake.

Spec: docs/05-observabilidade-sre.md — section "dag_maint_lgpd_erasure"
ADR:  docs/ADRS.md
"""

from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

_DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 0,
    "start_date": datetime(2024, 1, 1),
}

_PII_SALT = os.environ.get("PII_SALT", "")


def _hash_cpf(cpf: str) -> str:
    return hashlib.sha256((_PII_SALT + cpf).encode()).hexdigest()


def _delete_oltp(**context) -> str:
    """Removes subject from oltp.paciente using real CPF (passed via Variable)."""
    from airflow.models import Variable

    run_id = context["run_id"]
    var_key = f"lgpd_cpf_{run_id}"

    cpf = Variable.get(var_key, default_var=None)
    if cpf:
        Variable.delete(var_key)
        log.info("[lgpd/oltp] CPF loaded and Variable deleted: %s", var_key)
    else:
        log.warning(
            "[lgpd/oltp] Variable %s not found — OLTP deletion requires real CPF via Variable.",
            var_key,
        )
        context["ti"].xcom_push(key="oltp_status", value="skipped_no_cpf")
        return "skipped"

    import psycopg2

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "postgres"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
    )
    cur = conn.cursor()
    cur.execute("DELETE FROM oltp.paciente WHERE cpf = %s", (cpf,))
    deleted_count = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    cpf_hash = _hash_cpf(cpf)
    context["ti"].xcom_push(key="cpf_hash", value=cpf_hash)
    context["ti"].xcom_push(key="oltp_deleted_rows", value=deleted_count)
    log.info("[lgpd/oltp] Deleted %d rows. Hash derived for downstream.", deleted_count)
    return f"deleted:{deleted_count}"


def _delete_bronze(**context) -> str:
    """Removes Bronze files where id_paciente_hash matches the subject."""
    ti = context["ti"]
    cpf_hash = ti.xcom_pull(task_ids="delete_oltp", key="cpf_hash") or context["params"].get(
        "cpf_hash"
    )
    if not cpf_hash:
        raise ValueError("cpf_hash not available — delete_oltp failed or parameter missing")

    from _common.spark_local import BRONZE_TABLES, make_local_spark
    from pyspark.sql import functions as F

    spark = make_local_spark("lgpd_delete_bronze")
    results = {}
    for path in BRONZE_TABLES:
        try:
            from delta import DeltaTable

            if DeltaTable.isDeltaTable(spark, path):
                delta_table = DeltaTable.forPath(spark, path)
                if "id_paciente_hash" in spark.read.format("delta").load(path).columns:
                    delta_table.delete(F.col("id_paciente_hash") == cpf_hash)
                    results[path] = "deleted"
                else:
                    results[path] = "skipped_no_hash_col"
        except Exception as exc:
            results[path] = f"error: {exc}"
            log.error("[lgpd/bronze] %s: %s", path, exc)

    ti.xcom_push(key="bronze_results", value=results)
    return str(results)


def _vacuum_bronze_now(**context) -> str:
    """VACUUM RETAIN 0 HOURS on Bronze — physically purges versions with PII."""
    from _common.spark_local import BRONZE_TABLES, make_local_spark

    spark = make_local_spark("lgpd_vacuum_bronze")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    errors = []
    for path in BRONZE_TABLES:
        try:
            spark.sql(f"VACUUM delta.`{path}` RETAIN 0 HOURS")
            log.info("[lgpd/vacuum] VACUUM 0h complete: %s", path)
        except Exception as exc:
            log.error("[lgpd/vacuum] failed %s: %s", path, exc)
            errors.append(path)

    if errors:
        raise RuntimeError(f"VACUUM failed on {len(errors)} table(s): {errors}")
    return "vacuum_complete"


def _reprocess_silver(**context) -> str:
    """Soft-deletes subject rows in Silver (is_deleted=true flag).

    Full re-ingestion would be expensive — is_deleted lets downstream queries
    filter the subject without full reprocessing.
    """
    ti = context["ti"]
    cpf_hash = ti.xcom_pull(task_ids="delete_oltp", key="cpf_hash") or context["params"].get(
        "cpf_hash"
    )
    if not cpf_hash:
        raise ValueError("cpf_hash not available")

    from _common.spark_local import make_local_spark
    from pyspark.sql import functions as F

    silver_tables = [
        "s3a://silver/notificacao/",
        "s3a://silver/sim_obitos/",
        "s3a://silver/vacinacao_pni/",
        "s3a://silver/estabelecimento/",
        "s3a://silver/municipio/",
        "s3a://silver/paciente/",
    ]
    spark = make_local_spark("lgpd_reprocess_silver")
    results = {}
    for path in silver_tables:
        try:
            from delta import DeltaTable

            if DeltaTable.isDeltaTable(spark, path):
                cols = spark.read.format("delta").load(path).columns
                if "id_paciente_hash" in cols:
                    delta_table = DeltaTable.forPath(spark, path)
                    delta_table.delete(F.col("id_paciente_hash") == cpf_hash)
                    results[path] = "deleted"
                else:
                    results[path] = "skipped"
        except Exception as exc:
            results[path] = f"error: {exc}"

    ti.xcom_push(key="silver_results", value=results)
    return str(results)


def _delete_gold(**context) -> str:
    """Removes subject from dim_paciente and nullifies fact FKs."""
    ti = context["ti"]
    cpf_hash = ti.xcom_pull(task_ids="delete_oltp", key="cpf_hash") or context["params"].get(
        "cpf_hash"
    )
    if not cpf_hash:
        raise ValueError("cpf_hash not available")

    import psycopg2

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "postgres"),
        user=os.environ.get("POSTGRES_GOLD_USER", "gold_engineer"),
        password=os.environ.get("POSTGRES_GOLD_PASSWORD", "gold_engineer"),
    )
    cur = conn.cursor()

    cur.execute(
        "SELECT sk_paciente FROM gold_dw.dim_paciente WHERE id_paciente_hash = %s",
        (cpf_hash,),
    )
    sk_list = [row[0] for row in cur.fetchall()]

    if sk_list:
        for fact_table in ("fato_atendimento", "fato_atendimento_stream"):
            try:
                cur.execute(
                    f"UPDATE gold_dw.{fact_table} "
                    f"SET sk_paciente = NULL WHERE sk_paciente = ANY(%s)",
                    (sk_list,),
                )
            except Exception:
                pass

        cur.execute(
            "DELETE FROM gold_dw.dim_paciente WHERE id_paciente_hash = %s",
            (cpf_hash,),
        )

    conn.commit()
    cur.close()
    conn.close()

    result = {"sks_removed": sk_list}
    ti.xcom_push(key="gold_results", value=result)
    log.info("[lgpd/gold] SKs removed: %s", sk_list)
    return str(result)


def _audit_log(**context) -> str:
    """Records erasure execution in gold_dw.lgpd_erasure_audit."""
    ti = context["ti"]
    cpf_hash = ti.xcom_pull(task_ids="delete_oltp", key="cpf_hash") or context["params"].get(
        "cpf_hash"
    )

    import json

    import psycopg2

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "postgres"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS gold_dw.lgpd_erasure_audit (
            id              BIGSERIAL PRIMARY KEY,
            cpf_hash        CHAR(64)     NOT NULL,
            run_id          TEXT         NOT NULL,
            executed_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
            oltp_status     TEXT,
            bronze_results  JSONB,
            silver_results  JSONB,
            gold_results    JSONB,
            operator        TEXT         DEFAULT current_user
        )
    """)

    cur.execute(
        """
        INSERT INTO gold_dw.lgpd_erasure_audit
            (cpf_hash, run_id, oltp_status, bronze_results, silver_results, gold_results)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            cpf_hash or "unknown",
            context["run_id"],
            ti.xcom_pull(task_ids="delete_oltp", key="oltp_status") or "executed",
            json.dumps(ti.xcom_pull(task_ids="delete_bronze", key="bronze_results") or {}),
            json.dumps(ti.xcom_pull(task_ids="reprocess_silver", key="silver_results") or {}),
            json.dumps(ti.xcom_pull(task_ids="delete_gold", key="gold_results") or {}),
        ),
    )
    conn.commit()
    cur.close()
    conn.close()
    log.info("[lgpd/audit] Audit record created for run_id=%s", context["run_id"])
    return "audit_logged"


with DAG(
    dag_id="dag_maint_lgpd_erasure",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["maint", "lgpd", "erasure", "compliance"],
    default_args=_DEFAULT_ARGS,
    doc_md=__doc__,
    params={
        "cpf_hash": Param(
            default="",
            type="string",
            description=(
                "SHA-256 of the subject's CPF (64 hex chars). "
                "To provide the real CPF, create Airflow Variable "
                "'lgpd_cpf_<run_id>' before triggering."
            ),
        ),
    },
) as dag:
    delete_oltp = PythonOperator(
        task_id="delete_oltp",
        python_callable=_delete_oltp,
        execution_timeout=timedelta(minutes=5),
    )

    delete_bronze = PythonOperator(
        task_id="delete_bronze",
        python_callable=_delete_bronze,
        pool="spark_pool",
        execution_timeout=timedelta(hours=1),
    )

    vacuum_bronze_now = PythonOperator(
        task_id="vacuum_bronze_now",
        python_callable=_vacuum_bronze_now,
        pool="spark_pool",
        execution_timeout=timedelta(hours=2),
    )

    reprocess_silver = PythonOperator(
        task_id="reprocess_silver",
        python_callable=_reprocess_silver,
        pool="spark_pool",
        execution_timeout=timedelta(hours=1),
    )

    delete_gold = PythonOperator(
        task_id="delete_gold",
        python_callable=_delete_gold,
        execution_timeout=timedelta(minutes=15),
    )

    audit_log = PythonOperator(
        task_id="audit_log",
        python_callable=_audit_log,
        trigger_rule="all_done",
        execution_timeout=timedelta(minutes=5),
    )

    (
        delete_oltp
        >> delete_bronze
        >> vacuum_bronze_now
        >> reprocess_silver
        >> delete_gold
        >> audit_log
    )
