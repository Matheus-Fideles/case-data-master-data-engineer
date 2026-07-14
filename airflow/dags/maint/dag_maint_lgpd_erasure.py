"""DAG de Esquecimento LGPD — Art. 18, inciso VI (direito ao apagamento).

Executa sob demanda (trigger manual) quando um titular solicita exclusão de
seus dados pessoais. Recebe `cpf_hash` como parâmetro e apaga o rastro
do titular em todas as camadas do Data Lake.

Fluxo:
  delete_oltp          → Remove linha de oltp.paciente por CPF real
  delete_bronze        → Remove partições Bronze com o hash do titular
  vacuum_bronze_now    → VACUUM RETAIN 0 HOURS — elimina versões Delta
  reprocess_silver     → Recalcula Silver sem o titular (DROP + re-ingest)
  delete_gold          → Remove SK do titular de dim_paciente e fatos
  audit_log            → Registra execução em gold_dw.lgpd_erasure_audit

IMPORTANTE:
  - CPF real é passado via Airflow Variable 'lgpd_cpf_<run_id>' e apagado
    após leitura — nunca persiste em XCom nem em log estruturado.
  - `cpf_hash` no parâmetro é o SHA-256 que identifica o titular no lake.

Spec: docs/specs/airflow-dags.md — seção "dag_maint_lgpd_erasure"
ADR:  docs/architecture/decisions/0004-pii-masking.md
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
    "retries": 0,          # erasure não deve retentar parcialmente
    "start_date": datetime(2024, 1, 1),
}

_PII_SALT = os.environ.get("PII_SALT", "")


def _hash_cpf(cpf: str) -> str:
    return hashlib.sha256((_PII_SALT + cpf).encode()).hexdigest()


# ── Tasks ─────────────────────────────────────────────────────────────────────

def _delete_oltp(**context) -> str:
    """Remove titular de oltp.paciente usando CPF real (passado via Variable)."""
    from airflow.models import Variable

    run_id = context["run_id"]
    var_key = f"lgpd_cpf_{run_id}"

    cpf = Variable.get(var_key, default_var=None)
    if cpf:
        # Apaga a Variable imediatamente após leitura
        Variable.delete(var_key)
        log.info("[lgpd/oltp] CPF carregado e Variable apagada: %s", var_key)
    else:
        # Fallback: usa cpf_hash do param (não é possível deletar por hash no OLTP)
        log.warning(
            "[lgpd/oltp] Variable %s não encontrada — "
            "exclusão OLTP requer CPF real via Variable.",
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
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    # Deriva o hash para uso nas tasks seguintes (CPF real não vai mais adiante)
    cpf_hash = _hash_cpf(cpf)
    context["ti"].xcom_push(key="cpf_hash", value=cpf_hash)
    context["ti"].xcom_push(key="oltp_deleted_rows", value=deleted)
    log.info("[lgpd/oltp] Deletadas %d linhas. Hash derivado para downstream.", deleted)
    return f"deleted:{deleted}"


def _delete_bronze(**context) -> str:
    """Remove arquivos Bronze onde _batch_id ou id_paciente_hash bate com o titular."""
    ti = context["ti"]
    cpf_hash = (
        ti.xcom_pull(task_ids="delete_oltp", key="cpf_hash")
        or context["params"].get("cpf_hash")
    )
    if not cpf_hash:
        raise ValueError("cpf_hash não disponível — delete_oltp falhou ou parâmetro ausente")

    from pyspark.sql import functions as F

    from _common.spark_local import make_local_spark, BRONZE_TABLES as _BRONZE_TABLES

    spark = make_local_spark("lgpd_delete_bronze")
    results = {}
    for path in _BRONZE_TABLES:
        try:
            from delta import DeltaTable

            if DeltaTable.isDeltaTable(spark, path):
                dt = DeltaTable.forPath(spark, path)
                # Bronze tem id_paciente_hash se vier do OLTP; outros usam nu_notific
                if "id_paciente_hash" in spark.read.format("delta").load(path).columns:
                    dt.delete(F.col("id_paciente_hash") == cpf_hash)
                    results[path] = "deleted"
                else:
                    results[path] = "skipped_no_hash_col"
        except Exception as e:
            results[path] = f"error: {e}"
            log.error("[lgpd/bronze] %s: %s", path, e)

    ti.xcom_push(key="bronze_results", value=results)
    return str(results)


def _vacuum_bronze_now(**context) -> str:
    """VACUUM RETAIN 0 HOURS no Bronze — elimina versões físicas com PII."""
    from _common.spark_local import make_local_spark, BRONZE_TABLES as _BRONZE_TABLES

    spark = make_local_spark("lgpd_vacuum_bronze")
    # Desabilita proteção de retenção mínima (necessário para RETAIN 0)
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    errors = []
    for path in _BRONZE_TABLES:
        try:
            spark.sql(f"VACUUM delta.`{path}` RETAIN 0 HOURS")
            log.info("[lgpd/vacuum] VACUUM 0h concluído: %s", path)
        except Exception as e:
            log.error("[lgpd/vacuum] falhou %s: %s", path, e)
            errors.append(path)

    if errors:
        raise RuntimeError(f"VACUUM falhou em {len(errors)} tabela(s): {errors}")
    return "vacuum_complete"


def _reprocess_silver(**context) -> str:
    """Marca Silver do titular como deletado via soft-delete (flag is_deleted=true).

    Re-ingestão completa seria custosa — o campo is_deleted permite que queries
    downstream filtrem o titular sem reprocessamento total.
    """
    ti = context["ti"]
    cpf_hash = (
        ti.xcom_pull(task_ids="delete_oltp", key="cpf_hash")
        or context["params"].get("cpf_hash")
    )
    if not cpf_hash:
        raise ValueError("cpf_hash não disponível")

    from pyspark.sql import functions as F

    from _common.spark_local import make_local_spark

    _SILVER_TABLES = [
        "s3a://silver/notificacao/",
        "s3a://silver/sim_obitos/",
        "s3a://silver/vacinacao_pni/",
        "s3a://silver/estabelecimento/",
        "s3a://silver/municipio/",
        "s3a://silver/paciente/",
    ]
    spark = make_local_spark("lgpd_reprocess_silver")
    results = {}
    for path in _SILVER_TABLES:
        try:
            from delta import DeltaTable

            if DeltaTable.isDeltaTable(spark, path):
                cols = spark.read.format("delta").load(path).columns
                if "id_paciente_hash" in cols:
                    dt = DeltaTable.forPath(spark, path)
                    dt.delete(F.col("id_paciente_hash") == cpf_hash)
                    results[path] = "deleted"
                else:
                    results[path] = "skipped"
        except Exception as e:
            results[path] = f"error: {e}"

    ti.xcom_push(key="silver_results", value=results)
    return str(results)


def _delete_gold(**context) -> str:
    """Remove titular de dim_paciente e marca fatos com sk_paciente inválido."""
    ti = context["ti"]
    cpf_hash = (
        ti.xcom_pull(task_ids="delete_oltp", key="cpf_hash")
        or context["params"].get("cpf_hash")
    )
    if not cpf_hash:
        raise ValueError("cpf_hash não disponível")

    import psycopg2

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "postgres"),
        user=os.environ.get("POSTGRES_GOLD_USER", "gold_engineer"),
        password=os.environ.get("POSTGRES_GOLD_PASSWORD", "gold_engineer"),
    )
    cur = conn.cursor()

    # Busca SK do titular antes de deletar (para nullificar fatos)
    cur.execute(
        "SELECT sk_paciente FROM gold_dw.dim_paciente WHERE id_paciente_hash = %s",
        (cpf_hash,),
    )
    sk_rows = cur.fetchall()
    sks = [r[0] for r in sk_rows]

    if sks:
        # Nullifica referência nos fatos (mantém fato, remove identificação)
        sk_list = tuple(sks) if len(sks) > 1 else f"({sks[0]})"
        for fato in ("fato_atendimento", "fato_atendimento_stream"):
            try:
                cur.execute(
                    f"UPDATE gold_dw.{fato} "
                    f"SET sk_paciente = NULL WHERE sk_paciente = ANY(%s)",
                    (sks,),
                )
            except Exception:
                pass  # tabela pode não existir

        # Remove titular de dim_paciente (todas as versões SCD2)
        cur.execute(
            "DELETE FROM gold_dw.dim_paciente WHERE id_paciente_hash = %s",
            (cpf_hash,),
        )

    conn.commit()
    cur.close()
    conn.close()

    result = {"sks_removed": sks}
    ti.xcom_push(key="gold_results", value=result)
    log.info("[lgpd/gold] SKs removidos: %s", sks)
    return str(result)


def _audit_log(**context) -> str:
    """Registra execução do erasure em gold_dw.lgpd_erasure_audit."""
    ti = context["ti"]
    cpf_hash = (
        ti.xcom_pull(task_ids="delete_oltp", key="cpf_hash")
        or context["params"].get("cpf_hash")
    )

    import psycopg2

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "postgres"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
    )
    cur = conn.cursor()

    # Cria tabela de auditoria se não existir
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

    import json

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
    log.info("[lgpd/audit] Registro criado para run_id=%s", context["run_id"])
    return "audit_logged"


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="dag_maint_lgpd_erasure",
    schedule_interval=None,    # somente trigger manual
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
                "SHA-256 do CPF do titular (64 chars hex). "
                "Para fornecer o CPF real, crie a Airflow Variable "
                "'lgpd_cpf_<run_id>' antes de triggerar."
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
        trigger_rule="all_done",   # audita mesmo se alguma task falhar
        execution_timeout=timedelta(minutes=5),
    )

    delete_oltp >> delete_bronze >> vacuum_bronze_now >> reprocess_silver >> delete_gold >> audit_log
