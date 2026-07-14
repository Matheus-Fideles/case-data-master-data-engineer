"""DAG Quality Gates — Silver.

Roda após cada job Silver e valida as suites de qualidade:

  bronze_suite: completude de campos-chave, schema mínimo, sem coluna PII exposta
  silver_suite: id_paciente_hash único (NK), tipos corretos, mascaramento confirmado

Falha aqui bloqueia downstream Gold via ExternalTaskSensor.

Implementação:
  - Usa Great Expectations se disponível (via checkpoints configurados)
  - Fallback: validações nativas Spark sem GE (garante funcionar sem GE instalado)

Spec: docs/specs/airflow-dags.md — seção "dag_quality_gates_silver"
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

# Colunas PII que não podem aparecer em nenhuma tabela Silver
_PII_COLS = {"cpf", "rg", "nome", "nome_completo", "data_nascimento", "email", "telefone"}

# Tabelas Silver e seus campos obrigatórios de qualidade
_SILVER_CHECKS = {
    "s3a://silver/notificacao/": {
        "required_cols": ["id_notificacao", "ano_mes", "_ingestion_ts"],
        "not_null_cols": ["id_notificacao", "ano_mes"],
        "unique_cols": [],
    },
    "s3a://silver/paciente/": {
        "required_cols": ["id_paciente_hash", "snapshot_date", "masking_version"],
        "not_null_cols": ["id_paciente_hash", "snapshot_date"],
        "unique_cols": ["id_paciente_hash"],  # NK deve ser único por snapshot
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
    """Verifica que nenhuma coluna PII existe em nenhuma tabela Silver."""
    from pyspark.sql import functions as F

    spark = _get_spark()
    violations = {}

    for path in _SILVER_CHECKS:
        try:
            from delta import DeltaTable
            if not DeltaTable.isDeltaTable(spark, path):
                log.warning("[quality/silver] %s não é Delta Table — pulando", path)
                continue

            df   = spark.read.format("delta").load(path)
            cols = set(df.columns)
            leaked = _PII_COLS & cols
            if leaked:
                violations[path] = list(leaked)
                log.error("[quality/silver] PII detectado em %s: %s", path, leaked)
        except Exception as e:
            log.warning("[quality/silver] Não foi possível ler %s: %s", path, e)

    context["ti"].xcom_push(key="pii_violations", value=violations)

    if violations:
        raise ValueError(
            f"FALHA CRÍTICA: colunas PII detectadas no Silver!\n{violations}"
        )
    log.info("[quality/silver] PII check: OK")
    return {}


def _validate_silver_schemas(**context) -> dict:
    """Valida campos obrigatórios, not-null e unicidade de NK em cada tabela Silver."""
    from pyspark.sql import functions as F

    spark = _get_spark()
    report = {}

    for path, checks in _SILVER_CHECKS.items():
        table_report = {"status": "ok", "failures": []}
        try:
            from delta import DeltaTable
            if not DeltaTable.isDeltaTable(spark, path):
                table_report["status"] = "skipped"
                report[path] = table_report
                continue

            df   = spark.read.format("delta").load(path)
            cols = set(df.columns)

            # Campos obrigatórios
            missing = set(checks["required_cols"]) - cols
            if missing:
                table_report["failures"].append(f"Campos ausentes: {missing}")

            # Not-null
            for col in checks["not_null_cols"]:
                if col not in cols:
                    continue
                n = df.filter(F.col(col).isNull()).count()
                if n > 0:
                    table_report["failures"].append(f"{col}: {n} nulos")

            # Unicidade de NK
            for col in checks["unique_cols"]:
                if col not in cols:
                    continue
                total    = df.count()
                distinct = df.select(col).distinct().count()
                if total != distinct:
                    table_report["failures"].append(
                        f"{col}: {total - distinct} duplicatas (total={total}, distinct={distinct})"
                    )

        except Exception as e:
            table_report["status"]   = "error"
            table_report["failures"] = [str(e)]

        if table_report["failures"]:
            table_report["status"] = "failed"
        report[path] = table_report

    context["ti"].xcom_push(key="schema_report", value=report)

    failed = {p: r for p, r in report.items() if r["status"] == "failed"}
    if failed:
        raise ValueError(f"Quality gates Silver falharam:\n{failed}")

    log.info("[quality/silver] Schema check: OK (%d tabelas)", len(report))
    return report


def _run_ge_checkpoint(**context) -> str:
    """Roda checkpoint Great Expectations se disponível; não bloqueia se ausente."""
    try:
        import great_expectations as gx

        ctx    = gx.get_context()
        result = ctx.run_checkpoint(checkpoint_name="silver_paciente_checkpoint")
        if not result.success:
            raise ValueError(f"GE checkpoint falhou: {result.statistics}")
        log.info("[quality/silver] GE checkpoint: OK")
        return "ge_passed"
    except ImportError:
        log.info("[quality/silver] Great Expectations não instalado — pulando")
        return "ge_skipped"
    except Exception as e:
        if "does not exist" in str(e) or "not found" in str(e).lower():
            log.info("[quality/silver] GE checkpoint não configurado — pulando")
            return "ge_skipped"
        raise


with DAG(
    dag_id="dag_quality_gates_silver",
    schedule_interval=None,   # triggera por ExternalTaskSensor dos Silver jobs
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["quality", "silver", "great-expectations", "lgpd"],
    default_args=_DEFAULT_ARGS,
    doc_md=__doc__,
) as dag:

    # Aguarda o Silver job de paciente (o mais crítico — tem mascaramento LGPD)
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
