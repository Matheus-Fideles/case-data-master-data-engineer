"""Carga SCD Type 2 de dim_paciente no Gold DW.

Lê silver/paciente/ filtrado por snapshot_date e aplica lógica SCD2
diretamente no Postgres via psycopg2 (sem Spark — dado já está mascarado
e o volume por snapshot é pequeno o suficiente para carga direta).

Algoritmo SCD2:
  1. Para cada hash no Silver:
     a. Se não existe em dim_paciente → INSERT (dt_inicio=hoje, dt_fim=9999-12-31)
     b. Se existe mas atributos mudaram → fechar versão atual + INSERT nova
     c. Se existe e igual → skip
  2. Retorna número de linhas inseridas

ADR: docs/architecture/decisions/0006-scd2.md
"""
from __future__ import annotations

import logging
import os
from datetime import date

log = logging.getLogger(__name__)

_OPEN_DATE = date(9999, 12, 31)

# Atributos que, se mudarem, geram nova versão SCD2
_SCD2_ATTRS = ("sexo", "ano_nascimento", "cep_regiao", "municipio_codigo_ibge")


def _pg_conn(gold: bool = False):
    import psycopg2

    user     = os.environ.get("POSTGRES_GOLD_USER", "gold_engineer") if gold else os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_GOLD_PASSWORD", "gold_engineer") if gold else os.environ.get("POSTGRES_PASSWORD", "postgres")
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "postgres"),
        user=user,
        password=password,
        connect_timeout=10,
    )


def _read_silver(snapshot_date: str) -> list[dict]:
    """Lê silver/paciente/ para a data de snapshot via S3/Delta ou arquivo local."""
    silver_path = os.environ.get("SILVER_PACIENTE_PATH", "s3a://silver/paciente/")

    try:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession, functions as F

        spark = (
            configure_spark_with_delta_pip(
                SparkSession.builder.appName("gold_dim_paciente")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT", "http://minio:9000"))
                .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", "minioadmin"))
                .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"))
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .master("local[2]")
            ).getOrCreate()
        )

        df = (
            spark.read.format("delta").load(silver_path)
            .filter(F.col("snapshot_date") == snapshot_date)
            .select("id_paciente_hash", "sexo", "ano_nascimento", "cep_regiao", "municipio_codigo_ibge")
        )
        return [row.asDict() for row in df.collect()]

    except Exception as e:
        log.warning("[dim_paciente] Falha ao ler via Spark (%s) — tentando JSON local", e)

    # Fallback: lê fixture local (útil em testes sem Spark)
    import json
    from pathlib import Path

    fixture = Path(__file__).parents[2] / "tests" / "fixtures" / "oltp_sample.json"
    if fixture.exists():
        records = json.loads(fixture.read_text())
        return [
            {
                "id_paciente_hash": r.get("id_paciente_hash", f"hash_{r['id_paciente']}"),
                "sexo": r.get("sexo"),
                "ano_nascimento": int(r["data_nascimento"][:4]) if r.get("data_nascimento") else None,
                "cep_regiao": r.get("cep", "")[:3] if r.get("cep") else None,
                "municipio_codigo_ibge": r.get("municipio_codigo_ibge"),
            }
            for r in records
        ]
    return []


def load_dim_paciente(snapshot_date: str | None = None) -> int:
    """Executa carga SCD2 de dim_paciente para o snapshot_date informado.

    Retorna o número de linhas inseridas (novas versões).
    """
    snap = snapshot_date or date.today().strftime("%Y%m%d")
    today = date.today()

    silver_rows = _read_silver(snap)
    if not silver_rows:
        log.warning("[dim_paciente] Nenhuma linha no Silver para snapshot_date=%s", snap)
        return 0

    conn = _pg_conn(gold=True)
    cur  = conn.cursor()
    inserted = 0

    try:
        for row in silver_rows:
            hash_val = row["id_paciente_hash"]

            # Busca versão atual
            cur.execute(
                """
                SELECT sk_paciente, sexo, ano_nascimento, cep_regiao, municipio_codigo_ibge
                FROM gold_dw.dim_paciente
                WHERE id_paciente_hash = %s AND is_current = TRUE
                """,
                (hash_val,),
            )
            existing = cur.fetchone()

            if existing is None:
                # Novo titular — INSERT direto
                cur.execute(
                    """
                    INSERT INTO gold_dw.dim_paciente
                        (id_paciente_hash, sexo, ano_nascimento, cep_regiao,
                         municipio_codigo_ibge, dt_inicio, dt_fim, is_current, _batch_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE, %s)
                    """,
                    (
                        hash_val,
                        row.get("sexo"),
                        row.get("ano_nascimento"),
                        row.get("cep_regiao"),
                        row.get("municipio_codigo_ibge"),
                        today,
                        _OPEN_DATE,
                        snap,
                    ),
                )
                inserted += 1

            else:
                # Verifica se algum atributo mudou
                _, ex_sexo, ex_ano, ex_cep, ex_mun = existing
                changed = (
                    ex_sexo != row.get("sexo")
                    or ex_ano != row.get("ano_nascimento")
                    or ex_cep != row.get("cep_regiao")
                    or ex_mun != row.get("municipio_codigo_ibge")
                )

                if changed:
                    # Fecha versão atual
                    cur.execute(
                        """
                        UPDATE gold_dw.dim_paciente
                        SET dt_fim = %s, is_current = FALSE
                        WHERE id_paciente_hash = %s AND is_current = TRUE
                        """,
                        (today - __import__("datetime").timedelta(days=1), hash_val),
                    )
                    # Insere nova versão
                    cur.execute(
                        """
                        INSERT INTO gold_dw.dim_paciente
                            (id_paciente_hash, sexo, ano_nascimento, cep_regiao,
                             municipio_codigo_ibge, dt_inicio, dt_fim, is_current, _batch_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE, %s)
                        """,
                        (
                            hash_val,
                            row.get("sexo"),
                            row.get("ano_nascimento"),
                            row.get("cep_regiao"),
                            row.get("municipio_codigo_ibge"),
                            today,
                            _OPEN_DATE,
                            snap,
                        ),
                    )
                    inserted += 1

        conn.commit()
        log.info("[dim_paciente] SCD2 concluído: %d versões inseridas (snapshot=%s)", inserted, snap)
        return inserted

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
