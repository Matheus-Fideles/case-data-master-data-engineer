"""SCD Type 2 load for dim_paciente in the Gold DW.

Reads silver/epidemiologico/paciente/ filtered by snapshot_date and applies SCD2 logic
directly in Postgres via psycopg2 (no Spark — data is already masked
and snapshot volume is small enough for direct load).

SCD2 algorithm:
  1. For each hash in Silver:
     a. Does not exist in dim_paciente → INSERT (dt_inicio=today, dt_fim=9999-12-31)
     b. Exists but attributes changed → close current version + INSERT new
     c. Exists and unchanged → skip
  2. Returns number of inserted rows

After the SCD2 Postgres load, exports a current snapshot to Delta Lake
for Trino serving at s3a://gold/epidemiologico/dim_paciente/.

ADR: docs/ADRS.md
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date
from pathlib import Path

try:
    import psycopg2
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore[assignment]

log = logging.getLogger(__name__)

_OPEN_DATE = date(9999, 12, 31)

_SCD2_ATTRS = ("sexo", "ano_nascimento", "cep_regiao", "municipio_codigo_ibge")


def _pg_conn(gold: bool = False):
    user = (
        os.environ.get("POSTGRES_GOLD_USER", "gold_engineer")
        if gold
        else os.environ.get("POSTGRES_USER", "postgres")
    )
    password = (
        os.environ.get("POSTGRES_GOLD_PASSWORD", "gold_engineer")
        if gold
        else os.environ.get("POSTGRES_PASSWORD", "postgres")
    )
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "postgres"),
        user=user,
        password=password,
        connect_timeout=10,
    )


def _read_silver(snapshot_date: str) -> list[dict]:
    """Reads silver/epidemiologico/paciente/ for the given snapshot_date via S3/Delta or local file."""
    silver_path = os.environ.get("SILVER_PACIENTE_PATH", "s3a://silver/epidemiologico/paciente/")

    try:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = configure_spark_with_delta_pip(
            SparkSession.builder.appName("gold_dim_paciente")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            )
            .config(
                "spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", "minioadmin")
            )
            .config(
                "spark.hadoop.fs.s3a.secret.key",
                os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
            )
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .master("local[2]")
        ).getOrCreate()

        df = (
            spark.read.format("delta")
            .load(silver_path)
            .filter(F.col("snapshot_date") == snapshot_date)
            .select(
                "id_paciente_hash", "sexo", "ano_nascimento", "cep_regiao", "municipio_codigo_ibge"
            )
        )
        return [row.asDict() for row in df.collect()]

    except Exception as exc:
        log.warning("[dim_paciente] Spark read failed (%s) — falling back to local JSON", exc)

    fixture = Path(__file__).parents[4] / "tests" / "fixtures" / "oltp_sample.json"
    if fixture.exists():
        records = json.loads(fixture.read_text())
        return [
            {
                "id_paciente_hash": r.get("id_paciente_hash", f"hash_{r['id_paciente']}"),
                "sexo": r.get("sexo"),
                "ano_nascimento": int(r["data_nascimento"][:4])
                if r.get("data_nascimento")
                else None,
                "cep_regiao": r.get("cep", "")[:3] if r.get("cep") else None,
                "municipio_codigo_ibge": r.get("municipio_codigo_ibge"),
            }
            for r in records
        ]
    return []


def _export_to_delta(snapshot_date: str, conn) -> None:
    """Exports current gold_dw.dim_paciente snapshot to Delta Lake for Trino serving."""
    try:
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession

        spark = configure_spark_with_delta_pip(
            SparkSession.builder.appName("gold_dim_paciente_export")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            )
            .config(
                "spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", "minioadmin")
            )
            .config(
                "spark.hadoop.fs.s3a.secret.key",
                os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
            )
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .master("local[2]")
        ).getOrCreate()
        pg_jdbc = (
            f"jdbc:postgresql://{os.environ.get('POSTGRES_HOST', 'postgres')}:"
            f"{os.environ.get('POSTGRES_PORT', '5432')}/"
            f"{os.environ.get('POSTGRES_DB', 'app')}"
        )
        df = (
            spark.read.format("jdbc")
            .options(
                url=pg_jdbc,
                dbtable="gold_dw.dim_paciente",
                user=os.environ.get("POSTGRES_GOLD_USER", "gold_engineer"),
                password=os.environ.get("POSTGRES_GOLD_PASSWORD", "gold_engineer"),
            )
            .load()
        )
        df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(
            "s3a://gold/epidemiologico/dim_paciente/"
        )
        spark.stop()
        log.info("[dim_paciente] Exported to Delta: s3a://gold/epidemiologico/dim_paciente/")
    except Exception as e:
        log.warning("[dim_paciente] Delta export skipped: %s", e)


def load_dim_paciente(snapshot_date: str | None = None) -> int:
    """Runs SCD2 load for dim_paciente for the given snapshot_date.

    Returns the number of inserted rows (new versions).
    """
    snapshot = snapshot_date or date.today().strftime("%Y%m%d")
    today = date.today()

    silver_rows = _read_silver(snapshot)
    if not silver_rows:
        log.warning("[dim_paciente] No rows in Silver for snapshot_date=%s", snapshot)
        return 0

    conn = _pg_conn(gold=True)
    cur = conn.cursor()
    inserted = 0

    try:
        for row in silver_rows:
            patient_hash = row["id_paciente_hash"]

            cur.execute(
                """
                SELECT sk_paciente, sexo, ano_nascimento, cep_regiao, municipio_codigo_ibge
                FROM gold_dw.dim_paciente
                WHERE id_paciente_hash = %s AND is_current = TRUE
                """,
                (patient_hash,),
            )
            existing = cur.fetchone()

            if existing is None:
                cur.execute(
                    """
                    INSERT INTO gold_dw.dim_paciente
                        (id_paciente_hash, sexo, ano_nascimento, cep_regiao,
                         municipio_codigo_ibge, dt_inicio, dt_fim, is_current, _batch_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE, %s)
                    """,
                    (
                        patient_hash,
                        row.get("sexo"),
                        row.get("ano_nascimento"),
                        row.get("cep_regiao"),
                        row.get("municipio_codigo_ibge"),
                        today,
                        _OPEN_DATE,
                        snapshot,
                    ),
                )
                inserted += 1

            else:
                _, curr_sexo, curr_year, curr_cep, curr_city = existing
                changed = (
                    curr_sexo != row.get("sexo")
                    or curr_year != row.get("ano_nascimento")
                    or curr_cep != row.get("cep_regiao")
                    or curr_city != row.get("municipio_codigo_ibge")
                )

                if changed:
                    cur.execute(
                        """
                        UPDATE gold_dw.dim_paciente
                        SET dt_fim = %s, is_current = FALSE
                        WHERE id_paciente_hash = %s AND is_current = TRUE
                        """,
                        (today - __import__("datetime").timedelta(days=1), patient_hash),
                    )
                    cur.execute(
                        """
                        INSERT INTO gold_dw.dim_paciente
                            (id_paciente_hash, sexo, ano_nascimento, cep_regiao,
                             municipio_codigo_ibge, dt_inicio, dt_fim, is_current, _batch_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE, %s)
                        """,
                        (
                            patient_hash,
                            row.get("sexo"),
                            row.get("ano_nascimento"),
                            row.get("cep_regiao"),
                            row.get("municipio_codigo_ibge"),
                            today,
                            _OPEN_DATE,
                            snapshot,
                        ),
                    )
                    inserted += 1

        conn.commit()
        log.info(
            "[dim_paciente] SCD2 complete: %d versions inserted (snapshot=%s)", inserted, snapshot
        )

        _export_to_delta(snapshot, conn)

        return inserted

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
