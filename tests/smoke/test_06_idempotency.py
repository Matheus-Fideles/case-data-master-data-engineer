"""Smoke 06 — Idempotência de pipelines.

Reexecuta cada job (Bronze e Silver) com o mesmo partition_val
e verifica que a contagem final não muda.

Princípio: pipelines Delta com replaceWhere devem ser seguros de reexecutar.

Tempo alvo: < 90s
"""
from __future__ import annotations

import pytest

from tests.smoke.conftest import FIXTURES_DIR

pytestmark = pytest.mark.smoke


def _count(spark, path: str) -> int:
    return spark.read.format("delta").load(path).count()


# ── Bronze Arboviroses ─────────────────────────────────────────────────────────

def test_bronze_arboviroses_idempotent(spark_session, s3):
    """Três execuções do job Bronze dengue com mesmo partition_val → mesmo count."""
    from pipelines.batch.bronze_arboviroses import ArbovirosesBronzeJob

    fixture = FIXTURES_DIR / "dengue_sample.json"
    output  = "s3a://bronze/idempotency_dengue/"

    job = ArbovirosesBronzeJob()
    common = dict(
        spark=spark_session,
        input_path=str(fixture),
        output_path=output,
        partition_val="202401",
        source_url="smoke://idempotency/dengue",
    )

    job.run(**common, batch_id="idem-01")
    count_1 = _count(spark_session, output)

    job.run(**common, batch_id="idem-02")
    count_2 = _count(spark_session, output)

    job.run(**common, batch_id="idem-03")
    count_3 = _count(spark_session, output)

    assert count_1 == count_2 == count_3, \
        f"Bronze arboviroses não idempotente: {count_1} → {count_2} → {count_3}"


# ── Bronze CNES ───────────────────────────────────────────────────────────────

def test_bronze_cnes_idempotent(spark_session, s3):
    """Três execuções do job Bronze CNES → mesmo count."""
    from pipelines.batch.bronze_cnes import CnesBronzeJob

    fixture = FIXTURES_DIR / "cnes_sample.json"
    output  = "s3a://bronze/idempotency_cnes/"

    job = CnesBronzeJob()
    common = dict(
        spark=spark_session,
        input_path=str(fixture),
        output_path=output,
        partition_val="20240101",
        source_url="smoke://idempotency/cnes",
    )

    job.run(**common, batch_id="idem-cnes-01")
    count_1 = _count(spark_session, output)

    job.run(**common, batch_id="idem-cnes-02")
    count_2 = _count(spark_session, output)

    assert count_1 == count_2, \
        f"Bronze CNES não idempotente: {count_1} → {count_2}"


# ── Silver Paciente ────────────────────────────────────────────────────────────

def test_silver_paciente_idempotent(spark_session, s3):
    """Duas execuções do job Silver paciente → mesmo count."""
    fixture = FIXTURES_DIR / "oltp_sample.json"
    if not fixture.exists():
        pytest.skip("Fixture oltp_sample.json ausente")

    from pipelines.batch.silver_paciente import PacienteSilverJob

    output = "s3a://silver/idempotency_paciente/"
    job = PacienteSilverJob()
    common = dict(
        spark=spark_session,
        input_path=str(fixture),
        output_path=output,
        filter_col="snapshot_date",
        filter_val="20240101",
    )

    job.run(**common, batch_id="idem-silver-01")
    count_1 = _count(spark_session, output)

    job.run(**common, batch_id="idem-silver-02")
    count_2 = _count(spark_session, output)

    assert count_1 == count_2, \
        f"Silver paciente não idempotente: {count_1} → {count_2}"


# ── Múltiplas partições não interferem ────────────────────────────────────────

def test_different_partitions_accumulate(spark_session, s3):
    """Partições distintas devem acumular (não sobrescrever) no Delta."""
    from pipelines.batch.bronze_arboviroses import ArbovirosesBronzeJob

    fixture = FIXTURES_DIR / "dengue_sample.json"
    output  = "s3a://bronze/idempotency_multi_partition/"

    job = ArbovirosesBronzeJob()

    job.run(
        spark=spark_session,
        input_path=str(fixture),
        output_path=output,
        partition_val="202401",
        batch_id="multi-01",
        source_url="smoke://idempotency/multi",
    )
    count_jan = _count(spark_session, output)

    job.run(
        spark=spark_session,
        input_path=str(fixture),
        output_path=output,
        partition_val="202402",
        batch_id="multi-02",
        source_url="smoke://idempotency/multi",
    )
    count_jan_feb = _count(spark_session, output)

    assert count_jan_feb == count_jan * 2, \
        f"Partições deveriam acumular: 1×{count_jan} + 1×{count_jan} ≠ {count_jan_feb}"
