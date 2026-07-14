"""Smoke 04 — SCD Type 2 Gold DW.

Verifies SCD2 integrity on the dim_paciente dimension:
  1. First load: all records have dt_fim = '9999-12-31' (current)
  2. Second load with 5 updates: old versions closed, new versions opened
  3. No record has dt_inicio > dt_fim
  4. Overlap detection: no two current records for the same hash

Target time: < 90s
"""

from __future__ import annotations

from datetime import date

import pytest

from tests.smoke.conftest import FIXTURES_DIR

pytestmark = pytest.mark.smoke


# ── Helpers ───────────────────────────────────────────────────────────────────


def _count_dim(conn, schema="gold_dw", table="dim_paciente") -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
    n = cur.fetchone()[0]
    cur.close()
    return n


def _count_current(conn) -> int:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM gold_dw.dim_paciente WHERE dt_fim = '9999-12-31'")
    n = cur.fetchone()[0]
    cur.close()
    return n


def _count_closed(conn) -> int:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM gold_dw.dim_paciente WHERE dt_fim < '9999-12-31'")
    n = cur.fetchone()[0]
    cur.close()
    return n


# ── Structure tests ───────────────────────────────────────────────────────────


def test_dim_paciente_table_exists(pg_gold):
    """dim_paciente must exist in the gold_dw schema."""
    cur = pg_gold.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'gold_dw' AND table_name = 'dim_paciente'
    """)
    assert cur.fetchone() is not None, "Table gold_dw.dim_paciente does not exist"
    cur.close()


def test_dim_paciente_scd2_columns(pg_gold):
    """Mandatory SCD2 columns must exist."""
    cur = pg_gold.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'gold_dw' AND table_name = 'dim_paciente'
    """)
    cols = {row[0] for row in cur.fetchall()}
    cur.close()

    required = {"id_paciente_hash", "dt_inicio", "dt_fim", "is_current", "sk_paciente"}
    missing = required - cols
    assert not missing, f"SCD2 columns missing from dim_paciente: {missing}"


def test_no_pii_in_dim_paciente(pg_gold):
    """dim_paciente must not have PII columns."""
    cur = pg_gold.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'gold_dw' AND table_name = 'dim_paciente'
    """)
    cols = {row[0] for row in cur.fetchall()}
    cur.close()

    pii = {"cpf", "nome", "data_nascimento", "email", "telefone"}
    leakage = pii & cols
    assert not leakage, f"CRITICAL FAILURE: PII columns in dim_paciente: {leakage}"


# ── Temporal integrity tests ──────────────────────────────────────────────────


def test_no_inverted_dates(pg_gold):
    """dt_inicio must be <= dt_fim for all records."""
    cur = pg_gold.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM gold_dw.dim_paciente
        WHERE dt_inicio > dt_fim
    """)
    n = cur.fetchone()[0]
    cur.close()
    assert n == 0, f"{n} records with dt_inicio > dt_fim (temporal inversion)"


def test_no_duplicate_current_records(pg_gold):
    """There must not be two is_current=true records for the same id_paciente_hash."""
    cur = pg_gold.cursor()
    cur.execute("""
        SELECT id_paciente_hash, COUNT(*) as c
        FROM gold_dw.dim_paciente
        WHERE is_current = true
        GROUP BY id_paciente_hash
        HAVING COUNT(*) > 1
    """)
    duplicates = cur.fetchall()
    cur.close()
    assert not duplicates, f"SCD2 overlap: {len(duplicates)} hashes with multiple current records"


def test_current_records_have_open_dt_fim(pg_gold):
    """Records with is_current=true must have dt_fim = '9999-12-31'."""
    cur = pg_gold.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM gold_dw.dim_paciente
        WHERE is_current = true AND dt_fim != '9999-12-31'
    """)
    n = cur.fetchone()[0]
    cur.close()
    assert n == 0, f"{n} current records with dt_fim != 9999-12-31"


def test_closed_records_have_is_current_false(pg_gold):
    """Records with dt_fim < '9999-12-31' must have is_current = false."""
    cur = pg_gold.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM gold_dw.dim_paciente
        WHERE dt_fim < '9999-12-31' AND is_current = true
    """)
    n = cur.fetchone()[0]
    cur.close()
    assert n == 0, f"{n} closed records with is_current=true (SCD2 inconsistency)"


# ── Incremental load with updates ────────────────────────────────────────────


@pytest.mark.skipif(
    not (FIXTURES_DIR / "oltp_seed_v2.sql").exists(),
    reason="Fixture oltp_seed_v2.sql missing — skipping second-load test",
)
def test_scd2_second_load_creates_versions(pg_gold, pg):
    """
    Second load with 5 updates:
    - 5 old records must be closed (dt_fim = yesterday)
    - 5 new records must be opened (is_current = true)
    """
    from pipelines.gold.dim_paciente import load_dim_paciente

    count_before = _count_dim(pg_gold)
    current_before = _count_current(pg_gold)

    # Apply updates in OLTP
    cur = pg.cursor()
    seed_v2 = (FIXTURES_DIR / "oltp_seed_v2.sql").read_text()
    cur.execute(seed_v2)
    pg.commit()
    cur.close()

    # Run Gold load
    load_dim_paciente(snapshot_date=date.today().strftime("%Y%m%d"))

    count_after = _count_dim(pg_gold)
    current_after = _count_current(pg_gold)
    closed_after = _count_closed(pg_gold)

    # 5 new versions opened + 5 old ones closed
    assert count_after == count_before + 5, (
        f"Expected +5 records (SCD2 versions): {count_before} -> {count_after}"
    )
    assert current_after == current_before, (
        "Number of current records must not change (5 closed + 5 opened)"
    )
    assert closed_after >= 5, f"Expected >= 5 closed records, found {closed_after}"


# ── Fact Atendimento ──────────────────────────────────────────────────────────


def test_fato_atendimento_table_exists(pg_gold):
    """fato_atendimento must exist in the gold_dw schema."""
    cur = pg_gold.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'gold_dw' AND table_name = 'fato_atendimento'
    """)
    assert cur.fetchone() is not None, "Table gold_dw.fato_atendimento does not exist"
    cur.close()


def test_fato_referential_integrity(pg_gold):
    """sk_paciente in fato_atendimento must reference dim_paciente."""
    cur = pg_gold.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM gold_dw.fato_atendimento f
        LEFT JOIN gold_dw.dim_paciente d ON f.sk_paciente = d.sk_paciente
        WHERE d.sk_paciente IS NULL
    """)
    orphans = cur.fetchone()[0]
    cur.close()
    assert orphans == 0, f"{orphans} facts with sk_paciente not found in dim_paciente"
