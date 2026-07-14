"""Smoke 04 — SCD Type 2 Gold DW.

Verifica integridade do SCD2 na dimensão dim_paciente:
  1. Primeira carga: todos os registros têm dt_fim = '9999-12-31' (current)
  2. Segunda carga com 5 atualizações: versões antigas fechadas, novas abertas
  3. Nenhum registro com dt_inicio > dt_fim
  4. Overlap detection: não deve existir dois registros current para o mesmo hash

Tempo alvo: < 90s
"""
from __future__ import annotations

import json
from datetime import date

import pytest

from tests.smoke.conftest import FIXTURES_DIR, pg_conn

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


# ── Testes de estrutura ───────────────────────────────────────────────────────

def test_dim_paciente_table_exists(pg_gold):
    """dim_paciente deve existir no schema gold_dw."""
    cur = pg_gold.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'gold_dw' AND table_name = 'dim_paciente'
    """)
    assert cur.fetchone() is not None, "Tabela gold_dw.dim_paciente não existe"
    cur.close()


def test_dim_paciente_scd2_columns(pg_gold):
    """Colunas obrigatórias SCD2 devem existir."""
    cur = pg_gold.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'gold_dw' AND table_name = 'dim_paciente'
    """)
    cols = {row[0] for row in cur.fetchall()}
    cur.close()

    required = {"id_paciente_hash", "dt_inicio", "dt_fim", "is_current", "sk_paciente"}
    missing = required - cols
    assert not missing, f"Colunas SCD2 ausentes em dim_paciente: {missing}"


def test_no_pii_in_dim_paciente(pg_gold):
    """dim_paciente não deve ter colunas PII."""
    cur = pg_gold.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'gold_dw' AND table_name = 'dim_paciente'
    """)
    cols = {row[0] for row in cur.fetchall()}
    cur.close()

    pii = {"cpf", "nome", "data_nascimento", "email", "telefone"}
    leakage = pii & cols
    assert not leakage, f"FALHA CRÍTICA: colunas PII em dim_paciente: {leakage}"


# ── Testes de integridade temporal ────────────────────────────────────────────

def test_no_inverted_dates(pg_gold):
    """dt_inicio deve ser ≤ dt_fim para todos os registros."""
    cur = pg_gold.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM gold_dw.dim_paciente
        WHERE dt_inicio > dt_fim
    """)
    n = cur.fetchone()[0]
    cur.close()
    assert n == 0, f"{n} registros com dt_inicio > dt_fim (inversão temporal)"


def test_no_duplicate_current_records(pg_gold):
    """Não deve existir dois registros is_current=true para o mesmo id_paciente_hash."""
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
    assert not duplicates, \
        f"Overlap SCD2: {len(duplicates)} hashes com múltiplos registros current"


def test_current_records_have_open_dt_fim(pg_gold):
    """Registros is_current=true devem ter dt_fim = '9999-12-31'."""
    cur = pg_gold.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM gold_dw.dim_paciente
        WHERE is_current = true AND dt_fim != '9999-12-31'
    """)
    n = cur.fetchone()[0]
    cur.close()
    assert n == 0, f"{n} registros current com dt_fim ≠ 9999-12-31"


def test_closed_records_have_is_current_false(pg_gold):
    """Registros com dt_fim < '9999-12-31' devem ter is_current = false."""
    cur = pg_gold.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM gold_dw.dim_paciente
        WHERE dt_fim < '9999-12-31' AND is_current = true
    """)
    n = cur.fetchone()[0]
    cur.close()
    assert n == 0, f"{n} registros fechados com is_current=true (inconsistência SCD2)"


# ── Teste de carga incremental com updates ────────────────────────────────────

@pytest.mark.skipif(
    not (FIXTURES_DIR / "oltp_seed_v2.sql").exists(),
    reason="Fixture oltp_seed_v2.sql ausente — pula teste de segunda carga",
)
def test_scd2_second_load_creates_versions(pg_gold, pg):
    """
    Segunda carga com 5 atualizações:
    - 5 registros antigos devem ser fechados (dt_fim = ontem)
    - 5 novos registros devem ser abertos (is_current = true)
    """
    from pipelines.gold.dim_paciente import load_dim_paciente

    count_before = _count_dim(pg_gold)
    current_before = _count_current(pg_gold)

    # Aplica updates no OLTP
    cur = pg.cursor()
    seed_v2 = (FIXTURES_DIR / "oltp_seed_v2.sql").read_text()
    cur.execute(seed_v2)
    pg.commit()
    cur.close()

    # Roda carga Gold
    load_dim_paciente(snapshot_date=date.today().strftime("%Y%m%d"))

    count_after = _count_dim(pg_gold)
    current_after = _count_current(pg_gold)
    closed_after = _count_closed(pg_gold)

    # 5 novas versões abertas + 5 antigas fechadas
    assert count_after == count_before + 5, \
        f"Esperado +5 registros (SCD2 versões): {count_before} → {count_after}"
    assert current_after == current_before, \
        "Número de registros current não deve mudar (5 fechados + 5 abertos)"
    assert closed_after >= 5, f"Esperado ≥ 5 registros fechados, encontrado {closed_after}"


# ── Fato Atendimento ──────────────────────────────────────────────────────────

def test_fato_atendimento_table_exists(pg_gold):
    """fato_atendimento deve existir no schema gold_dw."""
    cur = pg_gold.cursor()
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'gold_dw' AND table_name = 'fato_atendimento'
    """)
    assert cur.fetchone() is not None, "Tabela gold_dw.fato_atendimento não existe"
    cur.close()


def test_fato_referential_integrity(pg_gold):
    """sk_paciente em fato_atendimento deve referenciar dim_paciente."""
    cur = pg_gold.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM gold_dw.fato_atendimento f
        LEFT JOIN gold_dw.dim_paciente d ON f.sk_paciente = d.sk_paciente
        WHERE d.sk_paciente IS NULL
    """)
    orphans = cur.fetchone()[0]
    cur.close()
    assert orphans == 0, \
        f"{orphans} fatos com sk_paciente sem correspondente em dim_paciente"
