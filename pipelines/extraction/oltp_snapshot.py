"""Extrator OLTP — snapshot diário de oltp.paciente.

Diferente dos outros extratores (REST API), este usa psycopg2 para ler
diretamente do Postgres. Retorna registros como lista de dicts e persiste
em MinIO landing/ via ExtractionService (mesma interface dos demais).

⚠️  ATENÇÃO PII: esta é a única extração que lida com dados pessoais em claro
(cpf, nome, data_nascimento, cep, email, telefone). O dado permanece em claro
apenas na camada Bronze; o job Silver aplica mascaramento (ADR-0004) antes de
qualquer uso downstream.

Campos PII presentes neste arquivo por necessidade de extração — ver ADR-0004.
"""
from __future__ import annotations

import logging
import os
from datetime import date

from pipelines.extraction.factory import make_extraction_service

log = logging.getLogger(__name__)

_SOURCE_LABEL = "postgres://oltp/paciente"
_CAMPOS_MINIMOS = {"id_paciente", "cpf", "nome", "data_nascimento"}

# Colunas PII — listadas aqui apenas para documentação e validação de extração.
# Não usar fora deste módulo e de tests/.
_PII_COLS = ("cpf", "nome", "data_nascimento", "cep", "email", "telefone")


def _pg_dsn() -> str:
    return (
        f"host={os.environ.get('OLTP_PG_HOST', 'localhost')} "
        f"port={os.environ.get('OLTP_PG_PORT', '5432')} "
        f"dbname={os.environ.get('OLTP_PG_DB', 'postgres')} "
        f"user={os.environ.get('OLTP_PG_USER', 'gold_engineer')} "
        f"password={os.environ.get('OLTP_PG_PASSWORD', 'gold_engineer')}"
    )


def _fetch_pacientes(updated_since: str | None = None) -> list[dict]:
    """Lê oltp.paciente via psycopg2.

    Se updated_since for fornecido (ISO date), retorna apenas registros
    modificados desde essa data — permite extração incremental.
    Caso contrário, faz snapshot completo.
    """
    import psycopg2
    import psycopg2.extras

    query = "SELECT * FROM oltp.paciente"
    params: tuple = ()
    if updated_since:
        query += " WHERE updated_at >= %s"
        params = (updated_since,)
    query += " ORDER BY id_paciente"

    conn = psycopg2.connect(_pg_dsn())
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    finally:
        conn.close()

    records = []
    for row in rows:
        rec = dict(row)
        # converte tipos não serializáveis em JSON
        for key, val in rec.items():
            if hasattr(val, "isoformat"):
                rec[key] = val.isoformat()
        records.append(rec)

    log.info("Lidos %d registros de oltp.paciente", len(records))
    return records


def _validate(records: list[dict]) -> None:
    if not records:
        raise ValueError("oltp.paciente retornou zero registros")
    missing = _CAMPOS_MINIMOS - set(records[0].keys())
    if missing:
        raise ValueError(f"Campos obrigatórios ausentes em oltp_snapshot: {missing}")


def run(snapshot_date: str | None = None, incremental: bool = False) -> dict:
    """Executa o snapshot de oltp.paciente.

    Args:
        snapshot_date: data de referência no formato YYYYMMDD.
                       Se None, usa a data de hoje.
        incremental:   se True, filtra por updated_at >= snapshot_date
                       (extração incremental). Se False, snapshot completo.
    """
    snap = snapshot_date or date.today().strftime("%Y%m%d")
    updated_since = snap if incremental else None

    def fetch() -> list[dict]:
        records = _fetch_pacientes(updated_since=updated_since)
        _validate(records)
        return records

    service = make_extraction_service()
    result = service.run(
        fonte="oltp_paciente",
        data_ref=snap,
        fetch_fn=fetch,
        source_url=_SOURCE_LABEL,
    )
    return {
        "row_count": result.row_count,
        "output_path": result.output_path,
        "source_url": result.source_url,
    }
