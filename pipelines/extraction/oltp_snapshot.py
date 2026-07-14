"""OLTP extractor — daily snapshot of oltp.paciente.

Unlike other extractors (REST API), this uses psycopg2 to read
directly from Postgres. Returns records as a list of dicts and persists
to MinIO landing/ via ExtractionService (same interface as others).

⚠️  PII WARNING: this is the only extraction that handles plaintext personal data
(cpf, nome, data_nascimento, cep, email, telefone). Data remains in plaintext
only in the Bronze layer; the Silver job applies masking (ADR-0004) before
any downstream use.

PII fields present in this file due to extraction requirements — see ADR-0004.
"""

from __future__ import annotations

import logging
import os
from datetime import date

from pipelines.extraction.factory import make_extraction_service

log = logging.getLogger(__name__)

_SOURCE_LABEL = "postgres://oltp/paciente"
_REQUIRED_FIELDS = {"id_paciente", "cpf", "nome", "data_nascimento"}

# PII columns — listed here only for documentation and extraction validation.
# Do not use outside this module and tests/.
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
    """Reads oltp.paciente via psycopg2.

    If updated_since is provided (ISO date), returns only records
    modified since that date — enables incremental extraction.
    Otherwise, performs a full snapshot.
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
        for key, val in rec.items():
            if hasattr(val, "isoformat"):
                rec[key] = val.isoformat()
        records.append(rec)

    log.info("Read %d records from oltp.paciente", len(records))
    return records


def _validate(records: list[dict]) -> None:
    if not records:
        raise ValueError("oltp.paciente returned zero records")
    missing = _REQUIRED_FIELDS - set(records[0].keys())
    if missing:
        raise ValueError(f"Required fields missing in oltp_snapshot: {missing}")


def run(snapshot_date: str | None = None, incremental: bool = False) -> dict:
    """Runs the oltp.paciente snapshot.

    Args:
        snapshot_date: reference date in YYYYMMDD format.
                       If None, uses today's date.
        incremental:   if True, filters by updated_at >= snapshot_date
                       (incremental extraction). If False, full snapshot.
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
