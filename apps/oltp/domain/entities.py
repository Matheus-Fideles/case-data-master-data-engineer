"""Domain entities for the OLTP bounded context."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PacienteOltp:
    """Represents a raw patient record from OLTP Postgres (contains PII).

    PII fields present intentionally — Bronze is the only raw layer.
    Silver masks before any propagation (ADR-0004).
    """

    id_paciente: int
    cpf: str  # PII
    nome: str  # PII
    data_nascimento: str  # PII
    sexo: str
    cep: str  # PII
    municipio_codigo_ibge: str
    email: str | None = None  # PII
    telefone: str | None = None  # PII
    snapshot_date: str | None = None
