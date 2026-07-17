"""Domain entities for the Geografico bounded context."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Municipio:
    """Represents a Brazilian municipality (IBGE × health regions)."""

    codigo_municipio: str
    municipio: str
    uf: str
    codigo_uf: str | None = None
    regiao_saude: str | None = None
    macrorregiao_saude: str | None = None
    populacao_estimada_ibge_2022: str | None = None
    snapshot_date: str | None = None
