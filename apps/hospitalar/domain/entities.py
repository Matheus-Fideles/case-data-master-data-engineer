"""Domain entities for the Hospitalar bounded context."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Obito:
    """Represents a death record from SIM."""

    contador: str
    causabas: str
    dtobito: str
    codmunocor: str | None = None
    idade: str | None = None
    sexo: str | None = None
    ano_part: str | None = None


@dataclass
class Estabelecimento:
    """Represents a healthcare establishment from CNES."""

    codigo_cnes: str
    nome_fantasia: str
    codigo_municipio: str
    snapshot_date: str | None = None
