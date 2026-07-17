"""Domain entities for the Vacinal bounded context."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DoseVacinacao:
    """Represents an administered vaccine dose (PNI)."""

    codigo_documento: str
    codigo_vacina: str
    data_vacina: str
    codigo_paciente: str | None = None
    sigla_vacina: str | None = None
    descricao_vacina: str | None = None
    codigo_cnes_estabelecimento: str | None = None
    codigo_municipio_paciente: str | None = None
    ano_mes: str | None = None
