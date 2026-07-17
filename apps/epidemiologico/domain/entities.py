"""Domain entities for the Epidemiologico bounded context."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Notificacao:
    """Represents a disease notification (SINAN)."""

    id_agravo: str
    dt_notific: str
    sg_uf_not: str
    id_municip: str
    nu_idade_n: str | None = None
    cs_sexo: str | None = None
    classi_fin: str | None = None
    evolucao: str | None = None
    hospitaliz: str | None = None
    agravo: str | None = None
    ano_mes: str | None = None


@dataclass
class Arbovirose:
    """Raw arbovirus record from SINAN API (dengue/zika/chikungunya)."""

    id_agravo: str
    dt_notific: str
    sg_uf_not: str
    id_municip: str
    agravo: str
