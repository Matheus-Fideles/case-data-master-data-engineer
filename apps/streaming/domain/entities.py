"""Domain entities for the Streaming bounded context."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Atendimento:
    """Represents a healthcare attendance event from Kafka."""

    id_atendimento: str
    id_paciente_hash: str
    id_cnes: str
    ts_evento: str
    tipo_atendimento: str | None = None
    triagem: str | None = None
