"""Domain services for the Geografico bounded context."""

from __future__ import annotations


class GeograficoService:
    """Orchestrates geographic data pipeline (municipalities, IBGE).

    Coordinates between inbound ports (API sources, Spark triggers) and
    outbound ports (Delta Lake storage, lineage emission).
    """

    pass
