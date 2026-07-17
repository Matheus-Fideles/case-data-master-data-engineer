"""Domain services for the Vacinal bounded context."""

from __future__ import annotations


class VacinalService:
    """Orchestrates vaccination data pipeline (PNI doses).

    Coordinates between inbound ports (API sources, Spark triggers) and
    outbound ports (Delta Lake storage, lineage emission).
    """

    pass
