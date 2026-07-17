"""Domain services for the Epidemiologico bounded context."""

from __future__ import annotations


class EpidemiologicoService:
    """Orchestrates epidemiological data pipeline (extraction, transformation, loading).

    Coordinates between inbound ports (API sources, Spark triggers) and
    outbound ports (Delta Lake storage, lineage emission).
    """

    pass
