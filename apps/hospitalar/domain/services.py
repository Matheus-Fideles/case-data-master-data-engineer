"""Domain services for the Hospitalar bounded context."""

from __future__ import annotations


class HospitalarService:
    """Orchestrates hospital data pipeline (SIM deaths, CNES establishments).

    Coordinates between inbound ports (API sources, Spark triggers) and
    outbound ports (Delta Lake storage, lineage emission).
    """

    pass
