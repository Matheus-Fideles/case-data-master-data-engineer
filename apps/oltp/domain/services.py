"""Domain services for the OLTP bounded context."""

from __future__ import annotations


class OltpService:
    """Orchestrates OLTP snapshot pipeline (oltp.paciente extraction).

    Coordinates between inbound ports (Postgres OLTP) and
    outbound ports (Delta Lake landing/bronze).
    """

    pass
