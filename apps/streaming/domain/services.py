"""Domain services for the Streaming bounded context."""

from __future__ import annotations


class StreamingService:
    """Orchestrates streaming pipeline (Kafka consumer for atendimentos).

    Coordinates between inbound ports (Kafka topics) and
    outbound ports (Delta Lake Bronze, Postgres Gold, DLQ).
    """

    pass
