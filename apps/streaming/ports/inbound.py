"""Inbound ports for the Streaming domain."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class IKafkaConsumerPort(Protocol):
    """Input port: consumes events from Kafka topic."""

    def consume(self) -> None: ...
