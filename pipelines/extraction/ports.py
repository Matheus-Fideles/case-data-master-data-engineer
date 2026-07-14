"""Ports (interfaces) da camada de extração — Arquitetura Hexagonal.

A lógica de domínio (o QUE extrair e ONDE guardar) é expressa em termos
desses protocolos. Os adapters concretos ficam em adapters/.
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class DataSourcePort(Protocol):
    """Port de entrada: qualquer coisa que forneça registros brutos."""

    def fetch(self) -> list[dict]:
        """Retorna todos os registros disponíveis."""
        ...


@runtime_checkable
class LandingStoragePort(Protocol):
    """Port de saída: qualquer coisa que persista registros na landing zone."""

    def write(
        self,
        records: list[dict],
        fonte: str,
        data_ref: str,
        source_url: str,
    ) -> str:
        """Persiste records e retorna o caminho de saída (s3a:// ou file://)."""
        ...


@runtime_checkable
class CachePort(Protocol):
    """Port de saída secundária: cache local para OFFLINE_MODE."""

    def save(self, records: list[dict], fonte: str, data_ref: str) -> None: ...

    def load(self, fonte: str, data_ref: str) -> list[dict]: ...
