"""Unit tests para ExtractionService (application service).

Testa a lógica pura de orquestração sem boto3 ou requests.
Usa mocks dos ports LandingStoragePort e CachePort.
"""
from unittest.mock import MagicMock, call

import pytest

from pipelines.extraction.service import ExtractionResult, ExtractionService


def _make_landing(path="s3a://landing/fonte/data/part.json"):
    m = MagicMock()
    m.write.return_value = path
    return m


def _make_cache():
    return MagicMock()


_RECORDS = [{"id": 1, "valor": "a"}, {"id": 2, "valor": "b"}]


class TestExtractionServiceOnline:
    def test_calls_fetch_fn_and_writes_to_landing(self):
        landing = _make_landing()
        cache = _make_cache()
        svc = ExtractionService(landing=landing, cache=cache, offline=False)

        fetch_fn = MagicMock(return_value=_RECORDS)
        result = svc.run(fonte="test", data_ref="2024", fetch_fn=fetch_fn, source_url="http://api/test")

        fetch_fn.assert_called_once()
        landing.write.assert_called_once_with(_RECORDS, "test", "2024", "http://api/test")
        assert result.row_count == 2
        assert result.output_path == "s3a://landing/fonte/data/part.json"
        assert result.source_url == "http://api/test"

    def test_saves_to_cache_after_fetch(self):
        cache = _make_cache()
        svc = ExtractionService(landing=_make_landing(), cache=cache, offline=False)

        svc.run(fonte="test", data_ref="2024", fetch_fn=MagicMock(return_value=_RECORDS), source_url="http://x")

        cache.save.assert_called_once_with(_RECORDS, "test", "2024")

    def test_raises_when_fetch_returns_empty(self):
        svc = ExtractionService(landing=_make_landing(), cache=_make_cache(), offline=False)

        with pytest.raises(ValueError, match="Nenhum registro"):
            svc.run(fonte="test", data_ref="2024", fetch_fn=MagicMock(return_value=[]), source_url="http://x")


class TestExtractionServiceOffline:
    def test_loads_from_cache_not_fetch(self):
        cache = _make_cache()
        cache.load.return_value = _RECORDS
        svc = ExtractionService(landing=_make_landing(), cache=cache, offline=True)

        fetch_fn = MagicMock()
        result = svc.run(fonte="test", data_ref="2024", fetch_fn=fetch_fn, source_url="http://x")

        fetch_fn.assert_not_called()
        cache.load.assert_called_once_with("test", "2024")
        assert result.row_count == 2

    def test_raises_when_cache_is_empty(self):
        cache = _make_cache()
        cache.load.return_value = []
        svc = ExtractionService(landing=_make_landing(), cache=cache, offline=True)

        with pytest.raises(ValueError, match="Nenhum registro"):
            svc.run(fonte="test", data_ref="2024", fetch_fn=MagicMock(), source_url="http://x")
