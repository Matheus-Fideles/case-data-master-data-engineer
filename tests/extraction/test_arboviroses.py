"""Unit tests para o extrator de arboviroses.

Demonstra o benefício do DIP: mockamos o ExtractionService (porta),
nunca boto3 ou requests diretamente.
"""
from unittest.mock import MagicMock, patch

import pytest

from pipelines.extraction.service import ExtractionResult


def _make_mock_service(row_count=5, path="s3a://landing/dengue/2024/part-00000.json"):
    svc = MagicMock()
    svc.run.return_value = ExtractionResult(
        row_count=row_count,
        output_path=path,
        source_url="https://api.example.com/dengue",
    )
    return svc


def test_run_offline():
    """run() delega ao ExtractionService e retorna o resultado correto."""
    from pipelines.extraction import arboviroses

    mock_svc = _make_mock_service()
    # Patch no módulo que importou a função (não no módulo de origem)
    with patch("pipelines.extraction.arboviroses.make_extraction_service", return_value=mock_svc):
        result = arboviroses.run(agravo="dengue", ano=2024)

    assert result["row_count"] == 5
    mock_svc.run.assert_called_once()
    call_kwargs = mock_svc.run.call_args
    assert call_kwargs.kwargs["fonte"] == "dengue"
    assert call_kwargs.kwargs["data_ref"] == "2024"


def test_invalid_agravo():
    """agravo desconhecido levanta KeyError antes de chamar qualquer adapter."""
    from pipelines.extraction import arboviroses

    with pytest.raises(KeyError):
        arboviroses.run(agravo="malaria", ano=2024)
