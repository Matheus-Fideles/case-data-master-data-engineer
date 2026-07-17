"""Unit tests for the arboviroses extractor.

Demonstrates the DIP benefit: we mock ExtractionService (the port),
never boto3 or requests directly.
"""

from unittest.mock import MagicMock, patch

import pytest
from apps.shared.extraction_service import ExtractionResult


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
    from apps.epidemiologico.jobs import extract_arboviroses as arboviroses

    mock_svc = _make_mock_service()
    # Patch in the module that imported the function (not the origin module)
    with patch("apps.epidemiologico.jobs.extract_arboviroses.make_extraction_service", return_value=mock_svc):
        result = arboviroses.run(agravo="dengue", ano=2024)

    assert result["row_count"] == 5
    mock_svc.run.assert_called_once()
    call_kwargs = mock_svc.run.call_args
    assert call_kwargs.kwargs["fonte"] == "epidemiologico/dengue"
    assert call_kwargs.kwargs["data_ref"] == "2024"


def test_invalid_agravo():
    """Unknown agravo raises KeyError before calling any adapter."""
    from apps.epidemiologico.jobs import extract_arboviroses as arboviroses

    with pytest.raises(KeyError):
        arboviroses.run(agravo="malaria", ano=2024)
