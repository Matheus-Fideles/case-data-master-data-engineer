"""Unit tests for the oltp_snapshot extractor.

Tests validation, delegation to ExtractionService and incremental
extraction behaviour — without opening a real Postgres connection.
"""

from unittest.mock import MagicMock, patch

import pytest
from apps.shared.extraction_service import ExtractionResult


def _mock_svc(row_count=10):
    svc = MagicMock()
    svc.run.return_value = ExtractionResult(
        row_count=row_count,
        output_path="s3a://landing/oltp_paciente/20240101/part.json",
        source_url="postgres://oltp/paciente",
    )
    return svc


_SAMPLE_RECORDS = [
    {
        "id_paciente": 1,
        "cpf": "12345678901",
        "nome": "Fulano Silva",
        "data_nascimento": "1990-01-15",
        "sexo": "M",
        "cep": "01310100",
        "municipio_codigo_ibge": "3550308",
        "email": "fulano@example.com",
        "telefone": "11999990000",
        "created_at": "2024-01-01T00:00:00",
        "updated_at": "2024-01-01T00:00:00",
    }
]


class TestOltpSnapshotRun:
    def test_delegates_to_extraction_service(self):
        from apps.oltp.jobs import extract_oltp_snapshot as oltp_snapshot

        mock_svc = _mock_svc()
        with patch(
            "apps.oltp.jobs.extract_oltp_snapshot.make_extraction_service", return_value=mock_svc
        ):
            with patch.object(oltp_snapshot, "_fetch_pacientes", return_value=_SAMPLE_RECORDS):
                result = oltp_snapshot.run(snapshot_date="20240101")

        assert result["row_count"] == 10
        mock_svc.run.assert_called_once()

    def test_fonte_is_oltp_paciente(self):
        from apps.oltp.jobs import extract_oltp_snapshot as oltp_snapshot

        mock_svc = _mock_svc()
        with patch(
            "apps.oltp.jobs.extract_oltp_snapshot.make_extraction_service", return_value=mock_svc
        ):
            with patch.object(oltp_snapshot, "_fetch_pacientes", return_value=_SAMPLE_RECORDS):
                oltp_snapshot.run(snapshot_date="20240101")

        assert mock_svc.run.call_args.kwargs["fonte"] == "oltp/paciente"

    def test_data_ref_matches_snapshot_date(self):
        from apps.oltp.jobs import extract_oltp_snapshot as oltp_snapshot

        mock_svc = _mock_svc()
        with patch(
            "apps.oltp.jobs.extract_oltp_snapshot.make_extraction_service", return_value=mock_svc
        ):
            with patch.object(oltp_snapshot, "_fetch_pacientes", return_value=_SAMPLE_RECORDS):
                oltp_snapshot.run(snapshot_date="20240315")

        assert mock_svc.run.call_args.kwargs["data_ref"] == "20240315"

    def test_uses_today_when_snapshot_date_is_none(self):
        from apps.oltp.jobs import extract_oltp_snapshot as oltp_snapshot

        mock_svc = _mock_svc()
        with patch(
            "apps.oltp.jobs.extract_oltp_snapshot.make_extraction_service", return_value=mock_svc
        ):
            with patch.object(oltp_snapshot, "_fetch_pacientes", return_value=_SAMPLE_RECORDS):
                oltp_snapshot.run(snapshot_date=None)

        data_ref = mock_svc.run.call_args.kwargs["data_ref"]
        assert len(data_ref) == 8
        assert data_ref.isdigit()

    def test_incremental_passes_updated_since(self):
        # The service mock does not execute fetch_fn automatically —
        # we extract the closure and invoke it inside the active patch.
        from apps.oltp.jobs import extract_oltp_snapshot as oltp_snapshot

        mock_svc = _mock_svc()
        captured = {}

        def fake_fetch(updated_since=None):
            captured["updated_since"] = updated_since
            return _SAMPLE_RECORDS

        with patch(
            "apps.oltp.jobs.extract_oltp_snapshot.make_extraction_service", return_value=mock_svc
        ):
            with patch.object(oltp_snapshot, "_fetch_pacientes", side_effect=fake_fetch):
                oltp_snapshot.run(snapshot_date="20240101", incremental=True)
                fetch_fn = mock_svc.run.call_args.kwargs["fetch_fn"]
                fetch_fn()  # invoca dentro do patch para _fetch_pacientes estar mockado

        assert captured["updated_since"] == "20240101"

    def test_full_snapshot_passes_none_as_updated_since(self):
        from apps.oltp.jobs import extract_oltp_snapshot as oltp_snapshot

        mock_svc = _mock_svc()
        captured = {}

        def fake_fetch(updated_since=None):
            captured["updated_since"] = updated_since
            return _SAMPLE_RECORDS

        with patch(
            "apps.oltp.jobs.extract_oltp_snapshot.make_extraction_service", return_value=mock_svc
        ):
            with patch.object(oltp_snapshot, "_fetch_pacientes", side_effect=fake_fetch):
                oltp_snapshot.run(snapshot_date="20240101", incremental=False)
                fetch_fn = mock_svc.run.call_args.kwargs["fetch_fn"]
                fetch_fn()

        assert captured["updated_since"] is None


class TestOltpValidate:
    def test_raises_on_empty_records(self):
        from apps.oltp.jobs.extract_oltp_snapshot import _validate

        with pytest.raises(ValueError, match="zero records"):
            _validate([])

    def test_raises_on_missing_required_fields(self):
        from apps.oltp.jobs.extract_oltp_snapshot import _validate

        with pytest.raises(ValueError, match="Required fields missing"):
            _validate([{"id_paciente": 1}])  # falta cpf, nome, data_nascimento

    def test_passes_with_all_required_fields(self):
        from apps.oltp.jobs.extract_oltp_snapshot import _validate

        _validate(_SAMPLE_RECORDS)  # must not raise
