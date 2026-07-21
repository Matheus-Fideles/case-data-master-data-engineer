"""Unit tests for the 4 extractors that use ExtractionService via DIP.

Pattern: patch make_extraction_service in the consumer module, not the origin.
Tests that run() delegates correctly and that invalid parameters fail early.
"""

from unittest.mock import MagicMock, patch

from apps.shared.extraction_service import ExtractionResult


def _mock_svc(row_count=3, fonte="test", path="s3a://landing/test/part.json"):
    svc = MagicMock()
    svc.run.return_value = ExtractionResult(
        row_count=row_count,
        output_path=path,
        source_url="http://api/test",
    )
    return svc


# ── CNES ─────────────────────────────────────────────────────────────────────


class TestCnes:
    def test_run_delegates_to_service(self):
        from apps.hospitalar.jobs import extract_cnes as cnes

        mock_svc = _mock_svc(row_count=10)
        with patch(
            "apps.hospitalar.jobs.extract_cnes.make_extraction_service", return_value=mock_svc
        ):
            result = cnes.run(snapshot_date="20240101")

        assert result["row_count"] == 10
        mock_svc.run.assert_called_once()

    def test_run_fonte_is_cnes(self):
        from apps.hospitalar.jobs import extract_cnes as cnes

        mock_svc = _mock_svc()
        with patch(
            "apps.hospitalar.jobs.extract_cnes.make_extraction_service", return_value=mock_svc
        ):
            cnes.run(snapshot_date="20240101")

        assert mock_svc.run.call_args.kwargs["fonte"] == "hospitalar/cnes"

    def test_run_uses_today_when_snapshot_date_is_none(self):
        from apps.hospitalar.jobs import extract_cnes as cnes

        mock_svc = _mock_svc()
        with patch(
            "apps.hospitalar.jobs.extract_cnes.make_extraction_service", return_value=mock_svc
        ):
            cnes.run(snapshot_date=None)

        data_ref = mock_svc.run.call_args.kwargs["data_ref"]
        assert len(data_ref) == 8  # YYYYMMDD
        assert data_ref.isdigit()


# ── Municipios ────────────────────────────────────────────────────────────────


class TestMunicipios:
    def test_run_delegates_to_service(self):
        from apps.geografico.jobs import extract_municipios as municipios

        mock_svc = _mock_svc(row_count=5570)
        with patch(
            "apps.geografico.jobs.extract_municipios.make_extraction_service", return_value=mock_svc
        ):
            result = municipios.run(snapshot_date="20240101")

        assert result["row_count"] == 5570
        mock_svc.run.assert_called_once()

    def test_run_fonte_is_municipios(self):
        from apps.geografico.jobs import extract_municipios as municipios

        mock_svc = _mock_svc()
        with patch(
            "apps.geografico.jobs.extract_municipios.make_extraction_service", return_value=mock_svc
        ):
            municipios.run(snapshot_date="20240101")

        assert mock_svc.run.call_args.kwargs["fonte"] == "geografico/municipios"

    def test_run_uses_today_when_snapshot_date_is_none(self):
        from apps.geografico.jobs import extract_municipios as municipios

        mock_svc = _mock_svc()
        with patch(
            "apps.geografico.jobs.extract_municipios.make_extraction_service", return_value=mock_svc
        ):
            municipios.run(snapshot_date=None)

        data_ref = mock_svc.run.call_args.kwargs["data_ref"]
        assert len(data_ref) == 8
        assert data_ref.isdigit()


# ── Vacinacao PNI ─────────────────────────────────────────────────────────────


class TestVacinacaoPni:
    def test_run_delegates_to_service(self):
        from apps.vacinal.jobs import extract_vacinacao_pni as vacinacao_pni

        mock_svc = _mock_svc(row_count=200)
        with patch(
            "apps.vacinal.jobs.extract_vacinacao_pni.make_extraction_service", return_value=mock_svc
        ):
            result = vacinacao_pni.run(ano="2024")

        assert result["row_count"] == 200
        mock_svc.run.assert_called_once()

    def test_run_fonte_is_vacinacao_pni(self):
        from apps.vacinal.jobs import extract_vacinacao_pni as vacinacao_pni

        mock_svc = _mock_svc()
        with patch(
            "apps.vacinal.jobs.extract_vacinacao_pni.make_extraction_service", return_value=mock_svc
        ):
            vacinacao_pni.run(ano="2024")

        assert mock_svc.run.call_args.kwargs["fonte"] == "vacinal/vacinacao_pni"

    def test_run_data_ref_matches_ano(self):
        from apps.vacinal.jobs import extract_vacinacao_pni as vacinacao_pni

        mock_svc = _mock_svc()
        with patch(
            "apps.vacinal.jobs.extract_vacinacao_pni.make_extraction_service", return_value=mock_svc
        ):
            vacinacao_pni.run(ano="2023")

        assert mock_svc.run.call_args.kwargs["data_ref"] == "2023"


# ── SIM Obitos ────────────────────────────────────────────────────────────────


class TestSimObitos:
    def test_run_delegates_to_service(self):
        from apps.hospitalar.jobs import extract_sim_obitos as sim_obitos

        mock_svc = _mock_svc(row_count=50)
        with patch(
            "apps.hospitalar.jobs.extract_sim_obitos.make_extraction_service", return_value=mock_svc
        ):
            result = sim_obitos.run(ano="2024")

        assert result["row_count"] == 50
        mock_svc.run.assert_called_once()

    def test_run_fonte_is_sim(self):
        from apps.hospitalar.jobs import extract_sim_obitos as sim_obitos

        mock_svc = _mock_svc()
        with patch(
            "apps.hospitalar.jobs.extract_sim_obitos.make_extraction_service", return_value=mock_svc
        ):
            sim_obitos.run(ano="2024")

        assert mock_svc.run.call_args.kwargs["fonte"] == "hospitalar/sim_obitos"

    def test_run_data_ref_matches_ano(self):
        from apps.hospitalar.jobs import extract_sim_obitos as sim_obitos

        mock_svc = _mock_svc()
        with patch(
            "apps.hospitalar.jobs.extract_sim_obitos.make_extraction_service", return_value=mock_svc
        ):
            sim_obitos.run(ano="2023")

        assert mock_svc.run.call_args.kwargs["data_ref"] == "2023"

    def test_run_defaults_to_env_or_2024(self, monkeypatch):
        from apps.hospitalar.jobs import extract_sim_obitos as sim_obitos

        monkeypatch.delenv("DATA_REF_ANO", raising=False)
        mock_svc = _mock_svc()
        with patch(
            "apps.hospitalar.jobs.extract_sim_obitos.make_extraction_service", return_value=mock_svc
        ):
            sim_obitos.run(ano=None)

        assert mock_svc.run.call_args.kwargs["data_ref"] == "2024"
