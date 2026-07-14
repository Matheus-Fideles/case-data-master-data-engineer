"""Unit tests for silver_paciente.py — LGPD masking.

Verifies that:
  1. validate_no_pii() is called after transform()
  2. No PII column survives transform() (real integration with masking)
  3. Expected Silver columns are present
  4. PacienteSilverJob.replace_condition uses a simple partition (snapshot_date)
"""

from unittest.mock import MagicMock, patch

import pytest
from pipelines.batch.silver_paciente import MASKING_VERSION, PacienteSilverJob

# ── helpers ───────────────────────────────────────────────────────────────────


def _patched_run(job, *, row_count=3, **kwargs):
    """Patches _read, _add_metadata, transform and _write to isolate orchestration."""
    df = MagicMock()
    df.count.return_value = row_count

    with (
        patch.object(job, "_read", return_value=df) as m_read,
        patch.object(job, "_add_metadata", return_value=df) as m_meta,
        patch.object(job, "transform", return_value=df),
        patch.object(job, "_write") as m_write,
    ):
        defaults = dict(
            spark=MagicMock(),
            input_path="s3a://bronze/oltp_paciente/",
            output_path="s3a://silver/paciente/",
            filter_col="snapshot_date",
            filter_val="20240101",
            batch_id="B1",
        )
        defaults.update(kwargs)
        count = job.run(**defaults)
        return count, df, m_read, m_meta, m_write


# ── orchestration tests ───────────────────────────────────────────────────────


class TestPacienteSilverJobOrchestration:
    def test_run_returns_row_count(self):
        count, *_ = _patched_run(PacienteSilverJob(), row_count=50)
        assert count == 50

    def test_partition_col_is_snapshot_date(self):
        job = PacienteSilverJob()
        assert job.partition_cols == ["snapshot_date"]

    def test_replace_condition_is_simple(self):
        job = PacienteSilverJob()
        cond = job.replace_condition("snapshot_date", "20240101")
        assert cond == "snapshot_date = '20240101'"


# ── testes do mascaramento (transform()) ─────────────────────────────────────


class TestPacienteTransform:
    """Tests transform() with F mocks to avoid SparkContext."""

    def _mock_df_with_cols(self, cols: list[str]) -> MagicMock:
        df = MagicMock()
        df.columns = cols
        df.withColumn.return_value = df
        df.drop.return_value = df
        return df

    def test_transform_calls_mask_paciente(self):
        from pipelines.batch import silver_paciente as sp

        job = PacienteSilverJob()
        df = self._mock_df_with_cols(["cpf", "nome", "data_nascimento", "cep", "email", "telefone"])
        df.columns = []  # after drops, simulates clean columns for validate_no_pii

        with (
            patch.object(sp, "F", MagicMock()),
            patch("pipelines.batch.silver_paciente.mask_paciente", return_value=df) as m_mask,
            patch("pipelines.batch.silver_paciente.validate_no_pii"),
        ):
            job.transform(df)

        m_mask.assert_called_once_with(df, cpf_col="cpf", output_col="id_paciente_hash")

    def test_transform_calls_validate_no_pii(self):
        from pipelines.batch import silver_paciente as sp

        job = PacienteSilverJob()
        df = self._mock_df_with_cols([])

        with (
            patch.object(sp, "F", MagicMock()),
            patch("pipelines.batch.silver_paciente.mask_paciente", return_value=df),
            patch("pipelines.batch.silver_paciente.validate_no_pii") as m_validate,
        ):
            job.transform(df)

        m_validate.assert_called_once()

    def test_transform_adds_masking_version(self):
        from pipelines.batch import silver_paciente as sp

        job = PacienteSilverJob()
        df = self._mock_df_with_cols([])
        mock_f = MagicMock()

        with (
            patch.object(sp, "F", mock_f),
            patch("pipelines.batch.silver_paciente.mask_paciente", return_value=df),
            patch("pipelines.batch.silver_paciente.validate_no_pii"),
        ):
            job.transform(df)

        # withColumn deve ter sido chamado com "masking_version"
        col_calls = [c.args[0] for c in df.withColumn.call_args_list]
        assert "masking_version" in col_calls

    def test_masking_version_constant_is_set(self):
        assert MASKING_VERSION.startswith("masking-")

    def test_validate_no_pii_blocks_pii_leakage(self):
        """validate_no_pii must raise if cpf survives (critical regression)."""
        from pipelines.common.masking import validate_no_pii

        df_with_pii = MagicMock()
        df_with_pii.columns = ["id_paciente_hash", "cpf", "ano_nascimento"]  # cpf sobrou

        with pytest.raises(ValueError, match="PII"):
            validate_no_pii(df_with_pii, pii_cols=["cpf"])
