"""Unit tests para BronzeJob (Template Method).

Estratégia: mocka os métodos protegidos (_read, _add_metadata, _write, _validate)
para isolar a lógica de orquestração do Template Method sem precisar de
SparkContext real — F.lit/F.col requerem JVM ativa.
"""
from unittest.mock import MagicMock, call, patch

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from pipelines.batch.bronze_base import BronzeJob


# ── stubs concretos ───────────────────────────────────────────────────────────

class _SimpleBronzeJob(BronzeJob):
    @property
    def schema(self) -> StructType:
        return StructType([StructField("id", StringType())])

    @property
    def source_name(self) -> str:
        return "test_source"

    @property
    def partition_col(self) -> str:
        return "ano_mes"


class _DerivedPartitionJob(_SimpleBronzeJob):
    """Stub que sobrescreve add_partition (como VacinacaoPniBronzeJob)."""

    @property
    def source_name(self) -> str:
        return "vacina_test"

    def add_partition(self, df, partition_val):
        # Simula derivação de partição de campo existente
        return df


# ── helpers ───────────────────────────────────────────────────────────────────

def _patched_run(job, *, row_count=5, **kwargs):
    """Executa job.run() com todos os métodos protegidos mockados.

    add_partition também é patchado para evitar F.lit sem SparkContext.
    O patch retorna o mesmo df, simulando que a coluna de partição foi adicionada.
    """
    df = MagicMock()
    df.count.return_value = row_count

    with (
        patch.object(job, "_read", return_value=df) as m_read,
        patch.object(job, "_add_metadata", return_value=df) as m_meta,
        patch.object(job, "add_partition", return_value=df) as m_part,
        patch.object(job, "_validate") as m_val,
        patch.object(job, "_write") as m_write,
    ):
        defaults = dict(
            spark=MagicMock(),
            input_path="s3a://landing/test/part.json",
            output_path="s3a://bronze/test/",
            partition_val="202401",
            batch_id="B1",
            source_url="http://api",
        )
        defaults.update(kwargs)
        count = job.run(**defaults)
        return count, df, m_read, m_meta, m_val, m_write


# ── testes de orquestração ────────────────────────────────────────────────────

def test_run_returns_row_count():
    job = _SimpleBronzeJob()
    count, *_ = _patched_run(job, row_count=10)
    assert count == 10


def test_run_calls_steps_in_order():
    """Template Method deve chamar _read → _add_metadata → add_partition → _validate → _write."""
    job = _SimpleBronzeJob()
    call_order = []

    df = MagicMock()
    df.count.return_value = 3
    df.withColumn.return_value = df  # add_partition padrão usa withColumn

    with (
        patch.object(job, "_read", side_effect=lambda *a, **kw: (call_order.append("read") or df)),
        patch.object(job, "_add_metadata", side_effect=lambda *a, **kw: (call_order.append("meta") or df)),
        patch.object(job, "_validate", side_effect=lambda *a, **kw: call_order.append("validate")),
        patch.object(job, "_write", side_effect=lambda *a, **kw: call_order.append("write")),
        patch("pipelines.batch.bronze_base.F") as mock_F,
    ):
        mock_F.lit.return_value = MagicMock()
        job.run(
            spark=MagicMock(),
            input_path="s3a://x",
            output_path="s3a://y",
            partition_val="202401",
            batch_id="B1",
            source_url="http://api",
        )

    assert call_order == ["read", "meta", "validate", "write"]


def test_run_passes_source_url_to_metadata():
    job = _SimpleBronzeJob()
    _, df, _, m_meta, *_ = _patched_run(job, source_url="http://custom-api")

    call_args = m_meta.call_args
    assert call_args.args[1] == "http://custom-api"


def test_run_passes_batch_id_to_metadata():
    job = _SimpleBronzeJob()
    _, df, _, m_meta, *_ = _patched_run(job, batch_id="BATCH-XYZ")

    call_args = m_meta.call_args
    assert call_args.args[2] == "BATCH-XYZ"


def test_validate_called_with_count():
    job = _SimpleBronzeJob()
    _, _, _, _, m_val, _ = _patched_run(job, row_count=42)

    m_val.assert_called_once()
    assert m_val.call_args.args[0] == 42


def test_validate_raises_on_empty():
    job = _SimpleBronzeJob()

    with pytest.raises(ValueError, match="Nenhuma linha lida"):
        job._validate(0, "s3a://empty.json")


def test_validate_passes_on_nonzero():
    job = _SimpleBronzeJob()
    job._validate(1, "s3a://x.json")  # deve não lançar


def test_write_receives_partition_val():
    job = _SimpleBronzeJob()
    _, _, _, _, _, m_write = _patched_run(job, partition_val="202403")

    call_args = m_write.call_args
    assert call_args.args[2] == "202403"


# ── testes dos hooks ──────────────────────────────────────────────────────────

def test_default_add_partition_calls_withcolumn():
    job = _SimpleBronzeJob()
    df = MagicMock()
    df.withColumn.return_value = df

    with patch("pipelines.batch.bronze_base.F") as mock_F:
        mock_F.lit.return_value = MagicMock()
        job.add_partition(df, "202401")

    df.withColumn.assert_called_once()
    assert df.withColumn.call_args.args[0] == "ano_mes"


def test_overridden_add_partition_is_used():
    """DerivedPartitionJob sobrescreve add_partition — run() deve usar a versão sobrescrita."""
    job = _DerivedPartitionJob()
    called = []

    # Sobrescreve o método ANTES dos patches, patchando tudo exceto add_partition
    real_add = job.add_partition
    job.add_partition = lambda df, pv: (called.append(pv) or df)

    df = MagicMock()
    df.count.return_value = 3

    with (
        patch.object(job, "_read", return_value=df),
        patch.object(job, "_add_metadata", return_value=df),
        patch.object(job, "_validate"),
        patch.object(job, "_write"),
    ):
        job.run(
            spark=MagicMock(),
            input_path="s3a://x",
            output_path="s3a://y",
            partition_val="202401",
            batch_id="B1",
            source_url="http://api",
        )

    assert called == ["202401"]
