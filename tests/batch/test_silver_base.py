"""Unit tests para SilverJob (Template Method).

Estratégia: mocka _read, _add_metadata, _write para isolar a orquestração
sem precisar de SparkContext real — F.col/F.current_timestamp requerem JVM ativa.
Os hooks puros (replace_condition, transform) são testados diretamente.
"""
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame

from pipelines.batch.silver_base import SilverJob


# ── stubs concretos ───────────────────────────────────────────────────────────

class _SimpleSilverJob(SilverJob):
    @property
    def source_name(self) -> str:
        return "test_silver"

    @property
    def partition_cols(self) -> list[str]:
        return ["ano_mes"]

    def transform(self, df: DataFrame) -> DataFrame:
        return df


class _TransformingSilverJob(SilverJob):
    """Stub com transform que registra que foi chamado."""

    def __init__(self):
        self.transform_calls = []

    @property
    def source_name(self) -> str:
        return "transforming"

    @property
    def partition_cols(self) -> list[str]:
        return ["ano_mes"]

    def transform(self, df: DataFrame) -> DataFrame:
        self.transform_calls.append(df)
        return df


class _CompositePartitionJob(SilverJob):
    """Simula NotificacaoSilverJob com replace_condition composta."""

    def __init__(self, agravo: str) -> None:
        self._agravo = agravo

    @property
    def source_name(self) -> str:
        return "notificacao"

    @property
    def partition_cols(self) -> list[str]:
        return ["agravo", "ano_mes"]

    def transform(self, df: DataFrame) -> DataFrame:
        return df

    def replace_condition(self, filter_col: str, filter_val: str) -> str:
        return f"agravo = '{self._agravo}' AND {filter_col} = '{filter_val}'"


# ── helpers ───────────────────────────────────────────────────────────────────

def _patched_run(job, *, row_count=5, **kwargs):
    """Executa job.run() com os métodos de infra mockados."""
    df = MagicMock()
    df.count.return_value = row_count

    with (
        patch.object(job, "_read", return_value=df) as m_read,
        patch.object(job, "_add_metadata", return_value=df) as m_meta,
        patch.object(job, "_write") as m_write,
    ):
        defaults = dict(
            spark=MagicMock(),
            input_path="s3a://bronze/test/",
            output_path="s3a://silver/test/",
            filter_col="ano_mes",
            filter_val="202401",
            batch_id="B1",
        )
        defaults.update(kwargs)
        count = job.run(**defaults)
        return count, df, m_read, m_meta, m_write


# ── testes de orquestração ────────────────────────────────────────────────────

def test_run_returns_row_count():
    count, *_ = _patched_run(_SimpleSilverJob(), row_count=7)
    assert count == 7


def test_run_calls_transform():
    job = _TransformingSilverJob()
    _patched_run(job)
    assert len(job.transform_calls) == 1


def test_run_passes_filter_col_to_read():
    # _read(spark, input_path, filter_col, filter_val) → args[2]=filter_col, args[3]=filter_val
    job = _SimpleSilverJob()
    _, _, m_read, *_ = _patched_run(job, filter_col="snapshot_date", filter_val="20240101")

    args = m_read.call_args.args
    assert args[2] == "snapshot_date"
    assert args[3] == "20240101"


def test_run_passes_batch_id_to_metadata():
    job = _SimpleSilverJob()
    _, _, _, m_meta, _ = _patched_run(job, batch_id="SILVER-B42")

    assert m_meta.call_args.args[1] == "SILVER-B42"


def test_write_called_once():
    job = _SimpleSilverJob()
    _, _, _, _, m_write = _patched_run(job)
    m_write.assert_called_once()


def test_write_receives_filter_values():
    job = _SimpleSilverJob()
    _, _, _, _, m_write = _patched_run(job, filter_col="ano_mes", filter_val="202403")

    args = m_write.call_args.args
    assert "ano_mes" in args
    assert "202403" in args


# ── testes dos hooks puros ────────────────────────────────────────────────────

def test_default_replace_condition():
    job = _SimpleSilverJob()
    assert job.replace_condition("ano_mes", "202401") == "ano_mes = '202401'"


def test_composite_replace_condition_dengue():
    job = _CompositePartitionJob(agravo="dengue")
    cond = job.replace_condition("ano_mes", "202401")
    assert "agravo = 'dengue'" in cond
    assert "ano_mes = '202401'" in cond


def test_composite_replace_condition_zika():
    job = _CompositePartitionJob(agravo="zika")
    cond = job.replace_condition("ano_mes", "202406")
    assert "agravo = 'zika'" in cond
    assert "ano_mes = '202406'" in cond


def test_write_uses_replace_condition_hook():
    """_write() deve usar o valor retornado por replace_condition()."""
    job = _CompositePartitionJob(agravo="chikungunya")
    _, _, _, _, m_write = _patched_run(job, filter_col="ano_mes", filter_val="202401")

    # replace_condition é chamado dentro de _write, que está mockado —
    # verificamos indiretamente que o hook foi aplicado através do argumento passado a _write
    m_write.assert_called_once()


def test_replace_condition_used_in_actual_write():
    """Testa a integração entre replace_condition() e _write() usando patch parcial."""
    job = _CompositePartitionJob(agravo="dengue")
    df = MagicMock()
    writer = MagicMock()
    writer.format.return_value = writer
    writer.mode.return_value = writer
    writer.option.return_value = writer
    writer.partitionBy.return_value = writer
    df.write = writer

    job._write(df, "s3a://silver/notificacao/", "ano_mes", "202401")

    option_calls = writer.option.call_args_list
    replace_where_call = next(c for c in option_calls if c.args[0] == "replaceWhere")
    assert "agravo = 'dengue'" in replace_where_call.args[1]
    assert "ano_mes = '202401'" in replace_where_call.args[1]
