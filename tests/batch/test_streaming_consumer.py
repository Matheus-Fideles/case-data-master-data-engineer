"""Unit tests for pipelines/streaming/atendimento_consumer.py.

Tests the parse logic, valid/DLQ separation and Gold DataFrame assembly
without initialising Kafka, a real SparkSession or MinIO.

Strategy: functions that build Column objects (F.col, F.lit, etc.) are
patched in the module namespace — same approach as BronzeJob/SilverJob tests.
Orchestration functions (_make_foreachbatch) are tested by patching the
internal helpers (_parse_kafka, _write_bronze, etc.).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

# ── helpers de mock ───────────────────────────────────────────────────────────


def _make_df(rows: list[dict] | None = None, *, empty: bool = False) -> MagicMock:
    """DataFrame mock with full method chaining."""
    df = MagicMock()
    df.rdd.isEmpty.return_value = empty or not rows
    df.count.return_value = len(rows) if rows else 0
    df.filter.return_value = df
    df.select.return_value = df
    df.withColumn.return_value = df
    df.withWatermark.return_value = df
    df.alias.return_value = df
    writer = MagicMock()
    writer.format.return_value = writer
    writer.mode.return_value = writer
    writer.option.return_value = writer
    writer.partitionBy.return_value = writer
    writer.save = MagicMock()
    writer.jdbc = MagicMock()
    df.write = writer
    return df


def _mock_F():
    """Returns a pyspark.sql.functions mock without a SparkContext."""
    f = MagicMock()
    f.col.side_effect = lambda name: MagicMock(name=f"col({name})")
    f.lit.side_effect = lambda v: MagicMock(name=f"lit({v})")
    f.current_timestamp.return_value = MagicMock()
    f.date_format.return_value = MagicMock()
    f.to_timestamp.return_value = MagicMock()
    f.from_json.return_value = MagicMock()
    f.to_json.return_value = MagicMock()
    f.struct.return_value = MagicMock()
    return f


# ── testes de _parse_kafka ────────────────────────────────────────────────────


class TestParseKafka:
    def test_parse_returns_two_dfs(self):
        from apps.streaming.jobs import atendimento_consumer as c

        raw = _make_df([{"offset": 1, "partition": 0, "value": "{}"}])
        with patch.object(c, "F", _mock_F()):
            result = c._parse_kafka(raw)

        assert len(result) == 2

    def test_valid_and_invalid_dfs_come_from_filter(self):
        from apps.streaming.jobs import atendimento_consumer as c

        raw = _make_df([{"offset": 1}])
        with patch.object(c, "F", _mock_F()):
            valid, invalid = c._parse_kafka(raw)

        # both are the result of filter() on the same chained df
        assert raw.filter.call_count >= 2


# ── testes de _write_bronze ───────────────────────────────────────────────────


class TestWriteBronze:
    def test_uses_merge_when_delta_table_exists(self):
        from apps.streaming.jobs import atendimento_consumer as c

        microbatch = _make_df([{"id_atendimento": "abc"}])
        mock_dt = MagicMock()
        mock_dt.alias.return_value = mock_dt
        mock_dt.merge.return_value = mock_dt
        mock_dt.whenNotMatchedInsertAll.return_value = mock_dt

        with (
            patch.object(c, "F", _mock_F()),
            patch.object(c.DeltaTable, "isDeltaTable", return_value=True),
            patch.object(c.DeltaTable, "forPath", return_value=mock_dt),
        ):
            c._write_bronze(microbatch, batch_id=1)

        mock_dt.merge.assert_called_once()
        mock_dt.whenNotMatchedInsertAll.assert_called_once()
        mock_dt.execute.assert_called_once()

    def test_uses_append_when_no_delta_table(self):
        from apps.streaming.jobs import atendimento_consumer as c

        microbatch = _make_df([{"id_atendimento": "abc"}])

        with (
            patch.object(c, "F", _mock_F()),
            patch.object(c.DeltaTable, "isDeltaTable", return_value=False),
        ):
            c._write_bronze(microbatch, batch_id=2)

        microbatch.write.mode.assert_called_with("append")
        microbatch.write.save.assert_called_once_with(c.BRONZE_PATH)


# ── testes de _write_gold_postgres ────────────────────────────────────────────


class TestWriteGoldPostgres:
    def test_writes_via_jdbc_with_correct_table(self):
        from apps.streaming.jobs import atendimento_consumer as c

        microbatch = _make_df([{"ts_evento": "2024-01-01"}])
        mock_url = "jdbc:postgresql://host:5432/postgres"
        mock_props = {
            "user": "u",
            "password": "p",
            "driver": "org.postgresql.Driver",
            "currentSchema": "gold_dw",
        }

        with (
            patch.object(c, "F", _mock_F()),
            patch(
                "apps.streaming.jobs.atendimento_consumer.make_pg_connection",
                return_value=(mock_url, mock_props),
            ),
        ):
            c._write_gold_postgres(microbatch, batch_id=3)

        microbatch.write.jdbc.assert_called_once()
        jdbc_kwargs = microbatch.write.jdbc.call_args.kwargs
        assert jdbc_kwargs["url"] == mock_url
        assert jdbc_kwargs["mode"] == "append"
        assert jdbc_kwargs["table"] == "fato_atendimento_stream"

    def test_passes_pg_props_to_jdbc(self):
        from apps.streaming.jobs import atendimento_consumer as c

        microbatch = _make_df([{}])
        props = {
            "user": "eng",
            "password": "secret",
            "driver": "org.postgresql.Driver",
            "currentSchema": "gold_dw",
        }

        with (
            patch.object(c, "F", _mock_F()),
            patch(
                "apps.streaming.jobs.atendimento_consumer.make_pg_connection",
                return_value=("jdbc://x", props),
            ),
        ):
            c._write_gold_postgres(microbatch, batch_id=4)

        assert microbatch.write.jdbc.call_args.kwargs["properties"] == props


# ── testes de _send_to_dlq ────────────────────────────────────────────────────


class TestSendToDlq:
    def test_skips_when_df_is_empty(self):
        from apps.streaming.jobs import atendimento_consumer as c

        empty_df = _make_df(empty=True)
        c._send_to_dlq(MagicMock(), empty_df, "LATE_EVENT")

        empty_df.write.format.assert_not_called()

    def test_writes_to_kafka_topic(self):
        from apps.streaming.jobs import atendimento_consumer as c

        df = _make_df([{"id_atendimento": "x"}], empty=False)

        with patch.object(c, "F", _mock_F()):
            c._send_to_dlq(MagicMock(), df, "SCHEMA_INVALID")

        df.write.format.assert_called_with("kafka")
        df.write.save.assert_called_once()

    def test_dlq_topic_is_configured(self):
        from apps.streaming.jobs import atendimento_consumer as c

        df = _make_df([{"id_atendimento": "x"}], empty=False)

        with patch.object(c, "F", _mock_F()):
            c._send_to_dlq(MagicMock(), df, "LATE_EVENT")

        option_calls = {call.args[0]: call.args[1] for call in df.write.option.call_args_list}
        assert "topic" in option_calls
        assert option_calls["topic"] == c.KAFKA_DLQ_TOPIC


# ── testes do foreachBatch orquestrador ───────────────────────────────────────


class TestForEachBatch:
    def test_skips_empty_microbatch(self):
        from apps.streaming.jobs import atendimento_consumer as c

        process_fn = c._make_foreachbatch(MagicMock())
        empty = _make_df(empty=True)

        with (
            patch.object(c, "_parse_kafka") as m_parse,
            patch.object(c, "_write_bronze") as m_bronze,
        ):
            process_fn(empty, batch_id=0)

        m_parse.assert_not_called()
        m_bronze.assert_not_called()

    def test_calls_bronze_and_postgres_on_valid_batch(self):
        from apps.streaming.jobs import atendimento_consumer as c

        spark = MagicMock()
        process_fn = c._make_foreachbatch(spark)

        microbatch = _make_df([{"value": "{}"}], empty=False)
        valid_df = _make_df([{"id_atendimento": "a"}], empty=False)
        invalid_df = _make_df(empty=True)

        with (
            patch.object(c, "_parse_kafka", return_value=(valid_df, invalid_df)),
            patch.object(c, "_write_bronze") as m_bronze,
            patch.object(c, "_write_gold_postgres") as m_pg,
        ):
            process_fn(microbatch, batch_id=5)

        m_bronze.assert_called_once()
        m_pg.assert_called_once()

    def test_sends_invalid_records_to_dlq(self):
        from apps.streaming.jobs import atendimento_consumer as c

        spark = MagicMock()
        process_fn = c._make_foreachbatch(spark)

        microbatch = _make_df([{"value": "{}"}], empty=False)
        valid_df = _make_df(empty=True)
        invalid_df = _make_df([{"ts_evento": None}], empty=False)

        with (
            patch.object(c, "_parse_kafka", return_value=(valid_df, invalid_df)),
            patch.object(c, "_send_to_dlq") as m_dlq,
            patch.object(c, "_write_bronze"),
            patch.object(c, "_write_gold_postgres"),
        ):
            process_fn(microbatch, batch_id=6)

        m_dlq.assert_called_once_with(spark, invalid_df, "SCHEMA_INVALID")

    def test_postgres_failure_does_not_abort_batch(self):
        """A Postgres failure must not abort the entire micro-batch."""
        from apps.streaming.jobs import atendimento_consumer as c

        spark = MagicMock()
        process_fn = c._make_foreachbatch(spark)

        microbatch = _make_df([{"value": "{}"}], empty=False)
        valid_df = _make_df([{"id_atendimento": "a"}], empty=False)
        invalid_df = _make_df(empty=True)

        with (
            patch.object(c, "_parse_kafka", return_value=(valid_df, invalid_df)),
            patch.object(c, "_write_bronze") as m_bronze,
            patch.object(c, "_write_gold_postgres", side_effect=Exception("jdbc timeout")),
        ):
            process_fn(microbatch, batch_id=7)  # must not raise

        m_bronze.assert_called_once()  # Bronze must always be written
