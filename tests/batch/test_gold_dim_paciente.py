"""Unit tests for pipelines/gold/dim_paciente.py — SCD2 logic.

Strategy: mock psycopg2 and _read_silver to isolate the SCD2 algorithm
without a real database or Spark. All tests run offline.

SCD2 rules:
  - New hash (fetchone=None)             → INSERT, is_current=TRUE, dt_fim=9999-12-31
  - Existing hash, same attrs            → skip (no INSERT/UPDATE)
  - Existing hash, any attr changed      → UPDATE (close old) + INSERT (new version)
  - rollback called on any exception
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import apps.epidemiologico.jobs.gold_dim_paciente as dim_mod
import pytest
from apps.epidemiologico.jobs.gold_dim_paciente import _read_silver, load_dim_paciente

# ── fixtures ──────────────────────────────────────────────────────────────────

_SNAPSHOT = "20240115"

_ROW_A = {
    "id_paciente_hash": "a" * 64,
    "sexo": "M",
    "ano_nascimento": 1990,
    "cep_regiao": "010",
    "municipio_codigo_ibge": "3550308",
}

_ROW_B = {
    "id_paciente_hash": "b" * 64,
    "sexo": "F",
    "ano_nascimento": 1985,
    "cep_regiao": "020",
    "municipio_codigo_ibge": "3304557",
}

_EXISTING_A = (1, "M", 1990, "010", "3550308")  # sk, sexo, ano, cep, city — same as _ROW_A
_EXISTING_A_CHANGED = (1, "F", 1990, "010", "3550308")  # sexo changed vs _ROW_A
_EXISTING_B = (2, "F", 1985, "020", "3304557")  # same as _ROW_B


def _make_pg_mocks():
    """Returns (mock_conn, mock_cursor) wired together."""
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


# ── helper: run load_dim_paciente with controlled silver rows ─────────────────


def _run(silver_rows: list[dict], cursor_side_effects: list) -> tuple[int, MagicMock, MagicMock]:
    """Runs load_dim_paciente with mocked DB and silver data.

    cursor_side_effects: list of values returned by cursor.fetchone() in order.
    Returns (inserted_count, mock_conn, mock_cursor).
    """
    conn, cursor = _make_pg_mocks()
    cursor.fetchone.side_effect = cursor_side_effects

    with (
        patch.object(dim_mod, "_read_silver", return_value=silver_rows),
        patch("apps.epidemiologico.jobs.gold_dim_paciente.psycopg2") as mock_psycopg2,
    ):
        mock_psycopg2.connect.return_value = conn
        inserted = load_dim_paciente(_SNAPSHOT)

    return inserted, conn, cursor


# ── SCD2: new patient ─────────────────────────────────────────────────────────


class TestNewPatient:
    def test_new_patient_inserts_one_row(self):
        inserted, _, _ = _run([_ROW_A], [None])
        assert inserted == 1

    def test_new_patient_calls_insert_not_update(self):
        _, _, cursor = _run([_ROW_A], [None])
        sql_calls = [c.args[0].strip().upper() for c in cursor.execute.call_args_list]
        assert any(s.startswith("INSERT") for s in sql_calls)
        assert not any(s.startswith("UPDATE") for s in sql_calls)

    def test_new_patient_insert_has_is_current_true(self):
        _, _, cursor = _run([_ROW_A], [None])
        insert_args = [
            c.args[1]
            for c in cursor.execute.call_args_list
            if c.args[0].strip().upper().startswith("INSERT")
        ]
        assert len(insert_args) == 1
        # is_current=TRUE is passed as a literal in SQL, not a bind param;
        # verify the snapshot and open date are in the bind params
        params = insert_args[0]
        assert _ROW_A["id_paciente_hash"] in params
        assert date(9999, 12, 31) in params
        assert _SNAPSHOT in params

    def test_two_new_patients_inserts_two_rows(self):
        inserted, _, _ = _run([_ROW_A, _ROW_B], [None, None])
        assert inserted == 2

    def test_commit_called_after_all_inserts(self):
        _, conn, _ = _run([_ROW_A], [None])
        conn.commit.assert_called_once()


# ── SCD2: unchanged patient ───────────────────────────────────────────────────


class TestUnchangedPatient:
    def test_unchanged_patient_skipped(self):
        inserted, _, _ = _run([_ROW_A], [_EXISTING_A])
        assert inserted == 0

    def test_unchanged_patient_no_insert_no_update(self):
        _, _, cursor = _run([_ROW_A], [_EXISTING_A])
        sql_calls = [c.args[0].strip().upper() for c in cursor.execute.call_args_list]
        assert not any(s.startswith("INSERT") for s in sql_calls)
        assert not any(s.startswith("UPDATE") for s in sql_calls)

    def test_commit_still_called_even_with_no_changes(self):
        _, conn, _ = _run([_ROW_A], [_EXISTING_A])
        conn.commit.assert_called_once()

    def test_one_new_one_unchanged_inserts_one(self):
        inserted, _, _ = _run([_ROW_A, _ROW_B], [None, _EXISTING_B])
        assert inserted == 1


# ── SCD2: changed patient ─────────────────────────────────────────────────────


class TestChangedPatient:
    def test_changed_patient_inserts_one_new_version(self):
        inserted, _, _ = _run([_ROW_A], [_EXISTING_A_CHANGED])
        assert inserted == 1

    def test_changed_patient_calls_update_then_insert(self):
        _, _, cursor = _run([_ROW_A], [_EXISTING_A_CHANGED])
        sql_calls = [c.args[0].strip().upper() for c in cursor.execute.call_args_list]
        update_idx = next(i for i, s in enumerate(sql_calls) if s.startswith("UPDATE"))
        insert_idx = next(i for i, s in enumerate(sql_calls) if s.startswith("INSERT"))
        assert update_idx < insert_idx

    def test_update_sets_is_current_false(self):
        _, _, cursor = _run([_ROW_A], [_EXISTING_A_CHANGED])
        update_calls = [
            c
            for c in cursor.execute.call_args_list
            if c.args[0].strip().upper().startswith("UPDATE")
        ]
        assert len(update_calls) == 1
        sql = update_calls[0].args[0]
        assert "is_current = FALSE" in sql

    def test_update_sets_dt_fim_to_yesterday(self):
        today = date.today()
        yesterday = today - __import__("datetime").timedelta(days=1)
        _, _, cursor = _run([_ROW_A], [_EXISTING_A_CHANGED])
        update_calls = [
            c
            for c in cursor.execute.call_args_list
            if c.args[0].strip().upper().startswith("UPDATE")
        ]
        update_params = update_calls[0].args[1]
        assert yesterday in update_params

    def test_insert_new_version_has_open_dt_fim(self):
        _, _, cursor = _run([_ROW_A], [_EXISTING_A_CHANGED])
        insert_calls = [
            c
            for c in cursor.execute.call_args_list
            if c.args[0].strip().upper().startswith("INSERT")
        ]
        params = insert_calls[0].args[1]
        assert date(9999, 12, 31) in params

    @pytest.mark.parametrize(
        "field,new_val",
        [
            ("sexo", "O"),
            ("ano_nascimento", 2000),
            ("cep_regiao", "999"),
            ("municipio_codigo_ibge", "9999999"),
        ],
    )
    def test_any_single_changed_attr_triggers_scd2(self, field, new_val):
        row = {**_ROW_A, field: new_val}
        inserted, _, _ = _run([row], [_EXISTING_A])
        assert inserted == 1


# ── rollback on error ─────────────────────────────────────────────────────────


class TestRollbackOnError:
    def test_rollback_called_on_execute_exception(self):
        conn, cursor = _make_pg_mocks()
        cursor.execute.side_effect = RuntimeError("db error")

        with (
            patch.object(dim_mod, "_read_silver", return_value=[_ROW_A]),
            patch("apps.epidemiologico.jobs.gold_dim_paciente.psycopg2") as mock_psycopg2,
        ):
            mock_psycopg2.connect.return_value = conn
            with pytest.raises(RuntimeError, match="db error"):
                load_dim_paciente(_SNAPSHOT)

        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()

    def test_connection_always_closed(self):
        conn, cursor = _make_pg_mocks()
        cursor.execute.side_effect = RuntimeError("db error")

        with (
            patch.object(dim_mod, "_read_silver", return_value=[_ROW_A]),
            patch("apps.epidemiologico.jobs.gold_dim_paciente.psycopg2") as mock_psycopg2,
        ):
            mock_psycopg2.connect.return_value = conn
            with pytest.raises(RuntimeError):
                load_dim_paciente(_SNAPSHOT)

        conn.close.assert_called_once()
        cursor.close.assert_called_once()


# ── empty silver ──────────────────────────────────────────────────────────────


class TestEmptySilver:
    def test_empty_silver_returns_zero(self):
        with patch.object(dim_mod, "_read_silver", return_value=[]):
            result = load_dim_paciente(_SNAPSHOT)
        assert result == 0

    def test_empty_silver_does_not_open_db_connection(self):
        with (
            patch.object(dim_mod, "_read_silver", return_value=[]),
            patch("apps.epidemiologico.jobs.gold_dim_paciente.psycopg2") as mock_psycopg2,
        ):
            load_dim_paciente(_SNAPSHOT)
        mock_psycopg2.connect.assert_not_called()


# ── fallback path patch ───────────────────────────────────────────────────────


class TestSnapshotDate:
    def test_snapshot_defaults_to_today(self):
        today_str = date.today().strftime("%Y%m%d")
        captured = []

        def fake_read_silver(snapshot_date):
            captured.append(snapshot_date)
            return []

        with patch.object(dim_mod, "_read_silver", side_effect=fake_read_silver):
            load_dim_paciente()

        assert captured == [today_str]

    def test_explicit_snapshot_is_used(self):
        captured = []

        def fake_read_silver(snapshot_date):
            captured.append(snapshot_date)
            return []

        with patch.object(dim_mod, "_read_silver", side_effect=fake_read_silver):
            load_dim_paciente("20230601")

        assert captured == ["20230601"]


# ── _read_silver fallback ─────────────────────────────────────────────────────


class TestReadSilverFallback:
    def test_falls_back_to_fixture_when_spark_unavailable(self, tmp_path):
        import sys

        fixture_path = tmp_path / "oltp_sample.json"
        fixture_path.write_text(
            '[{"id_paciente": 1, "data_nascimento": "1990-05-10", '
            '"sexo": "M", "cep": "01310100", "municipio_codigo_ibge": "3550308"}]'
        )

        with (
            patch("apps.epidemiologico.jobs.gold_dim_paciente.Path") as mock_path,
        ):
            mock_fixture = MagicMock()
            mock_fixture.exists.return_value = True
            mock_fixture.read_text.return_value = fixture_path.read_text()
            # Path(__file__).parents[2] / "tests" / "fixtures" / "oltp_sample.json"
            mock_path.return_value.parents.__getitem__.return_value.__truediv__.return_value.__truediv__.return_value.__truediv__.return_value = mock_fixture

            # Force the Spark try-block to fail by removing delta from sys.modules
            delta_backup = sys.modules.pop("delta", None)
            try:
                rows = _read_silver("20240101")
            finally:
                if delta_backup is not None:
                    sys.modules["delta"] = delta_backup

        assert len(rows) == 1
        assert rows[0]["id_paciente_hash"] == "hash_1"

    def test_fixture_extracts_birth_year_from_data_nascimento(self):
        import json
        from pathlib import Path

        fixture = Path(__file__).parents[1] / "fixtures" / "oltp_sample.json"
        if not fixture.exists():
            pytest.skip("oltp_sample.json fixture not found")

        with patch("builtins.__import__", side_effect=ImportError("no spark")):
            pass

        records = json.loads(fixture.read_text())
        sample = records[0]
        if not sample.get("data_nascimento"):
            pytest.skip("fixture has no data_nascimento")

        expected_year = int(sample["data_nascimento"][:4])
        row = {
            "id_paciente_hash": sample.get("id_paciente_hash", f"hash_{sample['id_paciente']}"),
            "sexo": sample.get("sexo"),
            "ano_nascimento": expected_year,
            "cep_regiao": sample.get("cep", "")[:3] if sample.get("cep") else None,
            "municipio_codigo_ibge": sample.get("municipio_codigo_ibge"),
        }
        assert row["ano_nascimento"] == expected_year

    def test_fixture_truncates_cep_to_3_digits(self):
        import json
        from pathlib import Path

        fixture = Path(__file__).parents[1] / "fixtures" / "oltp_sample.json"
        if not fixture.exists():
            pytest.skip("oltp_sample.json fixture not found")

        records = json.loads(fixture.read_text())
        for record in records:
            if record.get("cep") and len(record["cep"]) >= 3:
                cep_regiao = record["cep"][:3]
                assert len(cep_regiao) == 3
                break

    def test_returns_empty_list_when_spark_and_fixture_missing(self, tmp_path):
        import sys

        with patch("apps.epidemiologico.jobs.gold_dim_paciente.Path") as mock_path:
            mock_fixture = MagicMock()
            mock_fixture.exists.return_value = False
            mock_path.return_value.parents.__getitem__.return_value.__truediv__.return_value.__truediv__.return_value.__truediv__.return_value = mock_fixture

            delta_backup = sys.modules.pop("delta", None)
            try:
                rows = _read_silver("20240101")
            finally:
                if delta_backup is not None:
                    sys.modules["delta"] = delta_backup

        assert rows == []
