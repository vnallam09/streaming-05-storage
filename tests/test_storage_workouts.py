"""Tests for streaming.storage.storage_workouts (Phase 5)."""

from pathlib import Path

import duckdb

from streaming.storage.storage_workouts import (
    CONSUMED_VALID_FIELDNAMES,
    REJECTED_TABLE_NAME,
    VALID_TABLE_NAME,
    create_storage_tables,
    write_rejected_record,
    write_valid_record,
)

# === FIXTURES ===

SAMPLE_VALID_RECORD = {
    "session_id": "SES-0001",
    "datetime": "2026-06-01T06:15:00Z",
    "user_id": "USR-001",
    "activity": "run",
    "duration_min": "42",
    "distance_km": "8.10",
    "avg_hr": "162",
    "device_type": "watch",
    "note": "Morning tempo run",
    "calories": 528.6,
    "pace_min_per_km": 5.19,
    "intensity_pct": 84.4,
    "hr_zone": 4,
    "_kafka_key": "USR-001",
    "_kafka_partition": "0",
    "_kafka_offset": "1",
}


def test_create_storage_tables_creates_both_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    create_storage_tables(db_path)
    with duckdb.connect(str(db_path)) as conn:
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
    assert VALID_TABLE_NAME in tables
    assert REJECTED_TABLE_NAME in tables


def test_valid_table_includes_derived_fields() -> None:
    """The valid workouts table must store the derived fields."""
    for field in ("calories", "pace_min_per_km", "intensity_pct", "hr_zone"):
        assert field in CONSUMED_VALID_FIELDNAMES


def test_write_valid_record_stores_derived_values(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    create_storage_tables(db_path)
    write_valid_record(db_path, SAMPLE_VALID_RECORD)
    with duckdb.connect(str(db_path)) as conn:
        sql = f"SELECT session_id, calories, hr_zone FROM {VALID_TABLE_NAME}"  # noqa: S608
        row = conn.execute(sql).fetchone()
    assert row is not None
    assert row[0] == "SES-0001"
    assert float(row[1]) == 528.6
    assert int(row[2]) == 4


def test_write_rejected_record_inserts_with_errors(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"
    create_storage_tables(db_path)
    write_rejected_record(db_path, SAMPLE_VALID_RECORD, ["Unknown user_id"])
    with duckdb.connect(str(db_path)) as conn:
        sql = f"SELECT COUNT(*) FROM {REJECTED_TABLE_NAME}"  # noqa: S608
        row = conn.execute(sql).fetchone()
        count = row[0] if row is not None else 0
    assert count == 1


def test_aggregate_calories_by_activity(tmp_path: Path) -> None:
    """The stored derived fields should support SQL rollups."""
    db_path = tmp_path / "test.duckdb"
    create_storage_tables(db_path)
    write_valid_record(db_path, SAMPLE_VALID_RECORD)
    write_valid_record(
        db_path, {**SAMPLE_VALID_RECORD, "session_id": "SES-0002", "calories": 100.0}
    )
    with duckdb.connect(str(db_path)) as conn:
        sql = f"""
            SELECT activity, ROUND(SUM(CAST(calories AS DOUBLE)), 1)
            FROM {VALID_TABLE_NAME}
            GROUP BY activity
            """  # noqa: S608
        rows = conn.execute(sql).fetchall()
    assert rows == [("run", 628.6)]
