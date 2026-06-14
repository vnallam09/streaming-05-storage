"""src/streaming/storage/storage_workouts.py.

DuckDB storage for the Phase 5 wearable-workouts scenario.

This is the workouts equivalent of storage_case.py. It persists consumed
workout messages to disk so they can be queried, counted, and analyzed while
the consumer runs.

This module creates two DuckDB tables:
  - consumed_valid_workouts: sessions that passed all validation checks,
    including the derived fields (calories, pace, intensity, hr_zone)
  - consumed_rejected_workouts: sessions that failed, with error details

The generic SQL builders (CREATE, DELETE, INSERT) come from
datafun_streaming.storage.duckdb_sql and work with any table.
The domain-specific table names and field lists are defined here.
"""

# === DECLARE IMPORTS ===

from pathlib import Path
from typing import Any, Final

from datafun_streaming.core.types import DataRecordDict
from datafun_streaming.storage.duckdb_sql import (
    build_clear_table_sql,
    build_create_table_sql,
    build_insert_sql,
)
from datafun_toolkit.logger import get_logger
import duckdb

from streaming.data_validation.data_contract_workouts import (
    REJECTED_WORKOUT_FIELDNAMES,
    VALID_WORKOUT_FIELDNAMES,
    WORKOUT_DERIVED_FIELDS,
)
from streaming.data_validation.data_validation_case import add_validation_errors

# === DECLARE EXPORTS ===

__all__ = [
    "CONSUMED_REJECTED_FIELDNAMES",
    "CONSUMED_VALID_FIELDNAMES",
    "REJECTED_TABLE_NAME",
    "VALID_TABLE_NAME",
    "clear_storage_tables",
    "create_storage_tables",
    "init_db",
    "log_storage_summary",
    "write_rejected_record",
    "write_valid_record",
]

# === CONFIGURE LOGGER ONCE PER PYTHON FILE (MODULE) ===

LOG = get_logger("C05-WORKOUTS-STORAGE", level="DEBUG")

# === DECLARE GLOBAL CONSTANTS FOR TABLES ===

VALID_TABLE_NAME: Final[str] = "consumed_valid_workouts"
REJECTED_TABLE_NAME: Final[str] = "consumed_rejected_workouts"

# The valid table stores the raw + optional fields, the DERIVED fields,
# and the Kafka metadata fields.
CONSUMED_VALID_FIELDNAMES: Final[list[str]] = [
    *VALID_WORKOUT_FIELDNAMES,
    *WORKOUT_DERIVED_FIELDS,
    "_kafka_key",
    "_kafka_partition",
    "_kafka_offset",
]

CONSUMED_REJECTED_FIELDNAMES: Final[list[str]] = [
    *REJECTED_WORKOUT_FIELDNAMES,
    "_kafka_key",
    "_kafka_partition",
    "_kafka_offset",
]


# === DEFINE HELPER FUNCTIONS ===


def clean_valid_consumed_record(record: dict[str, Any]) -> dict[str, Any]:
    """Keep only the fields written to the valid workouts table."""
    return {field: record.get(field, "") for field in CONSUMED_VALID_FIELDNAMES}


def clean_rejected_consumed_record(record: dict[str, Any]) -> dict[str, Any]:
    """Keep only the fields written to the rejected workouts table."""
    return {field: record.get(field, "") for field in CONSUMED_REJECTED_FIELDNAMES}


def create_storage_tables(db_path: Path) -> None:
    """Create the consumed workout tables if they do not exist."""
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            build_create_table_sql(VALID_TABLE_NAME, CONSUMED_VALID_FIELDNAMES)
        )
        conn.execute(
            build_create_table_sql(REJECTED_TABLE_NAME, CONSUMED_REJECTED_FIELDNAMES)
        )


def clear_storage_tables(db_path: Path) -> None:
    """Clear prior consumed workout rows for a fresh run."""
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(build_clear_table_sql(VALID_TABLE_NAME))
        conn.execute(build_clear_table_sql(REJECTED_TABLE_NAME))


def init_db(db_path: Path) -> None:
    """Create the expected storage tables and clear old rows."""
    create_storage_tables(db_path)
    clear_storage_tables(db_path)


def write_valid_record(db_path: Path, record: DataRecordDict) -> None:
    """Write one valid consumed workout record (with derived fields) to DuckDB."""
    clean_record = clean_valid_consumed_record(record)
    insert_sql = build_insert_sql(VALID_TABLE_NAME, CONSUMED_VALID_FIELDNAMES)
    insert_values = [clean_record[field] for field in CONSUMED_VALID_FIELDNAMES]
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(insert_sql, insert_values)


def write_rejected_record(
    db_path: Path, record: DataRecordDict, errors: list[str]
) -> None:
    """Write one rejected consumed workout record to DuckDB."""
    rejected_record = add_validation_errors(record=record, errors=errors)
    clean_record = clean_rejected_consumed_record(rejected_record)
    insert_sql = build_insert_sql(REJECTED_TABLE_NAME, CONSUMED_REJECTED_FIELDNAMES)
    insert_values = [clean_record[field] for field in CONSUMED_REJECTED_FIELDNAMES]
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(insert_sql, insert_values)


def log_storage_summary(db_path: Path) -> None:
    """Log DuckDB query results after consuming workout messages.

    Table names are module constants, never user input, so they are safe
    to interpolate directly in SQL (flagged for security linters with noqa).
    """
    sql_valid_count = f"SELECT COUNT(*) FROM {VALID_TABLE_NAME}"  # noqa: S608
    sql_rejected_count = f"SELECT COUNT(*) FROM {REJECTED_TABLE_NAME}"  # noqa: S608

    # Per-activity rollup using the stored derived fields.
    sql_by_activity = f"""
        SELECT activity,
               COUNT(*) AS session_count,
               SUM(CAST(duration_min AS INTEGER)) AS total_minutes,
               ROUND(SUM(CAST(calories AS DOUBLE)), 1) AS total_calories,
               ROUND(AVG(CAST(intensity_pct AS DOUBLE)), 1) AS avg_intensity_pct
        FROM {VALID_TABLE_NAME}
        GROUP BY activity
        ORDER BY total_calories DESC
        """  # noqa: S608

    # Per-user rollup using the stored derived fields.
    sql_by_user = f"""
        SELECT user_id,
               COUNT(*) AS session_count,
               ROUND(SUM(CAST(calories AS DOUBLE)), 1) AS total_calories
        FROM {VALID_TABLE_NAME}
        GROUP BY user_id
        ORDER BY total_calories DESC
        """  # noqa: S608

    with duckdb.connect(str(db_path)) as conn:
        valid_result = conn.execute(sql_valid_count).fetchone()
        valid_count = valid_result[0] if valid_result else 0

        rejected_result = conn.execute(sql_rejected_count).fetchone()
        rejected_count = rejected_result[0] if rejected_result else 0

        activity_rows = conn.execute(sql_by_activity).fetchall()
        user_rows = conn.execute(sql_by_user).fetchall()

    LOG.info(f"DuckDB valid row(s): {valid_count}")
    LOG.info(f"DuckDB rejected row(s): {rejected_count}")

    LOG.info("DuckDB workouts by activity:")
    for activity, sessions, minutes, calories, avg_intensity in activity_rows:
        LOG.info(
            f"  {activity}: {sessions} session(s), {minutes} min, "
            f"{calories:,.1f} kcal, avg intensity {avg_intensity}%"
        )

    LOG.info("DuckDB total calories by user:")
    for user_id, sessions, calories in user_rows:
        LOG.info(f"  {user_id}: {sessions} session(s), {calories:,.1f} kcal")
