"""src/streaming/kafka_consumer_workouts_teja.py.

Kafka consumer for the Phase 5 wearable-workouts scenario.

Reads workout-session messages from a Kafka topic and runs the full pipeline:
  - Validates each message against the workouts data contract
  - Computes derived fields (calories, pace, intensity, hr_zone)
  - Stores each valid record in DuckDB and writes it to a local CSV file
  - Stores rejected records in DuckDB with their validation errors

This is the workouts equivalent of kafka_consumer_case.py. It runs against its
own topic (WORKOUTS_TOPIC) and its own DuckDB tables so the case and Phase 4
pipelines are unaffected.

Start with main() at the bottom.

Author: Teja
Date: 2026-06

Terminal command to run this file from the root project folder:

    uv run python -m streaming.kafka_consumer_workouts_teja
"""

# === DECLARE IMPORTS ===

import dataclasses
import os
from pathlib import Path
from typing import Any, Final

from confluent_kafka.cimpl import OFFSET_BEGINNING, TopicPartition
from datafun_streaming.io.io_utils import append_csv_row, read_csv_rows
from datafun_streaming.kafka.kafka_admin_utils import (
    create_admin_client,
    get_topic_message_count,
    topic_exists,
)
from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_consumer_utils import (
    consume_kafka_message,
    create_consumer,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_streaming.stats.stats_utils import RunningStats
from datafun_toolkit.logger import get_logger, log_header, log_path
from dotenv import load_dotenv

from streaming.core.utils import log_env_vars
from streaming.data_engineering.workout_fields import enrich_workout
from streaming.data_validation.data_contract_workouts import (
    CONSUMED_WORKOUT_FIELDNAMES,
    USERS_REQUIRED_FIELDS,
    validate_workout_record,
)
from streaming.data_validation.data_validation_case import (
    make_lookup_set,
    validate_reference_records,
)
from streaming.storage.storage_workouts import (
    init_db,
    log_storage_summary,
    write_rejected_record,
    write_valid_record,
)

# === CONFIGURE LOGGER ===

LOG = get_logger("C05-WORKOUTS", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===

load_dotenv(override=True)
log_env_vars(LOG)

# === DECLARE GLOBAL CONSTANTS ===

WORKOUTS_TOPIC: Final[str] = os.getenv(
    "KAFKA_TOPIC_WORKOUTS", "streaming-05-storage-workouts"
)
TIMEOUT_SECONDS: Final[float] = float(os.getenv("CONSUMER_TIMEOUT_SECONDS", "10.0"))
MAX_MESSAGES: Final[int] = int(os.getenv("CONSUMER_MAX_MESSAGES", "1000"))

# === DECLARE CONSTANT PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
OUTPUT_DIR: Final[Path] = DATA_DIR / "output"

OUTPUT_CSV: Final[Path] = OUTPUT_DIR / "consumed_workouts.csv"
OUTPUT_DB: Final[Path] = OUTPUT_DIR / "workouts.duckdb"

USERS_CSV: Final[Path] = DATA_DIR / "workout_users.csv"


# ==========================================================
# DEFINE SECTION A. ACQUIRE RESOURCES AND GET READY HELPERS
# ==========================================================


def log_paths() -> None:
    """Log run header and all paths."""
    log_header(LOG, "C05-WORKOUTS")
    LOG.info("========================")
    LOG.info("START workouts consumer main()")
    LOG.info("========================")
    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_DIR", DATA_DIR)
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)
    log_path(LOG, "OUTPUT_DB", OUTPUT_DB)
    log_path(LOG, "USERS_CSV", USERS_CSV)


def load_settings() -> KafkaSettings:
    """Load settings from .env, override the topic, and log them.

    Returns:
        A KafkaSettings instance using the workouts topic.
    """
    LOG.info("Loading settings from .env...")
    settings = dataclasses.replace(KafkaSettings.from_env(), topic=WORKOUTS_TOPIC)
    LOG.info(f"KAFKA_BOOTSTRAP_SERVERS  = {settings.bootstrap_servers}")
    LOG.info(f"KAFKA_TOPIC (workouts)   = {settings.topic}")
    LOG.info(f"KAFKA_GROUP_ID           = {settings.group_id}")
    LOG.info(f"CONSUMER_TIMEOUT_SECONDS = {TIMEOUT_SECONDS}")
    LOG.info(f"CONSUMER_MAX_MESSAGES    = {MAX_MESSAGES}")
    return settings


def verify_connection(settings: KafkaSettings) -> None:
    """Verify Kafka is reachable before doing anything else.

    Raises:
        SystemExit: If Kafka is not reachable.
    """
    LOG.info("Verifying Kafka connection...")
    try:
        verify_kafka_connection(settings)
        LOG.info("Kafka port is reachable.")
    except ConnectionError as error:
        LOG.error(str(error))
        raise SystemExit(1) from error


def verify_topic(settings: KafkaSettings) -> None:
    """Verify the topic exists and has messages.

    Raises:
        SystemExit: If the topic does not exist or is empty.
    """
    LOG.info("Verifying Kafka topic...")
    admin = create_admin_client(settings)

    if not topic_exists(admin, settings.topic):
        LOG.error(f"Topic {settings.topic!r} does not exist.")
        LOG.error("Run the producer first.")
        raise SystemExit(1)

    message_count = get_topic_message_count(admin, settings.topic, settings)
    LOG.info(f"Topic {settings.topic!r} exists.")
    LOG.info(f"Found {message_count} message(s) available.")

    if message_count == 0:
        LOG.error("Topic is empty. Run the producer first.")
        raise SystemExit(1)


def get_kafka_consumer(settings: KafkaSettings) -> Any:
    """Create a Kafka consumer subscribed to the topic.

    Resets offsets to the beginning so this example reads all available messages.

    Returns:
        A confluent_kafka.Consumer instance subscribed to the topic.
    """
    LOG.info("Creating Kafka consumer...")
    consumer = create_consumer(settings)
    consumer.subscribe(
        [settings.topic],
        on_assign=lambda c, partitions: c.assign(
            [
                TopicPartition(
                    partition.topic,
                    partition.partition,
                    OFFSET_BEGINNING,
                )
                for partition in partitions
            ]
        ),
    )
    LOG.info(f"Subscribed to topic: {settings.topic!r} (reading from beginning)")
    return consumer


# ===========================================================================
# DEFINE SECTION C. CONSUME AND PROCESS MESSAGES HELPERS
# ===========================================================================


def initialize_output() -> RunningStats:
    """Initialize output resources.

    Returns:
        A RunningStats instance (accumulates calories burned).
    """
    LOG.info("Initializing output...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if OUTPUT_CSV.exists():
        OUTPUT_CSV.unlink()
    LOG.info(f"Output CSV cleared: {OUTPUT_CSV.name}")

    init_db(OUTPUT_DB)
    LOG.info(f"Database initialized: {OUTPUT_DB.name}")

    return RunningStats()


def load_reference_data() -> tuple[dict[str, dict[str, float]], set[str]]:
    """Load user reference data for enrichment and validation.

    Returns:
        A tuple of:
          - user_lookup: maps user_id to {"weight_kg", "max_hr"}
          - valid_user_ids: the set of known user_id values
    """
    LOG.info("Loading enrichment reference data...")
    user_records = read_csv_rows(USERS_CSV)

    errors = validate_reference_records(
        records=user_records,
        required_fields=USERS_REQUIRED_FIELDS,
        label="workout_users.csv",
    )
    if errors:
        for error in errors:
            LOG.error(error)
        raise SystemExit(1)

    user_lookup: dict[str, dict[str, float]] = {
        record["user_id"]: {
            "weight_kg": float(record["weight_kg"]),
            "max_hr": float(record["max_hr"]),
        }
        for record in user_records
    }
    valid_user_ids = make_lookup_set(user_records, "user_id")
    LOG.info(f"Found {len(user_lookup)} user profiles.")
    return user_lookup, valid_user_ids


def process_message(
    row: dict[str, Any],
    *,
    user_lookup: dict[str, dict[str, float]],
    valid_user_ids: set[str],
    stats: RunningStats,
) -> dict[str, Any] | None:
    """Process one consumed message.

    Steps:
      - Validate against the workouts data contract
      - Enrich with derived fields
      - Update running statistics (calories)

    Arguments:
        row: A raw consumed Kafka message row.
        user_lookup: User profiles by user_id.
        valid_user_ids: Known user IDs.
        stats: Running statistics accumulator.

    Returns:
        The enriched row, or None if validation failed.
    """
    result = validate_workout_record(record=row, valid_user_ids=valid_user_ids)
    if not result.is_valid:
        LOG.warning(f"Validation failed for session {row.get('session_id', '?')}")
        LOG.warning(f"errors={result.errors}")
        # Persist the rejected record (with errors) for later analysis.
        write_rejected_record(OUTPUT_DB, row, result.errors)
        return None

    enriched = enrich_workout(row, user_lookup)
    LOG.info(f"calories={enriched['calories']}")
    LOG.info(f"pace_min_per_km={enriched['pace_min_per_km']}")
    LOG.info(f"intensity_pct={enriched['intensity_pct']}")
    LOG.info(f"hr_zone={enriched['hr_zone']}")

    stats.update(enriched["calories"])

    return enriched


def consume_messages(
    consumer: Any,
    *,
    user_lookup: dict[str, dict[str, float]],
    valid_user_ids: set[str],
    stats: RunningStats,
) -> tuple[int, int]:
    """Consume and process messages from the Kafka topic.

    Runs until MAX_MESSAGES is reached or TIMEOUT_SECONDS elapses
    with no new message.

    Arguments:
        consumer: An open Kafka consumer subscribed to the topic.
        user_lookup: User profiles by user_id.
        valid_user_ids: Known user IDs.
        stats: Running statistics accumulator.

    Returns:
        A tuple of (consumed_count, skipped_count).
    """
    LOG.info("Consuming messages...")
    LOG.info(f"Waiting for up to {MAX_MESSAGES} message(s).")
    LOG.info("Press CTRL+C to stop early.\n")

    consumed_count = 0
    skipped_count = 0

    while consumed_count + skipped_count < MAX_MESSAGES:
        row = consume_kafka_message(
            consumer=consumer,
            timeout_seconds=TIMEOUT_SECONDS,
        )

        if row is None:
            LOG.info(f"No message received within {TIMEOUT_SECONDS}s timeout.")
            LOG.info("Producer finished or paused. Stopping consumer.")
            break

        LOG.info(row)

        enriched = process_message(
            row,
            user_lookup=user_lookup,
            valid_user_ids=valid_user_ids,
            stats=stats,
        )

        if enriched is None:
            skipped_count += 1
            LOG.warning("MESSAGE REJECTED")
            LOG.warning(f"session={row.get('session_id', '?')}")
            LOG.warning(f"skipped={skipped_count}")
            continue

        # Write the valid, enriched record to DuckDB.
        write_valid_record(OUTPUT_DB, enriched)
        LOG.info("Wrote valid record to DuckDB:")
        LOG.info(f"  session={enriched['session_id']}")

        # Also update the CSV.
        append_csv_row(
            path=OUTPUT_CSV,
            row={
                field: enriched.get(field, "") for field in CONSUMED_WORKOUT_FIELDNAMES
            },
            fieldnames=CONSUMED_WORKOUT_FIELDNAMES,
        )

        consumed_count += 1
        LOG.info("MESSAGE ACCEPTED")
        LOG.info(f"session={enriched['session_id']}")
        LOG.info(f"calories={enriched['calories']:.1f}")
        LOG.info(f"consumed={consumed_count}")
        LOG.info("RUNNING STATS (calories)")
        LOG.info(f"total_calories={stats.total:,.1f}")
        LOG.info(f"average={stats.mean:,.1f}")
        LOG.info(f"min={stats.minimum:,.1f}")
        LOG.info(f"max={stats.maximum:,.1f}")

    return consumed_count, skipped_count


def save_artifacts() -> None:
    """Save output artifacts or note their location."""
    LOG.info("Saving artifacts...")
    log_path(LOG, "WROTE OUTPUT_CSV", OUTPUT_CSV)
    log_path(LOG, "WROTE OUTPUT_DB", OUTPUT_DB)


# ===========================================================================
# DEFINE SECTION E. EXIT AND CLEANUP HELPERS
# ===========================================================================


def log_summary(
    consumed_count: int,
    skipped_count: int,
    stats: RunningStats,
    settings: KafkaSettings,
) -> None:
    """Log final summary statistics."""
    LOG.info("Summary:")
    LOG.info(f"Consumed {consumed_count} message(s) from topic {settings.topic!r}.")
    LOG.info(f"Skipped  {skipped_count} message(s).")
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)

    if stats.count > 0:
        LOG.info(f"  Total calories:  {stats.total:,.1f}")
        LOG.info(f"  Average session: {stats.mean:,.1f}")
        LOG.info(f"  Minimum session: {stats.minimum:,.1f}")
        LOG.info(f"  Maximum session: {stats.maximum:,.1f}")

    # Report the DuckDB rollups (by activity and by user).
    log_storage_summary(OUTPUT_DB)

    LOG.info("========================")
    LOG.info("Consumer executed successfully!")
    LOG.info("========================")


# ===========================================================================
# MAIN FUNCTION
# ===========================================================================


def main() -> None:
    """Main entry point for the Kafka workouts consumer."""
    log_paths()

    LOG.info("========================")
    LOG.info("SECTION A. Acquire")
    LOG.info("========================")

    settings = load_settings()
    verify_connection(settings)
    verify_topic(settings)
    consumer = get_kafka_consumer(settings)

    LOG.info("========================")
    LOG.info("SECTION C. Consume and Process Messages")
    LOG.info("========================")

    stats = initialize_output()
    user_lookup, valid_user_ids = load_reference_data()

    consumed_count = 0
    skipped_count = 0

    try:
        consumed_count, skipped_count = consume_messages(
            consumer,
            user_lookup=user_lookup,
            valid_user_ids=valid_user_ids,
            stats=stats,
        )
    finally:
        consumer.close()
        LOG.info("Kafka consumer closed.")

    save_artifacts()

    LOG.info("========================")
    LOG.info("SECTION E. Exit")
    LOG.info("========================")

    log_summary(consumed_count, skipped_count, stats, settings)


# === CONDITIONAL EXECUTION GUARD ===

if __name__ == "__main__":
    main()
