"""src/streaming/kafka_producer_workouts_teja.py.

Kafka producer for the Phase 5 wearable-workouts scenario.

Reads workout sessions from data/workouts.csv, validates them against the
workouts data contract, writes rejected records to a local CSV file, and sends
valid records to a Kafka topic one message at a time.

This is the workouts equivalent of kafka_producer_case.py. It runs against its
own topic (WORKOUTS_TOPIC) so the case producer/consumer are unaffected.

Start with main() at the bottom.

Author: Teja
Date: 2026-06

Terminal command to run this file from the root project folder:

    uv run python -m streaming.kafka_producer_workouts_teja
"""

# === DECLARE IMPORTS ===

from collections.abc import Generator
import dataclasses
import os
from pathlib import Path
import time
from typing import Any, Final

from datafun_streaming.core.types import DataRecordDict
from datafun_streaming.io.errors import missing_csv_field_message
from datafun_streaming.io.io_utils import (
    append_csv_row,
    format_message_for_log,
    read_csv_rows,
)
from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_producer_utils import (
    create_producer,
    prepare_producer_topic,
    produce_kafka_message,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_toolkit.logger import get_logger, log_header, log_path
from dotenv import load_dotenv

from streaming.core.utils import log_env_vars
from streaming.data_validation.data_contract_workouts import (
    REJECTED_WORKOUT_FIELDNAMES,
    USERS_REQUIRED_FIELDS,
    keep_workout_fields,
    validate_workout_record,
)
from streaming.data_validation.data_validation_case import (
    add_validation_errors,
    make_lookup_set,
    validate_reference_records,
)

# === CONFIGURE LOGGER ===

LOG = get_logger("P05-WORKOUTS", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===

load_dotenv(override=True)
log_env_vars(LOG)

# === DECLARE GLOBAL CONSTANTS ===

# A dedicated topic so this scenario does not collide with the case topic.
WORKOUTS_TOPIC: Final[str] = os.getenv(
    "KAFKA_TOPIC_WORKOUTS", "streaming-05-storage-workouts"
)

msg_count = os.getenv("PRODUCER_MESSAGE_COUNT", "20")
msg_interval_seconds = os.getenv("PRODUCER_MESSAGE_INTERVAL_SECONDS", "2.0")

MESSAGE_COUNT: Final[int] = int(msg_count)
MESSAGE_INTERVAL_SECONDS: Final[float] = float(msg_interval_seconds)

# === DECLARE CONSTANT PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
OUTPUT_DIR: Final[Path] = DATA_DIR / "output"

WORKOUTS_CSV: Final[Path] = DATA_DIR / "workouts.csv"
USERS_CSV: Final[Path] = DATA_DIR / "workout_users.csv"
REJECTED_WORKOUTS_CSV: Final[Path] = OUTPUT_DIR / "producer_rejected_workouts.csv"


# ==========================================================
# DEFINE SECTION A. ACQUIRE RESOURCES AND GET READY HELPERS
# ==========================================================


def log_paths() -> None:
    """Log run header and all paths."""
    log_header(LOG, "P05")
    LOG.info("========================")
    LOG.info("START workouts producer main()")
    LOG.info("========================")
    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_DIR", DATA_DIR)
    log_path(LOG, "WORKOUTS_CSV", WORKOUTS_CSV)
    log_path(LOG, "USERS_CSV", USERS_CSV)
    log_path(LOG, "REJECTED_WORKOUTS_CSV", REJECTED_WORKOUTS_CSV)


def load_settings() -> KafkaSettings:
    """Load settings from .env, override the topic, and log them.

    Returns:
        A KafkaSettings instance using the workouts topic.
    """
    LOG.info("Loading settings from .env...")
    settings = dataclasses.replace(KafkaSettings.from_env(), topic=WORKOUTS_TOPIC)
    LOG.info(f"KAFKA_BOOTSTRAP_SERVERS           = {settings.bootstrap_servers}")
    LOG.info(f"KAFKA_TOPIC (workouts)            = {settings.topic}")
    LOG.info(f"PRODUCER_MESSAGE_COUNT            = {MESSAGE_COUNT}")
    LOG.info(f"PRODUCER_MESSAGE_INTERVAL_SECONDS = {MESSAGE_INTERVAL_SECONDS}")
    LOG.info(f"KAFKA_CLEAR_TOPIC_ON_START        = {settings.clear_topic_on_start}")
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


def load_reference_data() -> set[str]:
    """Load and validate user reference data.

    Returns:
        The set of valid user_id values.

    Raises:
        SystemExit: If the reference file is missing or invalid.
    """
    LOG.info("Loading validation reference data...")
    user_records = read_csv_rows(USERS_CSV)

    errors = validate_reference_records(
        records=user_records,
        required_fields=USERS_REQUIRED_FIELDS,
        label="workout_users.csv",
    )

    if errors:
        for error in errors:
            LOG.error(error)
        LOG.error("Reference data failed validation. Fix reference files first.")
        raise SystemExit(1)

    valid_user_ids = make_lookup_set(user_records, "user_id")
    LOG.info(f"Found {len(valid_user_ids)} valid users.")
    return valid_user_ids


# ===========================================================================
# DEFINE SECTION P. PRODUCE MESSAGES HELPERS
# ===========================================================================


def get_message_key(message: dict[str, Any]) -> str:
    """Return the Kafka message key for a workout record.

    We use user_id as the key so all sessions from the same user
    go to the same Kafka partition, keeping them in order.
    """
    try:
        return str(message["user_id"])
    except KeyError as error:
        msg = missing_csv_field_message(
            field="user_id",
            available_fields=list(message.keys()),
        )
        raise KeyError(msg) from error


def generate_messages(count: int) -> Generator[dict[str, str]]:
    """Generate a stream of workout sessions from the input CSV file.

    Arguments:
        count: How many sessions to generate.

    Yields:
        One workout row dictionary at a time.
    """
    workout_rows = read_csv_rows(WORKOUTS_CSV)
    yield from workout_rows[:count]


def write_rejected_record(record: DataRecordDict, errors: list[str]) -> None:
    """Write one rejected record to the rejected output CSV.

    Trim the record to the required fields first so the row matches
    REJECTED_WORKOUT_FIELDNAMES exactly. The raw message may also carry
    optional fields (e.g. note, device_type), and the CSV writer raises
    on keys that are not in fieldnames.
    """
    append_csv_row(
        path=REJECTED_WORKOUTS_CSV,
        row=add_validation_errors(record=keep_workout_fields(record), errors=errors),
        fieldnames=REJECTED_WORKOUT_FIELDNAMES,
    )


def initialize_output() -> None:
    """Initialize output directory and clear rejected CSV from prior runs."""
    LOG.info("Initializing output...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if REJECTED_WORKOUTS_CSV.exists():
        REJECTED_WORKOUTS_CSV.unlink()
    LOG.info(f"Output directory ready: {OUTPUT_DIR.name}")


def send_messages(
    producer: Any,
    settings: KafkaSettings,
    valid_user_ids: set[str],
) -> tuple[int, int]:
    """Generate, validate, and send messages to the Kafka topic.

    For each message:
      - Validate it against the workouts data contract.
      - If invalid, write it to the rejected CSV and skip.
      - If valid, send it to the Kafka topic and wait before the next one.

    Arguments:
        producer: An open Kafka producer.
        settings: Kafka settings including the topic name.
        valid_user_ids: Set of known user IDs for validation.

    Returns:
        A tuple of (sent_count, rejected_count).
    """
    LOG.info("Sending messages...")
    LOG.info(f"Sending up to {MESSAGE_COUNT} message(s) to topic {settings.topic!r}.")
    LOG.info("Watch each session arrive. Press CTRL+C to stop early.\n")

    sent_count = 0
    rejected_count = 0

    try:
        for message in generate_messages(MESSAGE_COUNT):
            LOG.info(format_message_for_log(message))

            result = validate_workout_record(
                record=message,
                valid_user_ids=valid_user_ids,
            )

            if not result.is_valid:
                rejected_count += 1
                LOG.warning("MESSAGE REJECTED")
                LOG.warning(f"  errors={result.errors}")
                write_rejected_record(message, result.errors)
                continue

            key = get_message_key(message)
            LOG.info(f"  Sending message with key={key}")

            produce_kafka_message(
                producer=producer,
                topic=settings.topic,
                key=key,
                message=message,
            )

            sent_count += 1
            LOG.info(f"  MESSAGE SENT  sent={sent_count}")
            time.sleep(MESSAGE_INTERVAL_SECONDS)

    except (FileNotFoundError, KeyError, RuntimeError, ValueError) as error:
        LOG.error(str(error))
        LOG.error("Producer stopped before completing all messages.")
        raise SystemExit(1) from error

    return sent_count, rejected_count


def log_rejected(rejected_count: int) -> None:
    """Log the rejected records CSV path if any records were rejected."""
    LOG.info("Checking for rejected records...")
    if rejected_count > 0:
        log_path(LOG, "  WROTE REJECTED_WORKOUTS_CSV", REJECTED_WORKOUTS_CSV)
    else:
        LOG.info("  No records rejected.")


# ===========================================================================
# DEFINE SECTION E. EXIT AND CLEANUP HELPERS
# ===========================================================================


def log_summary(sent_count: int, rejected_count: int, settings: KafkaSettings) -> None:
    """Log final summary statistics."""
    LOG.info("Summary:")
    LOG.info(f"Sent {sent_count} message(s) to topic {settings.topic!r}.")
    LOG.info(f"Rejected {rejected_count} message(s).")
    LOG.info("========================")
    LOG.info("Producer executed successfully!")
    LOG.info("========================")


# ===========================================================================
# MAIN FUNCTION
# ===========================================================================


def main() -> None:
    """Main entry point for the Kafka workouts producer."""
    log_paths()

    LOG.info("========================")
    LOG.info("SECTION A. Acquire")
    LOG.info("========================")

    settings = load_settings()
    verify_connection(settings)
    prepare_producer_topic(settings)
    valid_user_ids = load_reference_data()
    producer = create_producer(settings)

    LOG.info("========================")
    LOG.info("SECTION P. Produce Messages")
    LOG.info("========================")

    initialize_output()
    sent_count, rejected_count = send_messages(producer, settings, valid_user_ids)
    log_rejected(rejected_count)

    LOG.info("========================")
    LOG.info("SECTION E. Exit")
    LOG.info("========================")

    producer.flush()
    log_summary(sent_count, rejected_count, settings)


# === CONDITIONAL EXECUTION GUARD ===

if __name__ == "__main__":
    main()
