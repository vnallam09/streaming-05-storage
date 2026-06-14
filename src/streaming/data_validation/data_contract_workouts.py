"""src/streaming/data_validation/data_contract_workouts.py.

Data contract for the Phase 5 wearable-workouts scenario.

This is the workouts equivalent of data_contract_case.py. It defines what a
valid workout-session message looks like: required fields, allowed values,
reference table fields, and output field order.

Use the data/workouts.csv and data/workout_users.csv files as the source of
truth for this data contract.

The reusable validation helpers live in datafun_streaming.
The domain-specific field rules and validate_workout_record live here.
"""

# === DECLARE IMPORTS ===

from typing import Any, Final

from datafun_streaming.core.types import DataRecordDict
from datafun_streaming.data_validation.types import ValidationResult
from datafun_streaming.data_validation.validation_utils import (
    validate_datetime,
    validate_positive_integer,
    validate_required_fields,
)

# === EVENT TABLE FIELDS ===

WORKOUT_REQUIRED_FIELDS: Final[list[str]] = [
    "session_id",
    "datetime",
    "user_id",
    "activity",
    "duration_min",
    "distance_km",
    "avg_hr",
]

WORKOUT_OPTIONAL_FIELDS: Final[list[str]] = [
    "device_type",
    "note",
]

VALID_WORKOUT_FIELDNAMES: Final[list[str]] = [
    *WORKOUT_REQUIRED_FIELDS,
    *WORKOUT_OPTIONAL_FIELDS,
]

# === REFERENCE TABLE FIELDS ===

USERS_REQUIRED_FIELDS: Final[list[str]] = [
    "user_id",
    "name",
    "age",
    "weight_kg",
    "resting_hr",
    "max_hr",
]

# === ALLOWED VALUES ===

ALLOWED_ACTIVITIES: Final[set[str]] = {"run", "cycle", "swim", "walk", "row"}
ALLOWED_DEVICE_TYPES: Final[set[str]] = {"watch", "phone", "band"}

# === DERIVED FIELDS ADDED BY THE CONSUMER ===

# These are computed in data_engineering/workout_fields.py and stored in DuckDB.
WORKOUT_DERIVED_FIELDS: Final[list[str]] = [
    "calories",
    "pace_min_per_km",
    "intensity_pct",
    "hr_zone",
]

# === OUTPUT FIELD ORDER ===

CONSUMED_WORKOUT_FIELDNAMES: Final[list[str]] = [
    *WORKOUT_REQUIRED_FIELDS,
    *WORKOUT_DERIVED_FIELDS,
    "_kafka_key",
    "_kafka_partition",
    "_kafka_offset",
]

REJECTED_WORKOUT_FIELDNAMES: Final[list[str]] = [
    *WORKOUT_REQUIRED_FIELDS,
    "validation_errors",
]


# === DOMAIN-SPECIFIC VALIDATION ===


def validate_workout_record(
    *,
    record: DataRecordDict,
    valid_user_ids: set[str],
) -> ValidationResult:
    """Validate one workout-session record against the data contract.

    All arguments after the asterisk must be passed as keyword arguments.

    Arguments:
        record: The message to validate.
        valid_user_ids: The set of valid user_id values from the users table.

    Returns:
        A ValidationResult indicating whether the record is valid and any errors.
    """
    errors: list[str] = []

    # Required fields must all be present and non-empty.
    errors.extend(
        validate_required_fields(record=record, required_fields=WORKOUT_REQUIRED_FIELDS)
    )

    # Stop early if required fields are missing; later checks assume they exist.
    if errors:
        return ValidationResult(is_valid=False, errors=errors)

    # user_id must exist in the reference table.
    if record["user_id"] not in valid_user_ids:
        errors.append(f"Unknown user_id: {record['user_id']!r}")

    # activity must be one of the allowed values.
    if record["activity"] not in ALLOWED_ACTIVITIES:
        errors.append(f"Invalid activity: {record['activity']!r}")

    # Optional device_type, when present, must be allowed.
    device_type = record.get("device_type", "")
    if device_type and device_type not in ALLOWED_DEVICE_TYPES:
        errors.append(f"Invalid device_type: {device_type!r}")

    # datetime must parse as an ISO timestamp.
    errors.extend(validate_datetime(record["datetime"]))

    # duration_min and avg_hr must be positive integers (whole minutes / bpm).
    errors.extend(validate_positive_integer(record["duration_min"]))
    errors.extend(validate_positive_integer(record["avg_hr"]))

    # distance_km must be a non-negative number (0 is allowed, e.g. some rows).
    errors.extend(_validate_non_negative_number(record["distance_km"], "distance_km"))

    return ValidationResult(is_valid=not errors, errors=errors)


def _validate_non_negative_number(value: str, field_name: str) -> list[str]:
    """Return errors if value is not a number >= 0."""
    try:
        number = float(value)
    except TypeError, ValueError:
        return [f"{field_name} must be a number: {value!r}"]
    if number < 0:
        return [f"{field_name} must be >= 0: {value!r}"]
    return []


# === OUTPUT HELPERS ===


def keep_workout_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Return only required workout fields in standard order.

    Arguments:
        row: The original message as a dict.

    Returns:
        A new dict with only the required fields in the standard order.
    """
    return {field: row.get(field, "") for field in WORKOUT_REQUIRED_FIELDS}
