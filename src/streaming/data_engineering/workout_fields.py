"""src/streaming/data_engineering/workout_fields.py.

Derived field calculations for the Phase 5 wearable-workouts scenario.

This is the workouts equivalent of derived_fields.py. The producer sends raw
session measurements only (duration, distance, average heart rate). The
consumer is responsible for all derived calculations, using the per-user
reference data (workout_users.csv) for body weight and max heart rate.

We add these derived fields:
  - calories         estimated energy burned (MET formula)
  - pace_min_per_km  minutes per kilometer (0 when distance is 0)
  - intensity_pct    average HR as a percent of the user's max HR
  - hr_zone          training zone 1-5 derived from intensity_pct
"""

# === DECLARE IMPORTS ===

import logging
from typing import Any, Final

# === DECLARE EXPORTS ===

__all__ = [
    "ACTIVITY_MET",
    "MET_DEFAULT",
    "compute_calories",
    "compute_hr_zone",
    "compute_intensity_pct",
    "compute_pace_min_per_km",
    "enrich_workout",
]

# === DECLARE CONSTANTS ===

# Metabolic Equivalent of Task (MET) values per activity.
# Used to estimate energy expenditure: kcal = MET * weight_kg * hours.
ACTIVITY_MET: Final[dict[str, float]] = {
    "run": 9.8,
    "cycle": 7.5,
    "swim": 8.0,
    "walk": 3.5,
    "row": 7.0,
}

# Fallback MET used when an activity is not in the table.
MET_DEFAULT: Final[float] = 6.0

# Fallback max HR used when a user is not found in the lookup table.
MAX_HR_DEFAULT: Final[float] = 190.0

# === CONFIGURE LOGGER ONCE PER PYTHON FILE (MODULE) ===

LOG = logging.getLogger(__name__)

# === DEFINE DERIVED FIELD FUNCTIONS ===


def compute_calories(activity: str, weight_kg: float, duration_min: float) -> float:
    """Estimate calories burned using the MET formula.

    kcal = MET * weight_kg * (duration_min / 60)

    Arguments:
        activity: The workout activity (e.g. "run").
        weight_kg: The athlete's body weight in kilograms.
        duration_min: Session duration in minutes.

    Returns:
        Estimated calories rounded to 1 decimal place.
    """
    met = ACTIVITY_MET.get(activity, MET_DEFAULT)
    return round(met * weight_kg * (duration_min / 60.0), 1)


def compute_pace_min_per_km(duration_min: float, distance_km: float) -> float:
    """Compute pace in minutes per kilometer.

    Arguments:
        duration_min: Session duration in minutes.
        distance_km: Distance covered in kilometers.

    Returns:
        Pace rounded to 2 decimals, or 0.0 when distance is 0.
    """
    if distance_km <= 0:
        return 0.0
    return round(duration_min / distance_km, 2)


def compute_intensity_pct(avg_hr: float, max_hr: float) -> float:
    """Compute average HR as a percent of the user's max HR.

    Arguments:
        avg_hr: Average heart rate for the session (bpm).
        max_hr: The user's maximum heart rate (bpm).

    Returns:
        Intensity percent rounded to 1 decimal place.
    """
    if max_hr <= 0:
        return 0.0
    return round((avg_hr / max_hr) * 100.0, 1)


def compute_hr_zone(intensity_pct: float) -> int:
    """Map an intensity percent to a 1-5 training zone.

    Zones follow common HR-zone breakpoints:
      Zone 1 < 60%, Zone 2 60-69%, Zone 3 70-79%,
      Zone 4 80-89%, Zone 5 >= 90%.

    Arguments:
        intensity_pct: Average HR as a percent of max HR.

    Returns:
        The training zone as an integer from 1 to 5.
    """
    if intensity_pct >= 90:
        return 5
    if intensity_pct >= 80:
        return 4
    if intensity_pct >= 70:
        return 3
    if intensity_pct >= 60:
        return 2
    return 1


def enrich_workout(
    row: dict[str, Any],
    user_lookup: dict[str, dict[str, float]],
) -> dict[str, Any]:
    """Add all derived fields to a raw workout row.

    Looks up the user's weight and max HR from the reference table,
    then computes calories, pace, intensity, and HR zone.

    Arguments:
        row: A validated raw message row.
        user_lookup: A dict mapping user_id to {"weight_kg", "max_hr"}.

    Returns:
        A new dict containing all original fields plus derived fields.
    """
    activity = str(row.get("activity", ""))
    duration_min = float(row.get("duration_min", 0))
    distance_km = float(row.get("distance_km", 0))
    avg_hr = float(row.get("avg_hr", 0))
    user_id = str(row.get("user_id", ""))

    weight_kg, max_hr = get_user_profile(user_id, user_lookup)

    calories = compute_calories(activity, weight_kg, duration_min)
    pace = compute_pace_min_per_km(duration_min, distance_km)
    intensity_pct = compute_intensity_pct(avg_hr, max_hr)
    hr_zone = compute_hr_zone(intensity_pct)

    return {
        **row,
        "calories": calories,
        "pace_min_per_km": pace,
        "intensity_pct": intensity_pct,
        "hr_zone": hr_zone,
    }


def get_user_profile(
    user_id: str, user_lookup: dict[str, dict[str, float]]
) -> tuple[float, float]:
    """Look up a user's weight and max HR.

    Arguments:
        user_id: The user identifier from the message (e.g. "USR-001").
        user_lookup: A dict mapping user_id to {"weight_kg", "max_hr"}.

    Returns:
        A tuple of (weight_kg, max_hr). Falls back to defaults if unknown.
    """
    if user_id in user_lookup:
        profile = user_lookup[user_id]
        return profile["weight_kg"], profile["max_hr"]

    LOG.warning(
        f"User {user_id!r} not in lookup table. "
        f"Using default max HR {MAX_HR_DEFAULT} and weight 70.0."
    )
    return 70.0, MAX_HR_DEFAULT
