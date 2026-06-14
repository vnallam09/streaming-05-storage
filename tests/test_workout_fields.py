"""Tests for workout derived-field calculations (Phase 5)."""

from streaming.data_engineering.workout_fields import (
    ACTIVITY_MET,
    MAX_HR_DEFAULT,
    compute_calories,
    compute_hr_zone,
    compute_intensity_pct,
    compute_pace_min_per_km,
    enrich_workout,
    get_user_profile,
)


def test_compute_calories_uses_met_formula() -> None:
    """Calories should be MET * weight_kg * hours."""
    result = compute_calories("run", 70.0, 60)
    assert result == round(ACTIVITY_MET["run"] * 70.0 * 1.0, 1)


def test_compute_pace_handles_zero_distance() -> None:
    """Pace should be 0 when distance is 0 to avoid division by zero."""
    assert compute_pace_min_per_km(40, 0) == 0.0


def test_compute_pace_min_per_km() -> None:
    """Pace should be duration divided by distance."""
    assert compute_pace_min_per_km(40, 8.0) == 5.0


def test_compute_intensity_pct() -> None:
    """Intensity should be avg_hr as a percent of max_hr."""
    assert compute_intensity_pct(150, 200) == 75.0


def test_compute_intensity_pct_handles_zero_max_hr() -> None:
    """Intensity should be 0 when max_hr is 0."""
    assert compute_intensity_pct(150, 0) == 0.0


def test_compute_hr_zone_breakpoints() -> None:
    """HR zone should follow the 60/70/80/90 percent breakpoints."""
    assert compute_hr_zone(55) == 1
    assert compute_hr_zone(65) == 2
    assert compute_hr_zone(75) == 3
    assert compute_hr_zone(85) == 4
    assert compute_hr_zone(95) == 5


def test_get_user_profile_uses_default_for_unknown_user() -> None:
    """Unknown users should fall back to the default max HR."""
    weight, max_hr = get_user_profile("MISSING", {})
    assert max_hr == MAX_HR_DEFAULT
    assert weight == 70.0


def test_enrich_workout_adds_all_derived_fields() -> None:
    """Enriched messages should include all derived workout fields."""
    row = {
        "user_id": "USR-001",
        "activity": "run",
        "duration_min": "60",
        "distance_km": "10",
        "avg_hr": "171",
    }
    user_lookup = {"USR-001": {"weight_kg": 72.0, "max_hr": 190.0}}

    enriched = enrich_workout(row, user_lookup)

    assert enriched["calories"] == round(ACTIVITY_MET["run"] * 72.0 * 1.0, 1)
    assert enriched["pace_min_per_km"] == 6.0
    assert enriched["intensity_pct"] == round(171 / 190 * 100, 1)
    assert enriched["hr_zone"] == 5  # 171/190 = 90.0% -> zone 5


def test_enrich_workout_keeps_original_fields() -> None:
    """Enriched messages should preserve original message fields."""
    row = {
        "session_id": "SES-0001",
        "user_id": "USR-001",
        "activity": "cycle",
        "duration_min": "30",
        "distance_km": "12",
        "avg_hr": "140",
    }
    user_lookup = {"USR-001": {"weight_kg": 72.0, "max_hr": 190.0}}

    enriched = enrich_workout(row, user_lookup)

    assert enriched["session_id"] == "SES-0001"
    assert enriched["activity"] == "cycle"
