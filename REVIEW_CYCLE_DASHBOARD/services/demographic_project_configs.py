"""Project-specific configuration for Demographic Review flags.

The default configuration mirrors HRTVA_AUTOMATION/od_demographics_checks.py.
Add project-specific overrides in PROJECT_DEMOGRAPHIC_CONFIGS instead of
changing the evaluator logic.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

CATEGORY_TRANSPORT = "transport_logistic"
CATEGORY_DEMOGRAPHIC = "demographic"


def _expr(**kwargs: Any) -> dict[str, Any]:
    return kwargs


CURRENT_PROJECT_STUDENT_FARE_TYPES = [
    "U-Pass or CMSD ID",
    "Go Smester",
    "Student Freedom Pass",
    "1-Trip, 2-Trip, or 5-Trip Fare Card",
    "All-Day Pass",
    "1-Ride Ticket or cash fare",
]

SCRIPT_FLAG_COLUMNS = [
    "student_od_flag",
    "workplace_od_flag",
    "OLD_K12_STUDENT",
    "YOUNG_OLD_COLLEGE_STUDENT",
    "EMPLOYED_IN_HH_1",
    "EMPLOYED_IN_HH_2",
    "EMPLOYED_IN_HH_GREATER_THAN_HH_SIZE",
    "TRAVEL_WITH_HH_GREATER_THAN_HH_SIZE",
    "COUNT_VH_HH_FLAG",
    "YOUNG_DRIVER",
    "FARE_STUDENT_FLAG",
    "FARE_EMPLOYMENT_FLAG",
]

SCRIPT_OUTPUT_COLUMN_GROUPS = [
    ["id"],
    ["employedinhhcode", "hhsizecode"],
    ["originplacetypecode", "destinplacetypecode", "studentstatuscode"],
    ["employedinhh", "hhsize", "destinplacetype", "originplacetype", "studentstatus", "employmentstatuscode", "employmentstatus"],
    ["havedl"],
    ["yourage", "yearborn"],
    ["typefare", "typeoffare"],
]

DEFAULT_FIELD_ALIASES: dict[str, list[str]] = {
    "id": ["id", "elvis_id"],
    "age": ["yourage", "YourAge"],
    "year_born": ["yearborn", "YearBorn"],
    "origin_place_type": ["originplacetypecode", "ORIGIN_PLACE_TYPECode", "OriginPlaceTypeCode"],
    "destin_place_type": ["destinplacetypecode", "DESTIN_PLACE_TYPECode", "DestinPlaceTypeCode"],
    "student_status": ["studentstatuscode", "STUDENT_STATUSCode", "StudentStatusCode"],
    "employment_status": ["employmentstatuscode", "EMPLOYMENT_STATUSCode", "EmploymentStatusCode"],
    "employed_in_hh": ["employedinhhcode", "EMPLOYED_IN_HHCode", "EmployedInHHCode"],
    "hh_size": ["hhsizecode", "HH_SIZECode", "HHSizeCode"],
    "travel_hh": ["travelhh", "TRAVEL_HH", "TravelHH"],
    "count_vehicle_hh": ["countvhhhcode", "COUNT_VH_HHCode", "CountVhHHCode"],
    "used_vehicle_trip": ["usedvehtripcode", "USED_VEH_TRIPCode", "UsedVehTripCode"],
    "driver_license": ["havedl", "HaveDL"],
    "fare_type": ["typefare", "typeoffare", "TypeFare", "TypeofFare"],
    "Final_Usage": ["Final_Usage", "FINAL_USAGE"],
}

DEFAULT_FLAG_DEFINITIONS: list[dict[str, Any]] = [
    {
        "FLAG_KEY": "student_od_flag",
        "LABEL": "student_od_flag",
        "CATEGORY": CATEGORY_TRANSPORT,
        "DESCRIPTION": "School or college origin/destination while respondent is marked not a student.",
        "SEVERITY": "medium",
        "DISPLAY_ORDER": 10,
        "MESSAGE_TEMPLATE": "student_od_flag",
        "DEFAULT_PARAMS": {
            "expression": _expr(
                all=[
                    _expr(
                        any=[
                            _expr(field="destin_place_type", op="in", value=[5, 6]),
                            _expr(field="origin_place_type", op="in", value=[5, 6]),
                        ]
                    ),
                    _expr(field="student_status", op="eq", value=1),
                ]
            )
        },
    },
    {
        "FLAG_KEY": "workplace_od_flag",
        "LABEL": "workplace_od_flag",
        "CATEGORY": CATEGORY_TRANSPORT,
        "DESCRIPTION": "Usual workplace origin/destination while respondent employment status is greater than 2.",
        "SEVERITY": "medium",
        "DISPLAY_ORDER": 20,
        "MESSAGE_TEMPLATE": "workplace_od_flag",
        "DEFAULT_PARAMS": {
            "expression": _expr(
                all=[
                    _expr(
                        any=[
                            _expr(field="origin_place_type", op="eq", value=1),
                            _expr(field="destin_place_type", op="eq", value=1),
                        ]
                    ),
                    _expr(field="employment_status", op="gt", value=2),
                ]
            )
        },
    },
    {
        "FLAG_KEY": "OLD_K12_STUDENT",
        "LABEL": "OLD_K12_STUDENT",
        "CATEGORY": CATEGORY_DEMOGRAPHIC,
        "DESCRIPTION": "K-12 trip or student status with respondent age at least 18.",
        "SEVERITY": "medium",
        "DISPLAY_ORDER": 30,
        "MESSAGE_TEMPLATE": "OLD_K12_STUDENT",
        "DEFAULT_PARAMS": {
            "expression": _expr(
                all=[
                    _expr(
                        any=[
                            _expr(field="destin_place_type", op="eq", value=6),
                            _expr(field="origin_place_type", op="eq", value=6),
                            _expr(field="student_status", op="eq", value=5),
                        ]
                    ),
                    _expr(field="age", op="gte", value=18),
                    _expr(field="age", op="neq", value=0),
                ]
            )
        },
    },
    {
        "FLAG_KEY": "YOUNG_OLD_COLLEGE_STUDENT",
        "LABEL": "YOUNG_OLD_COLLEGE_STUDENT",
        "CATEGORY": CATEGORY_DEMOGRAPHIC,
        "DESCRIPTION": "College trip or student status with respondent age <= 16 or >= 65.",
        "SEVERITY": "medium",
        "DISPLAY_ORDER": 40,
        "MESSAGE_TEMPLATE": "YOUNG_OLD_COLLEGE_STUDENT",
        "DEFAULT_PARAMS": {
            "expression": _expr(
                all=[
                    _expr(
                        any=[
                            _expr(field="destin_place_type", op="eq", value=5),
                            _expr(field="origin_place_type", op="eq", value=5),
                            _expr(field="student_status", op="in", value=[2, 3]),
                        ]
                    ),
                    _expr(any=[_expr(field="age", op="lte", value=16), _expr(field="age", op="gte", value=65)]),
                    _expr(field="age", op="neq", value=0),
                ]
            )
        },
    },
    {
        "FLAG_KEY": "EMPLOYED_IN_HH_1",
        "LABEL": "EMPLOYED_IN_HH_1",
        "CATEGORY": CATEGORY_DEMOGRAPHIC,
        "DESCRIPTION": "Employed-in-household count is zero while respondent is employed full or part time.",
        "SEVERITY": "medium",
        "DISPLAY_ORDER": 50,
        "MESSAGE_TEMPLATE": "EMPLOYED_IN_HH_1",
        "DEFAULT_PARAMS": {
            "expression": _expr(
                all=[
                    _expr(field="employed_in_hh", op="eq", value=0),
                    _expr(field="employment_status", op="in", value=[1, 2]),
                ]
            )
        },
    },
    {
        "FLAG_KEY": "EMPLOYED_IN_HH_2",
        "LABEL": "EMPLOYED_IN_HH_2",
        "CATEGORY": CATEGORY_DEMOGRAPHIC,
        "DESCRIPTION": "One-person household has one employed member while respondent is not employed.",
        "SEVERITY": "medium",
        "DISPLAY_ORDER": 60,
        "MESSAGE_TEMPLATE": "EMPLOYED_IN_HH_2",
        "DEFAULT_PARAMS": {
            "expression": _expr(
                all=[
                    _expr(field="employed_in_hh", op="eq", value=1),
                    _expr(field="hh_size", op="eq", value=1),
                    _expr(not_=_expr(field="employment_status", op="in", value=[1, 2])),
                ]
            )
        },
    },
    {
        "FLAG_KEY": "EMPLOYED_IN_HH_GREATER_THAN_HH_SIZE",
        "LABEL": "EMPLOYED_IN_HH_GREATER_THAN_HH_SIZE",
        "CATEGORY": CATEGORY_DEMOGRAPHIC,
        "DESCRIPTION": "Employed-in-household count is greater than household size.",
        "SEVERITY": "high",
        "DISPLAY_ORDER": 70,
        "MESSAGE_TEMPLATE": "EMPLOYED_IN_HH_GREATER_THAN_HH_SIZE",
        "DEFAULT_PARAMS": {"expression": _expr(field="employed_in_hh", op="gt_field", compare_field="hh_size")},
    },
    {
        "FLAG_KEY": "TRAVEL_WITH_HH_GREATER_THAN_HH_SIZE",
        "LABEL": "TRAVEL_WITH_HH_GREATER_THAN_HH_SIZE",
        "CATEGORY": CATEGORY_DEMOGRAPHIC,
        "DESCRIPTION": "Reported household travel party is greater than household size.",
        "SEVERITY": "high",
        "DISPLAY_ORDER": 80,
        "MESSAGE_TEMPLATE": "TRAVEL_WITH_HH_GREATER_THAN_HH_SIZE",
        "DEFAULT_PARAMS": {"expression": _expr(field="travel_hh", op="gt_field", compare_field="hh_size")},
    },
    {
        "FLAG_KEY": "COUNT_VH_HH_FLAG",
        "LABEL": "COUNT_VH_HH_FLAG",
        "CATEGORY": CATEGORY_TRANSPORT,
        "DESCRIPTION": "Household vehicle count is zero and used vehicle trip is not null.",
        "SEVERITY": "medium",
        "DISPLAY_ORDER": 90,
        "MESSAGE_TEMPLATE": "COUNT_VH_HH_FLAG",
        "DEFAULT_PARAMS": {
            "expression": _expr(
                all=[
                    _expr(field="count_vehicle_hh", op="raw_eq", value=0),
                    _expr(field="used_vehicle_trip", op="not_null"),
                ]
            )
        },
    },
    {
        "FLAG_KEY": "YOUNG_DRIVER",
        "LABEL": "YOUNG_DRIVER",
        "CATEGORY": CATEGORY_DEMOGRAPHIC,
        "DESCRIPTION": "Respondent has a driver license value of yes and is age 16 or younger.",
        "SEVERITY": "high",
        "DISPLAY_ORDER": 100,
        "MESSAGE_TEMPLATE": "YOUNG_DRIVER",
        "DEFAULT_PARAMS": {
            "expression": _expr(
                all=[
                    _expr(field="driver_license", op="raw_str_lower_eq", value="yes"),
                    _expr(field="age", op="lte", value=16),
                ]
            )
        },
    },
    {
        "FLAG_KEY": "FARE_STUDENT_FLAG",
        "LABEL": "FARE_STUDENT_FLAG",
        "CATEGORY": CATEGORY_TRANSPORT,
        "DESCRIPTION": "Configured student fare product while respondent student status equals numeric 1.",
        "SEVERITY": "medium",
        "DISPLAY_ORDER": 110,
        "MESSAGE_TEMPLATE": "FARE_STUDENT_FLAG",
        "DEFAULT_PARAMS": {
            "expression": _expr(
                all=[
                    _expr(field="fare_type", op="raw_in", value=CURRENT_PROJECT_STUDENT_FARE_TYPES),
                    _expr(field="student_status", op="raw_eq", value=1),
                ]
            )
        },
    },
    {
        "FLAG_KEY": "FARE_EMPLOYMENT_FLAG",
        "LABEL": "FARE_EMPLOYMENT_FLAG",
        "CATEGORY": CATEGORY_TRANSPORT,
        "DESCRIPTION": "Employee fare keyword while respondent employment status is greater than 2.",
        "SEVERITY": "medium",
        "DISPLAY_ORDER": 120,
        "MESSAGE_TEMPLATE": "FARE_EMPLOYMENT_FLAG",
        "DEFAULT_PARAMS": {
            "expression": _expr(
                all=[
                    _expr(field="fare_type", op="contains_any", value=["employee", "metroworks"]),
                    _expr(field="employment_status", op="gt", value=2),
                ]
            )
        },
    },
]

DEFAULT_DEMOGRAPHIC_PROJECT_CONFIG: dict[str, Any] = {
    "field_aliases": DEFAULT_FIELD_ALIASES,
    "flag_definitions": DEFAULT_FLAG_DEFINITIONS,
    "script_flag_columns": SCRIPT_FLAG_COLUMNS,
    "script_output_column_groups": SCRIPT_OUTPUT_COLUMN_GROUPS,
    "age_strategy": "script_column_level_fallback",
}

PROJECT_DEMOGRAPHIC_CONFIGS: dict[str, dict[str, Any]] = {
    # The default/current project mirrors HRTVA_AUTOMATION/od_demographics_checks.py.
    "default": DEFAULT_DEMOGRAPHIC_PROJECT_CONFIG,
}


def _normalized_project_name(project_name: str | None) -> str:
    return "".join(ch for ch in str(project_name or "").lower() if ch.isalnum())


def _merge_config(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if key == "field_aliases":
            aliases = merged.setdefault("field_aliases", {})
            aliases.update(deepcopy(value))
        elif key == "flag_definitions":
            merged["flag_definitions"] = deepcopy(value)
        else:
            merged[key] = deepcopy(value)
    return merged


def get_demographic_project_config(project_name: str | None) -> dict[str, Any]:
    config = deepcopy(DEFAULT_DEMOGRAPHIC_PROJECT_CONFIG)
    project_key = _normalized_project_name(project_name)
    for configured_name, override in PROJECT_DEMOGRAPHIC_CONFIGS.items():
        if configured_name == "default":
            continue
        if _normalized_project_name(configured_name) == project_key:
            config = _merge_config(config, override)
            break
    return config
