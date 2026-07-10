"""Validation helpers for the Review Cycle dashboard."""



from __future__ import annotations



from pathlib import Path



import pandas as pd



from core.config import ROOT_DIR, SCRIPTS_DIR





REQUIRED_SCRIPTS = [

    "improved_auto_approval.py",

    "od_distance_checks_auto_approval.py",

    "traditional_transfer_checks_auto_approval.py",

    "transfer_distance_flags_auto_approval.py",

    "directional_stops_flags_auto_approval.py",

    "combining_distance_flags_auto_approval.py",

    "sort_improved_auto_approval_output.py",

    "od_distance_checks.py",

    "traditional_transfer_checks.py",

    "transfer_distance_flags.py",

    "directional_stops_flags.py",

    "combining_distance_flags.py",

    "reviewer_stats_kcata.py",

    "Removed_ids_field_team.py",

    "od_demographics_checks.py",

    "constants.py",

    "database.py",

]





def validate_project_structure() -> dict:

    missing = [name for name in REQUIRED_SCRIPTS if not (SCRIPTS_DIR / name).exists()]

    core_files = [

        ROOT_DIR / "app.py",

        ROOT_DIR / "core" / "data_access.py",

        ROOT_DIR / "core" / "snowflake_conn.py",

        ROOT_DIR / "core" / "schema.py",

        ROOT_DIR / "pipeline" / "runner.py",

        ROOT_DIR / "pipeline" / "ingest.py",

        ROOT_DIR / "services" / "history.py",

        ROOT_DIR / "services" / "assignments.py",

        ROOT_DIR / "services" / "sync.py",

        ROOT_DIR / "services" / "analytics.py",

        ROOT_DIR / "services" / "quality.py",

        ROOT_DIR / "services" / "notifications.py",

        ROOT_DIR / "views" / "cleaning.py",

        ROOT_DIR / "views" / "review.py",

        ROOT_DIR / "views" / "combined_checks_fields.py",

        ROOT_DIR / "views" / "reviewer_stats.py",

        ROOT_DIR / "views" / "history.py",

        ROOT_DIR / "views" / "project_dashboard.py",

        ROOT_DIR / "views" / "demographic.py",

        ROOT_DIR / "sql" / "002_notifications_quality.sql",

    ]

    missing_core = [str(path.relative_to(ROOT_DIR)) for path in core_files if not path.exists()]

    return {

        "scripts_ok": not missing,

        "missing_scripts": missing,

        "core_ok": not missing_core,

        "missing_core": missing_core,

    }





def validate_dataframe_roundtrip(records_df: pd.DataFrame, checks_df: pd.DataFrame) -> dict:

    return {

        "records_count": len(records_df),

        "checks_count": len(checks_df),

        "records_has_id": any(col in records_df.columns for col in ("id", "elvis_id")),

        "checks_has_flags": "SUM_ALL_CHECKS" in checks_df.columns or "2X_REVIEW_CHECK" in checks_df.columns,

    }





def validate_enrich_payload() -> dict:

    """Typed Snowflake columns should override stale payload on read."""

    from core.data_access import enrich_payload_from_typed_columns, normalize_payload_aliases

    payload = {"Final_Usage": "Use", "ROUTE_SURVEYEDCode": ""}
    row = {"RECORD_ID": "123", "FINAL_USAGE": "Remove", "ROUTE_SURVEYED_CODE": "R42"}
    enriched = enrich_payload_from_typed_columns(row, payload)
    ok_typed = (
        enriched.get("Final_Usage") == "Remove"
        and enriched.get("ROUTE_SURVEYEDCode") == "R42"
        and enriched.get("id") == "123"
    )
    tampa_payload = normalize_payload_aliases({"RouteSurveyedCode": "TAM_1_5_01", "RouteSurveyed": "Route 5"})
    ok_alias = (
        tampa_payload.get("ROUTE_SURVEYEDCode") == "TAM_1_5_01"
        and tampa_payload.get("ROUTE_SURVEYED") == "Route 5"
    )
    return {"enrich_ok": ok_typed and ok_alias, "Final_Usage": enriched.get("Final_Usage")}





def validate_offline_services() -> dict:

    """Smoke-test service logic that does not require Snowflake."""

    from services import analytics

    from views.record_card import _comparison_frame



    compare = _comparison_frame({"A": 1, "B": 2}, {"A": 1, "B": 3})

    changed = int(compare["Changed"].sum())



    routes = pd.DataFrame(

        {

            "RECORD_ID": ["1", "2", "3", "4"],

            "FINAL_USAGE": ["Remove", "Use", "Remove", "Use"],

            "ROUTE_SURVEYED_CODE": ["R1", "R1", "R2", "R2"],

        }

    )

    import core.data_access as da

    import services.analytics as analytics_mod



    original = da.load_records

    original_analytics = analytics_mod.load_records

    try:

        da.load_records = lambda project_name=None, **kw: routes

        analytics_mod.load_records = lambda project_name=None, **kw: routes

        removal = analytics.removal_by("TEST", "ROUTE_SURVEYED_CODE")

        score = analytics.project_quality_score("TEST")

    finally:

        da.load_records = original

        analytics_mod.load_records = original_analytics



    enrich = validate_enrich_payload()

    return {

        "comparison_changed_fields": changed,

        "removal_rows": len(removal),

        "quality_score": score.get("quality_score"),

        "enrich_ok": enrich["enrich_ok"],

        "offline_ok": changed == 1 and len(removal) == 2 and score.get("total") == 4 and enrich["enrich_ok"],

    }





if __name__ == "__main__":

    result = validate_project_structure()

    offline = validate_offline_services()

    print(result)

    print(offline)

