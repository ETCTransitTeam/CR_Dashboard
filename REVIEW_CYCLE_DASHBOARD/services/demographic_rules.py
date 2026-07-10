"""Configurable Demographic Review Flag definitions and evaluation.

Rules are stored as JSON expressions in Snowflake and evaluated by a small,
allow-listed condition engine. This keeps project-specific changes out of
Python deployments without allowing arbitrary executable code.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from core.config import REVIEW_CYCLE_SCHEMA
from core.data_access import build_elvis_review_export
from core.snowflake_conn import execute, fetch_df_optional, merge_upsert
from services.demographic_project_configs import (
    CATEGORY_DEMOGRAPHIC,
    CATEGORY_TRANSPORT,
    get_demographic_project_config,
)

DEFAULT_FIELD_ALIASES: dict[str, list[str]] = get_demographic_project_config(None)["field_aliases"]
DEFAULT_FLAG_DEFINITIONS: list[dict[str, Any]] = get_demographic_project_config(None)["flag_definitions"]


def _json_dumps(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "{}"
    if isinstance(value, str):
        return value
    return json.dumps(value, default=str)


def _json_loads(value: Any) -> Any:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return {}
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {}
    return {}


def _truthy(value: Any, default: bool = False) -> bool:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y", "t"}


def _norm(text: Any) -> str:
    return re.sub(r"[_\[\]\s#]+", "", str(text or "")).lower()


def _clean_scalar(value: Any) -> Any:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else None
    return value


def _to_number(value: Any) -> float | None:
    value = _clean_scalar(value)
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").replace("$", ""))
    except (TypeError, ValueError):
        return None


def _value_equal(left: Any, right: Any) -> bool:
    left_clean = _clean_scalar(left)
    right_clean = _clean_scalar(right)
    left_num = _to_number(left_clean)
    right_num = _to_number(right_clean)
    if left_num is not None and right_num is not None:
        return left_num == right_num
    return str(left_clean).strip().lower() == str(right_clean).strip().lower()


def _raw_value_equal(left: Any, right: Any) -> bool:
    try:
        result = left == right
    except (TypeError, ValueError):
        return False
    if result is pd.NA or (isinstance(result, float) and pd.isna(result)):
        return False
    try:
        return bool(result)
    except (TypeError, ValueError):
        return False


def _script_age_split(value: Any) -> Any:
    if isinstance(value, str):
        for item in value.split(" "):
            if item.isnumeric():
                return int(item)
    elif isinstance(value, (int, float)):
        return value
    return 0


def _script_birth_year_age(value: Any) -> float | int:
    birth_year = _to_number(value)
    if birth_year in (None, 0, 0.0):
        return 0
    return datetime.now().year - birth_year


def seed_default_flag_definitions() -> int:
    """Ensure built-in rule definitions exist without overwriting local edits."""
    rows: list[dict[str, Any]] = []
    for item in DEFAULT_FLAG_DEFINITIONS:
        row = item.copy()
        row.setdefault("RULE_KIND", "declarative")
        row.setdefault("DEFAULT_ENABLED", True)
        row.setdefault("VERSION", 1)
        row["DEFAULT_PARAMS"] = _json_dumps(row.get("DEFAULT_PARAMS"))
        row["UPDATED_AT"] = datetime.utcnow()
        rows.append(row)
    return merge_upsert(
        pd.DataFrame(rows),
        "DEMOGRAPHIC_FLAG_DEFINITIONS",
        key_columns=["FLAG_KEY"],
        insert_only=True,
        variant_columns=["DEFAULT_PARAMS"],
    )


def load_flag_definitions() -> pd.DataFrame:
    seed_default_flag_definitions()
    return fetch_df_optional(
        f"""
        SELECT FLAG_KEY, LABEL, CATEGORY, DESCRIPTION, SEVERITY, RULE_KIND,
               DEFAULT_ENABLED, DEFAULT_PARAMS, MESSAGE_TEMPLATE, DISPLAY_ORDER, VERSION, UPDATED_AT
        FROM {REVIEW_CYCLE_SCHEMA}.DEMOGRAPHIC_FLAG_DEFINITIONS
        ORDER BY DISPLAY_ORDER, FLAG_KEY
        """
    )


def _configured_default_definitions(project_name: str | None) -> pd.DataFrame:
    configured = get_demographic_project_config(project_name).get("flag_definitions") or []
    rows: list[dict[str, Any]] = []
    for item in configured:
        row = item.copy()
        row.setdefault("RULE_KIND", "declarative")
        row.setdefault("DEFAULT_ENABLED", True)
        row.setdefault("VERSION", 1)
        rows.append(row)
    return pd.DataFrame(rows)


def _apply_configured_defaults(defs: pd.DataFrame, project_name: str | None) -> pd.DataFrame:
    configured = _configured_default_definitions(project_name)
    if configured.empty:
        return defs
    if defs.empty:
        return configured

    merged = defs.copy()
    by_key = {str(row["FLAG_KEY"]): row.to_dict() for _, row in configured.iterrows()}
    configured_cols = [
        "LABEL",
        "CATEGORY",
        "DESCRIPTION",
        "SEVERITY",
        "RULE_KIND",
        "DEFAULT_ENABLED",
        "DEFAULT_PARAMS",
        "MESSAGE_TEMPLATE",
        "DISPLAY_ORDER",
        "VERSION",
    ]
    for idx, row in merged.iterrows():
        flag_key = str(row.get("FLAG_KEY") or "")
        configured_row = by_key.pop(flag_key, None)
        if not configured_row:
            continue
        for col in configured_cols:
            if col in configured_row:
                merged.at[idx, col] = configured_row[col]
    if by_key:
        merged = pd.concat([merged, pd.DataFrame(by_key.values())], ignore_index=True)
    return merged


def load_project_flag_matrix(project_name: str) -> pd.DataFrame:
    """Return default definitions merged with project-specific overrides."""
    defs = _apply_configured_defaults(load_flag_definitions(), project_name)
    if defs.empty:
        return defs
    config = fetch_df_optional(
        f"""
        SELECT PROJECT_NAME, FLAG_KEY, IS_ENABLED, LABEL_OVERRIDE, DESCRIPTION_OVERRIDE,
               SEVERITY_OVERRIDE, MESSAGE_TEMPLATE_OVERRIDE, PARAMS, FIELD_ALIASES,
               VERSION AS CONFIG_VERSION, UPDATED_BY, UPDATED_AT AS CONFIG_UPDATED_AT
        FROM {REVIEW_CYCLE_SCHEMA}.DEMOGRAPHIC_FLAG_PROJECT_CONFIG
        WHERE PROJECT_NAME = %s
        """,
        (project_name,),
    )
    merged = defs.merge(config, on="FLAG_KEY", how="left") if not config.empty else defs.copy()
    for col in (
        "IS_ENABLED",
        "LABEL_OVERRIDE",
        "DESCRIPTION_OVERRIDE",
        "SEVERITY_OVERRIDE",
        "MESSAGE_TEMPLATE_OVERRIDE",
        "PARAMS",
        "FIELD_ALIASES",
        "CONFIG_VERSION",
        "UPDATED_BY",
        "CONFIG_UPDATED_AT",
    ):
        if col not in merged.columns:
            merged[col] = None
    merged["EFFECTIVE_ENABLED"] = merged.apply(
        lambda row: _truthy(row.get("IS_ENABLED"), _truthy(row.get("DEFAULT_ENABLED"), True)),
        axis=1,
    )
    for out_col, default_col, override_col in [
        ("EFFECTIVE_LABEL", "LABEL", "LABEL_OVERRIDE"),
        ("EFFECTIVE_DESCRIPTION", "DESCRIPTION", "DESCRIPTION_OVERRIDE"),
        ("EFFECTIVE_SEVERITY", "SEVERITY", "SEVERITY_OVERRIDE"),
        ("EFFECTIVE_MESSAGE", "MESSAGE_TEMPLATE", "MESSAGE_TEMPLATE_OVERRIDE"),
    ]:
        merged[out_col] = merged.apply(
            lambda row: row.get(override_col)
            if _clean_scalar(row.get(override_col)) is not None
            else row.get(default_col),
            axis=1,
        )
    merged["EFFECTIVE_VERSION"] = merged.apply(
        lambda row: int(row.get("CONFIG_VERSION") or row.get("VERSION") or 1),
        axis=1,
    )
    return merged


def _audit(project_name: str | None, flag_key: str | None, action: str, old_config: Any, new_config: Any, actor: str) -> None:
    execute(
        f"""
        INSERT INTO {REVIEW_CYCLE_SCHEMA}.DEMOGRAPHIC_FLAG_AUDIT
        (PROJECT_NAME, FLAG_KEY, ACTION, OLD_CONFIG, NEW_CONFIG, ACTOR)
        SELECT %s, %s, %s, TRY_PARSE_JSON(%s), TRY_PARSE_JSON(%s), %s
        """,
        (project_name, flag_key, action, _json_dumps(old_config), _json_dumps(new_config), actor),
    )


def save_project_flag_config(project_name: str, config: pd.DataFrame, actor: str) -> int:
    """Persist project overrides from the Super Admin editor."""
    if config.empty:
        return 0
    existing = load_project_flag_matrix(project_name)
    existing_by_key = {
        str(row["FLAG_KEY"]): row.to_dict()
        for _, row in existing.iterrows()
        if _clean_scalar(row.get("FLAG_KEY")) is not None
    }
    rows: list[dict[str, Any]] = []
    now = datetime.utcnow()
    for _, row in config.iterrows():
        flag_key = str(row.get("FLAG_KEY") or "").strip()
        if not flag_key:
            continue
        old = existing_by_key.get(flag_key, {})
        old_version = int(old.get("EFFECTIVE_VERSION") or old.get("VERSION") or 1)
        new_row = {
            "PROJECT_NAME": project_name,
            "FLAG_KEY": flag_key,
            "IS_ENABLED": _truthy(row.get("EFFECTIVE_ENABLED"), True),
            "LABEL_OVERRIDE": _clean_scalar(row.get("EFFECTIVE_LABEL")),
            "DESCRIPTION_OVERRIDE": _clean_scalar(row.get("EFFECTIVE_DESCRIPTION")),
            "SEVERITY_OVERRIDE": _clean_scalar(row.get("EFFECTIVE_SEVERITY")) or "medium",
            "MESSAGE_TEMPLATE_OVERRIDE": _clean_scalar(row.get("EFFECTIVE_MESSAGE")),
            "PARAMS": _json_dumps(_json_loads(row.get("PARAMS")) or _json_loads(row.get("DEFAULT_PARAMS"))),
            "FIELD_ALIASES": _json_dumps(_json_loads(row.get("FIELD_ALIASES"))),
            "VERSION": old_version + 1,
            "UPDATED_BY": actor,
            "UPDATED_AT": now,
        }
        rows.append(new_row)
        _audit(project_name, flag_key, "project_config_saved", old, new_row, actor)
    if not rows:
        return 0
    return merge_upsert(
        pd.DataFrame(rows),
        "DEMOGRAPHIC_FLAG_PROJECT_CONFIG",
        key_columns=["PROJECT_NAME", "FLAG_KEY"],
        variant_columns=["PARAMS", "FIELD_ALIASES"],
    )


def create_or_update_flag_definition(definition: dict[str, Any], actor: str) -> int:
    flag_key = str(definition.get("FLAG_KEY") or "").strip()
    if not flag_key:
        raise ValueError("FLAG_KEY is required.")
    params = _json_loads(definition.get("DEFAULT_PARAMS"))
    if "expression" not in params:
        raise ValueError("DEFAULT_PARAMS must include an `expression` object.")
    row = {
        "FLAG_KEY": flag_key,
        "LABEL": str(definition.get("LABEL") or flag_key).strip(),
        "CATEGORY": str(definition.get("CATEGORY") or CATEGORY_DEMOGRAPHIC).strip(),
        "DESCRIPTION": str(definition.get("DESCRIPTION") or "").strip(),
        "SEVERITY": str(definition.get("SEVERITY") or "medium").strip(),
        "RULE_KIND": "declarative",
        "DEFAULT_ENABLED": _truthy(definition.get("DEFAULT_ENABLED"), True),
        "DEFAULT_PARAMS": _json_dumps(params),
        "MESSAGE_TEMPLATE": str(definition.get("MESSAGE_TEMPLATE") or "").strip(),
        "DISPLAY_ORDER": int(definition.get("DISPLAY_ORDER") or 500),
        "VERSION": int(definition.get("VERSION") or 1),
        "UPDATED_AT": datetime.utcnow(),
    }
    _audit(None, flag_key, "definition_saved", {}, row, actor)
    return merge_upsert(
        pd.DataFrame([row]),
        "DEMOGRAPHIC_FLAG_DEFINITIONS",
        key_columns=["FLAG_KEY"],
        variant_columns=["DEFAULT_PARAMS"],
    )


def _column_lookup(df: pd.DataFrame) -> dict[str, str]:
    return {_norm(col): str(col) for col in df.columns}


def _script_matching_columns(df: pd.DataFrame, columns_to_check: list[str]) -> list[str]:
    wanted = {_norm(column) for column in columns_to_check}
    return [str(column) for column in df.columns if _norm(column) in wanted]


def _resolve_column(df: pd.DataFrame, field: str, aliases: dict[str, list[str]], lookup: dict[str, str]) -> str | None:
    candidates = [field, *aliases.get(field, []), *DEFAULT_FIELD_ALIASES.get(field, [])]
    for candidate in candidates:
        normalized = _norm(candidate)
        if normalized in lookup:
            return lookup[normalized]
    return None


def _resolve_alias_column(df: pd.DataFrame, field: str, aliases: dict[str, list[str]], lookup: dict[str, str]) -> str | None:
    candidates = [*aliases.get(field, []), *DEFAULT_FIELD_ALIASES.get(field, [])]
    for candidate in candidates:
        normalized = _norm(candidate)
        if normalized in lookup:
            return lookup[normalized]
    return None


def _field_value(row: pd.Series, df: pd.DataFrame, field: str, aliases: dict[str, list[str]], lookup: dict[str, str]) -> Any:
    if field == "age":
        age_col = _resolve_alias_column(df, "age", aliases, lookup)
        if age_col:
            if df[age_col].isna().all():
                year_col = _resolve_alias_column(df, "year_born", aliases, lookup)
                if year_col and not df[year_col].isna().all():
                    return _script_birth_year_age(row.get(year_col))
                return 0
            return _script_age_split(row.get(age_col))
        return 0
    col = _resolve_column(df, field, aliases, lookup)
    return row.get(col) if col else None


def _eval_condition(
    condition: dict[str, Any],
    row: pd.Series,
    df: pd.DataFrame,
    aliases: dict[str, list[str]],
    lookup: dict[str, str],
) -> bool:
    if "all" in condition:
        return all(_eval_condition(item, row, df, aliases, lookup) for item in condition.get("all") or [])
    if "any" in condition:
        return any(_eval_condition(item, row, df, aliases, lookup) for item in condition.get("any") or [])
    if "not" in condition or "not_" in condition:
        return not _eval_condition(condition.get("not") or condition.get("not_") or {}, row, df, aliases, lookup)

    field = str(condition.get("field") or "").strip()
    op = str(condition.get("op") or "eq").strip().lower()
    left = _field_value(row, df, field, aliases, lookup)
    right = condition.get("value")

    if op == "blank":
        return _clean_scalar(left) is None
    if op in {"not_blank", "exists"}:
        return _clean_scalar(left) is not None
    if op == "not_null":
        return not pd.isna(left)
    if op == "missing":
        return _clean_scalar(left) is None
    if op == "eq":
        return _value_equal(left, right)
    if op == "neq":
        return not _value_equal(left, right)
    if op == "in":
        return any(_value_equal(left, item) for item in (right or []))
    if op == "not_in":
        return not any(_value_equal(left, item) for item in (right or []))
    if op == "raw_eq":
        return _raw_value_equal(left, right)
    if op == "raw_in":
        return any(_raw_value_equal(left, item) for item in (right or []))
    if op == "raw_str_lower_eq":
        return isinstance(left, str) and left.lower() == str(right or "").lower()
    if op in {"gt", "gte", "lt", "lte"}:
        left_num = _to_number(left)
        right_num = _to_number(right)
        if left_num is None or right_num is None:
            return False
        return {
            "gt": left_num > right_num,
            "gte": left_num >= right_num,
            "lt": left_num < right_num,
            "lte": left_num <= right_num,
        }[op]
    if op in {"gt_field", "gte_field", "lt_field", "lte_field", "eq_field"}:
        right_value = _field_value(row, df, str(condition.get("compare_field") or ""), aliases, lookup)
        left_num = _to_number(left)
        right_num = _to_number(right_value)
        if op == "eq_field":
            return _value_equal(left, right_value)
        if left_num is None or right_num is None:
            return False
        return {
            "gt_field": left_num > right_num,
            "gte_field": left_num >= right_num,
            "lt_field": left_num < right_num,
            "lte_field": left_num <= right_num,
        }[op]
    if op == "contains":
        return str(right or "").lower() in str(left or "").lower()
    if op == "contains_any":
        text = str(left or "").lower()
        return any(str(item).lower() in text for item in (right or []))
    if op == "startswith":
        return str(left or "").lower().startswith(str(right or "").lower())
    return False


def _active_rules(project_name: str) -> pd.DataFrame:
    matrix = load_project_flag_matrix(project_name)
    if matrix.empty:
        return matrix
    return matrix[matrix["EFFECTIVE_ENABLED"].map(lambda v: _truthy(v, True))].copy()


def _project_aliases(project_name: str) -> dict[str, list[str]]:
    configured = get_demographic_project_config(project_name).get("field_aliases") or DEFAULT_FIELD_ALIASES
    return {str(key): list(value) if isinstance(value, list) else [str(value)] for key, value in configured.items()}


def _load_demographic_source(project_name: str) -> pd.DataFrame:
    from pipeline.runner import _stage_elvis_export_for_demographics, build_context

    aliases = _project_aliases(project_name)
    ctx = build_context(project_name)
    elvis_csv = _stage_elvis_export_for_demographics(ctx)
    elvis = pd.read_csv(elvis_csv, low_memory=False)
    review = build_elvis_review_export(project_name)
    if elvis.empty or review.empty:
        return pd.DataFrame()
    id_col = _resolve_column(elvis, "id", aliases, _column_lookup(elvis))
    review_id_col = _resolve_column(review, "id", aliases, _column_lookup(review))
    if not id_col or not review_id_col:
        return pd.DataFrame()
    usage_col = _resolve_column(review, "Final_Usage", aliases, _column_lookup(review))
    elvis[id_col] = elvis[id_col].astype(str).str.strip()
    review[review_id_col] = review[review_id_col].astype(str).str.strip()
    review_cols = [review_id_col]
    if usage_col and usage_col not in review_cols:
        review_cols.append(usage_col)
    merged = elvis.merge(review[review_cols], left_on=id_col, right_on=review_id_col, how="left")
    if usage_col and usage_col in merged.columns:
        merged = merged[merged[usage_col].astype(str).str.strip().str.lower() == "use"]
    return merged


def _flag_columns_for_rules(project_name: str, rules: pd.DataFrame) -> list[str]:
    configured = get_demographic_project_config(project_name)
    columns = [str(item) for item in configured.get("script_flag_columns") or []]
    for flag_key in rules["FLAG_KEY"].astype(str).tolist() if "FLAG_KEY" in rules.columns else []:
        if flag_key not in columns:
            columns.append(flag_key)
    return columns


def script_flag_columns(project_name: str) -> list[str]:
    return _flag_columns_for_rules(project_name, _active_rules(project_name))


def _script_output_columns(project_name: str, source: pd.DataFrame, flag_columns: list[str], id_col: str | None) -> list[str]:
    configured = get_demographic_project_config(project_name)
    groups = configured.get("script_output_column_groups") or [["id"]]
    columns: list[str] = []
    seen: set[str] = set()
    for index, group in enumerate(groups):
        matched = _script_matching_columns(source, [str(item) for item in group])
        if index in {1, 2, 3}:
            matched.sort()
        for column in matched:
            if column not in seen:
                seen.add(column)
                columns.append(column)
    if id_col and id_col not in seen:
        columns.insert(0, id_col)
        seen.add(id_col)
    for column in [*flag_columns, "SUM_ALL_CHECKS"]:
        if column in source.columns and column not in seen:
            seen.add(column)
            columns.append(column)
    return columns


def evaluate_project_script_output(project_name: str, *, sample_size: int | None = None) -> pd.DataFrame:
    """Return the same row-shaped demographic output produced by od_demographics_checks.py."""
    rules = _active_rules(project_name)
    if rules.empty:
        return pd.DataFrame()

    source = _load_demographic_source(project_name)
    if source.empty:
        return pd.DataFrame()
    if sample_size:
        source = source.head(sample_size)

    source = source.copy()
    project_aliases = _project_aliases(project_name)
    lookup = _column_lookup(source)
    id_col = _resolve_column(source, "id", project_aliases, lookup)
    flag_columns = _flag_columns_for_rules(project_name, rules)
    for flag_key in flag_columns:
        source[flag_key] = 0

    for idx, record in source.iterrows():
        for _, rule in rules.iterrows():
            params = _json_loads(rule.get("PARAMS")) or _json_loads(rule.get("DEFAULT_PARAMS"))
            expression = params.get("expression") if isinstance(params, dict) else None
            if not isinstance(expression, dict):
                continue
            aliases = {str(key): list(value) for key, value in project_aliases.items()}
            custom_aliases = _json_loads(rule.get("FIELD_ALIASES"))
            if isinstance(custom_aliases, dict):
                aliases.update({str(k): list(v) if isinstance(v, list) else [str(v)] for k, v in custom_aliases.items()})
            flag_key = str(rule.get("FLAG_KEY"))
            source.at[idx, flag_key] = 1 if _eval_condition(expression, record, source, aliases, lookup) else 0

    source["SUM_ALL_CHECKS"] = source[flag_columns].any(axis=1).astype(int) if flag_columns else 0
    final_columns = _script_output_columns(project_name, source, flag_columns, id_col)
    output = source[source["SUM_ALL_CHECKS"] == 1].copy()
    if id_col and id_col in output.columns:
        output.drop_duplicates(subset=[id_col], keep="first", inplace=True)
    return output[final_columns] if final_columns else output


def _is_flagged(value: Any) -> bool:
    try:
        return int(value) == 1
    except (TypeError, ValueError):
        return str(value).strip().lower() in {"1", "true"}


def evaluate_project_rules(project_name: str, *, persist: bool = True, sample_size: int | None = None) -> pd.DataFrame:
    rules = _active_rules(project_name)
    if rules.empty:
        if persist:
            execute(f"DELETE FROM {REVIEW_CYCLE_SCHEMA}.DEMOGRAPHIC_CHECKS WHERE PROJECT_NAME = %s", (project_name,))
        return pd.DataFrame()

    output = evaluate_project_script_output(project_name, sample_size=sample_size)
    if output.empty:
        if persist:
            execute(f"DELETE FROM {REVIEW_CYCLE_SCHEMA}.DEMOGRAPHIC_CHECKS WHERE PROJECT_NAME = %s", (project_name,))
        return pd.DataFrame()

    project_aliases = _project_aliases(project_name)
    id_col = _resolve_column(output, "id", project_aliases, _column_lookup(output))
    rules_by_key = {str(rule.get("FLAG_KEY")): rule for _, rule in rules.iterrows()}
    flag_columns = [column for column in _flag_columns_for_rules(project_name, rules) if column in output.columns]
    rows: list[dict[str, Any]] = []
    now = datetime.utcnow()
    for _, record in output.iterrows():
        record_id = str(record.get(id_col) or "").strip() if id_col else ""
        if not record_id:
            continue
        for flag_key in flag_columns:
            if not _is_flagged(record.get(flag_key)):
                continue
            rule = rules_by_key.get(flag_key, {})
            detail = str(rule.get("EFFECTIVE_MESSAGE") or rule.get("EFFECTIVE_LABEL") or flag_key)
            rows.append(
                {
                    "PROJECT_NAME": project_name,
                    "RECORD_ID": record_id,
                    "CHECK_TYPE": flag_key,
                    "STATUS": "fail",
                    "DETAIL": detail[:4000],
                    "CATEGORY": str(rule.get("CATEGORY") or CATEGORY_DEMOGRAPHIC),
                    "SEVERITY": str(rule.get("EFFECTIVE_SEVERITY") or "medium"),
                    "RULE_VERSION": int(rule.get("EFFECTIVE_VERSION") or 1),
                    "DETAIL_JSON": _json_dumps(
                        {
                            "label": rule.get("EFFECTIVE_LABEL") or flag_key,
                            "description": rule.get("EFFECTIVE_DESCRIPTION"),
                            "category": rule.get("CATEGORY"),
                            "severity": rule.get("EFFECTIVE_SEVERITY"),
                        }
                    ),
                    "CREATED_AT": now,
                }
            )

    result = pd.DataFrame(rows)
    if persist:
        execute(f"DELETE FROM {REVIEW_CYCLE_SCHEMA}.DEMOGRAPHIC_CHECKS WHERE PROJECT_NAME = %s", (project_name,))
        if not result.empty:
            merge_upsert(
                result,
                "DEMOGRAPHIC_CHECKS",
                key_columns=["PROJECT_NAME", "RECORD_ID", "CHECK_TYPE"],
                variant_columns=["DETAIL_JSON"],
            )
    return result


def preview_project_rules(project_name: str, sample_size: int = 500) -> pd.DataFrame:
    return evaluate_project_script_output(project_name, sample_size=sample_size)


def load_demographic_results(project_name: str, status: str | None = "fail") -> pd.DataFrame:
    clauses = ["c.PROJECT_NAME = %s"]
    params: list[Any] = [project_name]
    if status:
        clauses.append("c.STATUS = %s")
        params.append(status)
    where = " AND ".join(clauses)
    return fetch_df_optional(
        f"""
        SELECT
            c.PROJECT_NAME,
            c.RECORD_ID,
            c.CHECK_TYPE,
            COALESCE(c.DETAIL_JSON:label::STRING, c.CHECK_TYPE) AS LABEL,
            COALESCE(c.CATEGORY, d.CATEGORY) AS CATEGORY,
            COALESCE(c.SEVERITY, cfg.SEVERITY_OVERRIDE, d.SEVERITY) AS SEVERITY,
            c.STATUS,
            c.DETAIL,
            c.RULE_VERSION,
            c.CREATED_AT
        FROM {REVIEW_CYCLE_SCHEMA}.DEMOGRAPHIC_CHECKS c
        LEFT JOIN {REVIEW_CYCLE_SCHEMA}.DEMOGRAPHIC_FLAG_DEFINITIONS d
            ON c.CHECK_TYPE = d.FLAG_KEY
        LEFT JOIN {REVIEW_CYCLE_SCHEMA}.DEMOGRAPHIC_FLAG_PROJECT_CONFIG cfg
            ON c.PROJECT_NAME = cfg.PROJECT_NAME AND c.CHECK_TYPE = cfg.FLAG_KEY
        WHERE {where}
        ORDER BY c.CREATED_AT DESC, c.RECORD_ID, c.CHECK_TYPE
        """,
        tuple(params),
    )


def export_results_csv(project_name: str) -> bytes:
    result = evaluate_project_script_output(project_name)
    return result.to_csv(index=False).encode("utf-8")


def last_result_path(project_name: str) -> Path | None:
    """Compatibility hook for callers that still expect a file path."""
    return None
