from __future__ import annotations

import re

import pandas as pd
import streamlit as st

from services import demographic_rules
from views.ui import empty_state, info_strip, page_header, section_title, stats_bar


def _actor(user: dict) -> str:
    return str(user.get("name") or user.get("EMAIL") or "unknown")


FIELD_OPTIONS = {
    "Age": "age",
    "Origin Place Type": "origin_place_type",
    "Destination Place Type": "destin_place_type",
    "Student Status": "student_status",
    "Employment Status": "employment_status",
    "Employed In Household": "employed_in_hh",
    "Household Size": "hh_size",
    "Travel With Household": "travel_hh",
    "Household Vehicle Count": "count_vehicle_hh",
    "Used Vehicle On Trip": "used_vehicle_trip",
    "Driver License": "driver_license",
    "Fare Type": "fare_type",
}

OPERATOR_OPTIONS = {
    "Equals": "eq",
    "Does not equal": "neq",
    "Raw equals (script-style)": "raw_eq",
    "Greater than": "gt",
    "Greater than or equal to": "gte",
    "Less than": "lt",
    "Less than or equal to": "lte",
    "Is one of": "in",
    "Is not one of": "not_in",
    "Raw is one of (script-style)": "raw_in",
    "Contains": "contains",
    "Contains any of": "contains_any",
    "Raw lowercase text equals": "raw_str_lower_eq",
    "Is blank": "blank",
    "Is not blank": "not_blank",
    "Is not null": "not_null",
    "Greater than another field": "gt_field",
}


def _flag_key(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", value.upper()).strip("_")[:128]


def _parse_condition_value(value: object, op: str) -> object:
    text = str(value or "").strip()
    if op in {"blank", "not_blank", "not_null"}:
        return None
    if op in {"in", "not_in", "contains_any", "raw_in"}:
        return [_parse_single_value(item) for item in text.split(",") if str(item).strip()]
    return _parse_single_value(text)


def _parse_single_value(value: object) -> object:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        num = float(text)
        return int(num) if num.is_integer() else num
    except ValueError:
        return text


def _condition_from_row(row: pd.Series) -> dict | None:
    field_label = str(row.get("Field") or "").strip()
    operator_label = str(row.get("Operator") or "").strip()
    if not field_label or not operator_label:
        return None
    field = FIELD_OPTIONS.get(field_label)
    op = OPERATOR_OPTIONS.get(operator_label)
    if not field or not op:
        return None
    if op == "gt_field":
        compare_label = str(row.get("Compare field") or "").strip()
        compare_field = FIELD_OPTIONS.get(compare_label)
        if not compare_field:
            raise ValueError("Choose a compare field for every 'Greater than another field' condition.")
        return {"field": field, "op": op, "compare_field": compare_field}
    if op in {"blank", "not_blank", "not_null"}:
        return {"field": field, "op": op}
    value = _parse_condition_value(row.get("Value"), op)
    if value == "" or value == []:
        raise ValueError("Enter a value for each condition that requires one.")
    return {"field": field, "op": op, "value": value}


def _render_new_flag_builder(project: str, user: dict) -> None:
    with st.expander("Create a new project-specific flag", expanded=False):
        st.caption("Build the flag with dropdowns. The flag column name is what appears on the Demographic Review output.")
        with st.form(f"new_flag_builder_{project}"):
            c1, c2, c3 = st.columns([1, 1, 1])
            with c1:
                flag_column = st.text_input("Flag column name", placeholder="SENIOR_STUDENT_FLAG")
                label = st.text_input("Display name", placeholder="Senior Student Flag")
                category = st.selectbox(
                    "Category",
                    [demographic_rules.CATEGORY_TRANSPORT, demographic_rules.CATEGORY_DEMOGRAPHIC],
                    format_func=lambda value: "Transport / Logistic" if value == demographic_rules.CATEGORY_TRANSPORT else "Demographic",
                )
            with c2:
                severity = st.selectbox("Severity", ["low", "medium", "high"], index=1)
                logic = st.radio("Match logic", ["All conditions", "Any condition"], horizontal=True)
            with c3:
                enabled = st.checkbox("Enable for this project", value=True)
            description = st.text_area("Description", placeholder="Explain when this flag should fire.")
            message = st.text_area("Reviewer message", placeholder="Message reviewers should see when this flag fails.")

            default_conditions = pd.DataFrame(
                [
                    {"Field": "Age", "Operator": "Greater than", "Value": "65", "Compare field": ""},
                    {"Field": "Student Status", "Operator": "Equals", "Value": "2", "Compare field": ""},
                ]
            )
            conditions = st.data_editor(
                default_conditions,
                hide_index=True,
                use_container_width=True,
                num_rows="dynamic",
                column_config={
                    "Field": st.column_config.SelectboxColumn("Field", options=list(FIELD_OPTIONS.keys()), required=True),
                    "Operator": st.column_config.SelectboxColumn(
                        "Operator",
                        options=list(OPERATOR_OPTIONS.keys()),
                        required=True,
                    ),
                    "Value": st.column_config.TextColumn("Value", help="For multiple values, separate with commas."),
                    "Compare field": st.column_config.SelectboxColumn(
                        "Compare field",
                        options=["", *FIELD_OPTIONS.keys()],
                        help="Used only for field-to-field comparisons.",
                    ),
                },
            )
            submitted = st.form_submit_button("Create flag", type="primary")
        if not submitted:
            return
        try:
            flag_key = _flag_key(flag_column)
            if not flag_key:
                raise ValueError("Flag column name is required.")
            if not label.strip():
                label = flag_key
            parsed_conditions = [
                condition
                for _, row in conditions.iterrows()
                if (condition := _condition_from_row(row)) is not None
            ]
            if not parsed_conditions:
                raise ValueError("Add at least one condition.")
            expression_key = "all" if logic == "All conditions" else "any"
            params = {"expression": {expression_key: parsed_conditions}}
            demographic_rules.create_or_update_flag_definition(
                {
                    "FLAG_KEY": flag_key,
                    "LABEL": label.strip(),
                    "CATEGORY": category,
                    "DESCRIPTION": description.strip(),
                    "SEVERITY": severity,
                    "DEFAULT_ENABLED": False,
                    "DEFAULT_PARAMS": params,
                    "MESSAGE_TEMPLATE": message.strip() or label.strip(),
                    "DISPLAY_ORDER": 500,
                },
                _actor(user),
            )
            demographic_rules.save_project_flag_config(
                project,
                pd.DataFrame(
                    [
                        {
                            "FLAG_KEY": flag_key,
                            "EFFECTIVE_ENABLED": enabled,
                            "EFFECTIVE_LABEL": label.strip(),
                            "EFFECTIVE_DESCRIPTION": description.strip(),
                            "EFFECTIVE_SEVERITY": severity,
                            "EFFECTIVE_MESSAGE": message.strip() or label.strip(),
                            "PARAMS": params,
                            "DEFAULT_PARAMS": params,
                            "FIELD_ALIASES": {},
                        }
                    ]
                ),
                _actor(user),
            )
            st.success(f"Created flag column '{flag_key}' for {project}.")
            st.rerun()
        except Exception as exc:
            st.error(str(exc))


def _editor_frame(project: str) -> pd.DataFrame:
    matrix = demographic_rules.load_project_flag_matrix(project)
    if matrix.empty:
        return matrix
    out = matrix[
        [
            "FLAG_KEY",
            "CATEGORY",
            "EFFECTIVE_ENABLED",
            "EFFECTIVE_LABEL",
            "EFFECTIVE_SEVERITY",
            "EFFECTIVE_MESSAGE",
            "EFFECTIVE_DESCRIPTION",
            "PARAMS",
            "DEFAULT_PARAMS",
            "FIELD_ALIASES",
        ]
    ].copy()
    return out


def render_demographic_config_page(user: dict) -> None:
    role = str(user.get("ROLE") or user.get("role") or "").lower()
    if role not in {"admin", "super_admin"}:
        page_header("Demographic Flag Configuration")
        empty_state("Admin only", "Only System Admin and Super Admin users can configure demographic review flags.")
        return

    page_header(
        "Demographic Flag Configuration",
        "Manage project-specific Transport / Logistic and Demographic Review Flags without code deployments.",
    )

    from core.session_project import require_active_project

    project = require_active_project()
    if not project:
        return

    section_title("Project")
    with st.container(border=True):
        st.caption(f"Active project: **{project}**")
        info_strip("Changes are stored in Snowflake and apply the next time demographic checks are refreshed.")

    _render_new_flag_builder(project, user)

    section_title("Flag rules")
    editor = _editor_frame(project)
    if editor.empty:
        empty_state("No flags", "Default flag definitions could not be loaded.")
        return

    category_counts = editor.groupby("CATEGORY")["FLAG_KEY"].count().to_dict()
    stats_bar(
        [
            ("Total flags", str(len(editor))),
            ("Transport / Logistic", str(category_counts.get(demographic_rules.CATEGORY_TRANSPORT, 0))),
            ("Demographic", str(category_counts.get(demographic_rules.CATEGORY_DEMOGRAPHIC, 0))),
            ("Enabled", str(int(editor["EFFECTIVE_ENABLED"].sum()))),
        ]
    )

    visible_columns = [
        "FLAG_KEY",
        "CATEGORY",
        "EFFECTIVE_ENABLED",
        "EFFECTIVE_LABEL",
        "EFFECTIVE_SEVERITY",
        "EFFECTIVE_MESSAGE",
        "EFFECTIVE_DESCRIPTION",
    ]
    edited = st.data_editor(
        editor[visible_columns],
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        column_config={
            "FLAG_KEY": st.column_config.TextColumn("Flag key", disabled=True),
            "CATEGORY": st.column_config.SelectboxColumn(
                "Category",
                options=[demographic_rules.CATEGORY_TRANSPORT, demographic_rules.CATEGORY_DEMOGRAPHIC],
                disabled=True,
            ),
            "EFFECTIVE_ENABLED": st.column_config.CheckboxColumn("Enabled"),
            "EFFECTIVE_LABEL": st.column_config.TextColumn("Label"),
            "EFFECTIVE_SEVERITY": st.column_config.SelectboxColumn("Severity", options=["low", "medium", "high"]),
            "EFFECTIVE_MESSAGE": st.column_config.TextColumn("Reviewer message"),
            "EFFECTIVE_DESCRIPTION": st.column_config.TextColumn("Description"),
        },
        key=f"demo_flag_editor_{project}",
    )

    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("Save project configuration", type="primary", use_container_width=True):
            try:
                save_frame = editor.copy()
                for col in visible_columns:
                    save_frame[col] = edited[col]
                count = demographic_rules.save_project_flag_config(project, save_frame, _actor(user))
                st.success(f"Saved {count} configuration row(s).")
                st.rerun()
            except Exception as exc:
                st.error(str(exc))
    with c2:
        if st.button("Preview current results", use_container_width=True):
            try:
                preview = demographic_rules.preview_project_rules(project)
                if preview.empty:
                    st.info("Preview found no failed checks.")
                else:
                    st.success(f"Preview found {len(preview)} script-style flagged record row(s).")
                    st.dataframe(preview, use_container_width=True, hide_index=True)
            except Exception as exc:
                st.error(str(exc))
