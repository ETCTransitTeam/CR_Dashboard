from __future__ import annotations

import re

import pandas as pd
import streamlit as st

from services import demographic_rules
from views.ui import (
    empty_state,
    info_strip,
    loading,
    page_header,
    section_title,
    set_operation_flash,
    stats_bar,
)


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


def _resolve_field_token(label: str) -> str:
    """Map known friendly labels to engine keys; otherwise keep free-text as typed."""
    text = str(label or "").strip()
    if not text:
        return ""
    if text in FIELD_OPTIONS:
        return FIELD_OPTIONS[text]
    known_keys = set(FIELD_OPTIONS.values())
    if text in known_keys:
        return text
    by_lower = {key.lower(): value for key, value in FIELD_OPTIONS.items()}
    if text.lower() in by_lower:
        return by_lower[text.lower()]
    return text


def _condition_from_row(row: pd.Series) -> dict | None:
    field_label = str(row.get("Field") or "").strip()
    operator_label = str(row.get("Operator") or "").strip()
    if not field_label or not operator_label:
        return None
    field = _resolve_field_token(field_label)
    op = OPERATOR_OPTIONS.get(operator_label)
    if not field or not op:
        return None
    if op == "gt_field":
        compare_label = str(row.get("Compare field") or "").strip()
        compare_field = _resolve_field_token(compare_label)
        if not compare_field:
            raise ValueError("Enter a compare field for every 'Greater than another field' condition.")
        return {"field": field, "op": op, "compare_field": compare_field}
    if op in {"blank", "not_blank", "not_null"}:
        return {"field": field, "op": op}
    value = _parse_condition_value(row.get("Value"), op)
    if value == "" or value == []:
        raise ValueError("Enter a value for each condition that requires one.")
    return {"field": field, "op": op, "value": value}


def _bump_condition_rows(count_key: str, delta: int, project: str) -> None:
    current = int(st.session_state.get(count_key, 2) or 2)
    new_count = max(1, min(current + delta, 12))
    if delta > 0:
        i = new_count - 1
        st.session_state.setdefault(f"nf_field_{project}_{i}", "")
        st.session_state.setdefault(f"nf_op_{project}_{i}", "Equals")
        st.session_state.setdefault(f"nf_value_{project}_{i}", "")
        st.session_state.setdefault(f"nf_compare_{project}_{i}", "")
    st.session_state[count_key] = new_count


def _seed_example_conditions(project: str, operator_labels: list[str]) -> None:
    """Ensure the two starter example rows keep Age / Student Status until the user edits them."""
    examples = [
        ("Age", "Greater than", "65", ""),
        ("Student Status", "Equals", "2", ""),
    ]
    seeded_key = f"nf_examples_seeded_{project}"
    if st.session_state.get(seeded_key):
        return
    for i, (field, op, value, compare) in enumerate(examples):
        st.session_state[f"nf_field_{project}_{i}"] = field
        st.session_state[f"nf_op_{project}_{i}"] = op if op in operator_labels else "Equals"
        st.session_state[f"nf_value_{project}_{i}"] = value
        st.session_state[f"nf_compare_{project}_{i}"] = compare
    st.session_state[seeded_key] = True


@st.fragment
def _render_new_flag_builder_fragment(project: str, user: dict) -> None:
    """Isolated fragment so add/remove row does not reload the full flag matrix."""
    st.caption(
        "Type any field name freely. Known labels (e.g. Age) still map to the engine; "
        "custom names are stored as typed. Leave unused condition rows blank."
    )

    count_key = f"new_flag_cond_count_{project}"
    if count_key not in st.session_state:
        st.session_state[count_key] = 2

    operator_labels = list(OPERATOR_OPTIONS.keys())
    _seed_example_conditions(project, operator_labels)

    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        flag_column = st.text_input(
            "Flag column name",
            placeholder="SENIOR_STUDENT_FLAG",
            key=f"new_flag_col_{project}",
        )
        label = st.text_input(
            "Display name",
            placeholder="Senior Student Flag",
            key=f"new_flag_label_{project}",
        )
        category = st.selectbox(
            "Category",
            [demographic_rules.CATEGORY_TRANSPORT, demographic_rules.CATEGORY_DEMOGRAPHIC],
            format_func=lambda value: (
                "Transport / Logistic"
                if value == demographic_rules.CATEGORY_TRANSPORT
                else "Demographic"
            ),
            key=f"new_flag_cat_{project}",
        )
    with c2:
        severity = st.selectbox(
            "Severity",
            ["low", "medium", "high"],
            index=1,
            key=f"new_flag_sev_{project}",
        )
        logic = st.radio(
            "Match logic",
            ["All conditions", "Any condition"],
            horizontal=True,
            key=f"new_flag_logic_{project}",
        )
    with c3:
        enabled = st.checkbox(
            "Enable for this project",
            value=True,
            key=f"new_flag_enabled_{project}",
        )
    description = st.text_area(
        "Description",
        placeholder="Explain when this flag should fire.",
        key=f"new_flag_desc_{project}",
    )
    message = st.text_area(
        "Reviewer message",
        placeholder="Message reviewers should see when this flag fails.",
        key=f"new_flag_msg_{project}",
    )

    with st.container(border=True):
        st.markdown("**Conditions**")
        st.caption("Each row is one rule. Use the buttons below the rows to add or remove.")

        hdr = st.columns([2, 2, 2, 2])
        hdr[0].caption("Field")
        hdr[1].caption("Operator")
        hdr[2].caption("Value")
        hdr[3].caption("Compare field")

        condition_rows: list[dict[str, str]] = []
        n_rows = int(st.session_state[count_key])
        for i in range(n_rows):
            field_key = f"nf_field_{project}_{i}"
            op_key = f"nf_op_{project}_{i}"
            value_key = f"nf_value_{project}_{i}"
            compare_key = f"nf_compare_{project}_{i}"
            st.session_state.setdefault(field_key, "")
            st.session_state.setdefault(op_key, "Equals")
            st.session_state.setdefault(value_key, "")
            st.session_state.setdefault(compare_key, "")

            rc1, rc2, rc3, rc4 = st.columns([2, 2, 2, 2])
            field_val = rc1.text_input(
                f"field_{i}",
                label_visibility="collapsed",
                placeholder="Field name",
                key=field_key,
            )
            op_val = rc2.selectbox(
                f"op_{i}",
                options=operator_labels,
                label_visibility="collapsed",
                key=op_key,
            )
            value_val = rc3.text_input(
                f"value_{i}",
                label_visibility="collapsed",
                placeholder="Value",
                key=value_key,
            )
            compare_val = rc4.text_input(
                f"compare_{i}",
                label_visibility="collapsed",
                placeholder="Optional",
                key=compare_key,
            )
            condition_rows.append(
                {
                    "Field": field_val,
                    "Operator": op_val,
                    "Value": value_val,
                    "Compare field": compare_val,
                }
            )

        st.markdown("")
        _spacer, add_col, rem_col = st.columns([3.2, 0.9, 1.1])
        add_col.button(
            "＋ Add",
            type="primary",
            use_container_width=True,
            key=f"new_flag_add_cond_{project}",
            on_click=_bump_condition_rows,
            args=(count_key, 1, project),
        )
        rem_col.button(
            "－ Remove",
            type="secondary",
            use_container_width=True,
            key=f"new_flag_rm_cond_{project}",
            disabled=n_rows <= 1,
            on_click=_bump_condition_rows,
            args=(count_key, -1, project),
        )

    submitted = st.button(
        "Create flag",
        type="primary",
        key=f"new_flag_submit_{project}",
    )
    if not submitted:
        return
    try:
        flag_key = _flag_key(flag_column)
        if not flag_key:
            raise ValueError("Flag column name is required.")
        if not str(label or "").strip():
            label = flag_key
        parsed_conditions = [
            condition
            for row in condition_rows
            if (condition := _condition_from_row(pd.Series(row))) is not None
        ]
        if not parsed_conditions:
            raise ValueError("Add at least one condition.")
        expression_key = "all" if logic == "All conditions" else "any"
        params = {"expression": {expression_key: parsed_conditions}}
        with loading(f"Creating demographic flag '{flag_key}'..."):
            demographic_rules.create_or_update_flag_definition(
                {
                    "FLAG_KEY": flag_key,
                    "LABEL": str(label).strip(),
                    "CATEGORY": category,
                    "DESCRIPTION": str(description or "").strip(),
                    "SEVERITY": severity,
                    "DEFAULT_ENABLED": False,
                    "DEFAULT_PARAMS": params,
                    "MESSAGE_TEMPLATE": str(message or "").strip() or str(label).strip(),
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
                            "EFFECTIVE_LABEL": str(label).strip(),
                            "EFFECTIVE_DESCRIPTION": str(description or "").strip(),
                            "EFFECTIVE_SEVERITY": severity,
                            "EFFECTIVE_MESSAGE": str(message or "").strip() or str(label).strip(),
                            "PARAMS": params,
                            "DEFAULT_PARAMS": params,
                            "FIELD_ALIASES": {},
                        }
                    ]
                ),
                _actor(user),
            )
        set_operation_flash(f"Created flag column '{flag_key}' for {project}.")
        st.rerun()
    except Exception as exc:
        st.error(str(exc))


def _render_new_flag_builder(project: str, user: dict) -> None:
    with st.expander("Create a new project-specific flag", expanded=False):
        _render_new_flag_builder_fragment(project, user)


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
    with loading("Loading project demographic flag rules..."):
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
                with loading("Saving demographic flag configuration..."):
                    count = demographic_rules.save_project_flag_config(project, save_frame, _actor(user))
                set_operation_flash(f"Saved {count} configuration row(s).")
                st.rerun()
            except Exception as exc:
                st.error(str(exc))
    with c2:
        if st.button("Preview current results", use_container_width=True):
            try:
                with loading("Evaluating demographic rules against preview records..."):
                    preview = demographic_rules.preview_project_rules(project)
                if preview.empty:
                    st.info("Preview found no failed checks.")
                else:
                    st.success(f"Preview found {len(preview)} script-style flagged record row(s).")
                    st.dataframe(preview, use_container_width=True, hide_index=True)
            except Exception as exc:
                st.error(str(exc))
