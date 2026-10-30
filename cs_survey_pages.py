"""Streamlit pages for the optional Customer Satisfaction (second) survey."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import cs_survey as cs

CS_REVIEW_MENU_LABEL = "📝  CS Demographic Review"
CS_REVIEW_PAGE_KEY = "cs_demographic"
CS_SETUP_PAGE_KEY = "cs_demographic_setup"
CS_SETUP_MENU_LABEL = "🧾  CS Demographic Setup"

_BAR_COLOR = "#0068c9"


# ---------------------------------------------------------------------------
# Cached reads (cleared whenever the setup page saves)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300, show_spinner=False)
def get_cs_config_cached(project):
    return cs.load_cs_survey_config(project)


@st.cache_data(ttl=120, show_spinner=False)
def _details_sheets_cached(details_key):
    return cs.list_details_sheets(details_key)


@st.cache_data(ttl=120, show_spinner=False)
def _dictionary_cached(details_key, sheet_name):
    return cs.read_cs_dictionary_sheet(details_key, sheet_name)


@st.cache_data(ttl=120, show_spinner=False)
def _table_columns_cached(database, table):
    from utils import fetch_table_columns

    return fetch_table_columns(database, table)


def clear_cs_caches():
    get_cs_config_cached.clear()
    _details_sheets_cached.clear()
    _dictionary_cached.clear()
    _table_columns_cached.clear()


def project_has_cs_review(project):
    """True when the project has an enabled second survey (drives the menu item)."""
    if not project:
        return False
    try:
        return cs.is_cs_enabled(get_cs_config_cached(project))
    except Exception as e:
        print(f"[CS survey] Menu check skipped for {project}: {e}")
        return False


# ---------------------------------------------------------------------------
# Setup page
# ---------------------------------------------------------------------------
def _status_strip(cfg, sheet_found, selected_count, total_count):
    c1, c2, c3, c4 = st.columns(4)
    enabled = cs.is_cs_enabled(cfg)
    c1.metric("Second survey", "On" if enabled else "Off")
    c2.metric("Survey id", cfg.get("survey_id") or "-")
    c3.metric("Dictionary sheet", "Found" if sheet_found else ("Missing" if enabled else "-"))
    c4.metric("Questions selected", f"{selected_count} / {total_count}" if total_count else "-")


def _connection_form(project, cfg, details_key):
    with st.container(border=True):
        st.markdown("#### 1. Second survey connection")
        st.caption(
            "Turn this on only for projects that run a second survey. "
            "When it is off, the project's sync, menu and reports stay exactly as they are."
        )
        sheets = _details_sheets_cached(details_key) if details_key else []

        with st.form(f"cs_connection_form_{project}", border=False):
            enabled = st.toggle(
                "This project has a Customer Satisfaction survey",
                value=bool(cfg.get("enabled")),
                key=f"cs_enabled_{project}",
            )
            c1, c2 = st.columns([3, 1])
            survey_name = c1.text_input(
                "Survey name", value=cfg.get("survey_name", ""),
                placeholder="Nashville Customer Satisfaction Survey",
            )
            survey_id = c2.text_input("Survey id", value=cfg.get("survey_id", ""), placeholder="347822")

            c3, c4 = st.columns(2)
            database = c3.text_input(
                "CS Elvis database", value=cfg.get("elvis_database", ""),
                help="MySQL database that stores the second survey's records.",
            )
            table = c4.text_input(
                "CS Elvis table", value=cfg.get("elvis_table", ""),
                help="Table inside that database with one row per CS response.",
            )

            c5, c6 = st.columns(2)
            current_sheet = cfg.get("dictionary_sheet") or cs.CS_DEFAULT_DICTIONARY_SHEET
            if sheets:
                options = list(sheets)
                if current_sheet not in options:
                    options = [current_sheet] + options
                sheet = c5.selectbox(
                    "Dictionary sheet in the Details file",
                    options,
                    index=options.index(current_sheet),
                    help="Sheet with FIELD NAME, DESCRIPTION, CODE VALUES and optional Group Name.",
                )
            else:
                sheet = c5.text_input("Dictionary sheet in the Details file", value=current_sheet)
            ok_field = c6.text_input(
                "OD invite field (Yes/No)",
                value=cfg.get("ok2invite_field") or cs.CS_DEFAULT_OK2INVITE_FIELD,
                help="OD question that asks whether the rider can be invited. "
                     "Shown as '% of OK2Invite2CS' on the Surveyor and Route reports.",
            )
            saved = st.form_submit_button("Save connection", type="primary")

        if saved:
            new_cfg = {
                "enabled": enabled,
                "survey_name": survey_name,
                "survey_id": survey_id,
                "elvis_database": database,
                "elvis_table": table,
                "dictionary_sheet": sheet,
                "ok2invite_field": ok_field,
            }
            if enabled and not (database.strip() and table.strip()):
                st.error("Enter the CS Elvis database and table before turning the second survey on.")
            elif cs.save_cs_survey_config(project, new_cfg):
                clear_cs_caches()
                st.session_state[f"cs_conn_saved_{project}"] = True
                st.rerun()
            else:
                st.error("Could not save the connection to S3.")

        if st.session_state.pop(f"cs_conn_saved_{project}", False):
            st.success("Connection saved.")

        if cs.is_cs_enabled(cfg):
            if st.button("Test CS table", key=f"cs_test_{project}"):
                _table_columns_cached.clear()
                with st.spinner("Connecting to the CS table..."):
                    cols = _table_columns_cached(cfg["elvis_database"], cfg["elvis_table"])
                if cols:
                    st.success(f"Connected. {len(cols)} columns found in {cfg['elvis_database']}.{cfg['elvis_table']}.")
                else:
                    st.error("Could not read that table. Check the database and table names.")


def _match_label(fields, matches, checked):
    if not checked:
        return "Not checked"
    found = sum(1 for f in fields if matches.get(f))
    if found == len(fields):
        return "✅ Found"
    if found == 0:
        return "❌ Not found"
    return f"⚠️ {found} of {len(fields)}"


def _answers_label(item):
    if item["slots"]:
        return f"{len(item['slots'])} ranks × {item['options']} options"
    if item["type_label"] == cs.TYPE_LABELS[cs.TYPE_VALUE]:
        return "Values as recorded"
    return f"{item['options']} options"


def _question_picker(project, cfg, details_key):
    sheet = cfg["dictionary_sheet"]
    with st.container(border=True):
        st.markdown("#### 2. Questions for CS Demographic Review")
        st.caption(
            f"Read from the **{sheet}** sheet. Answer labels come from **CODE VALUES** "
            "(for example `05 = Very satisfied`), so charts show words instead of codes."
        )
        with st.spinner("Reading the CS dictionary..."):
            dd_df = _dictionary_cached(details_key, sheet) if details_key else None
        if dd_df is None or dd_df.empty:
            st.warning(
                f"Sheet **{sheet}** was not found in the Details file `{details_key or '-'}`.\n\n"
                "Add it with the columns **FIELD NAME**, **DESCRIPTION**, **CODE VALUES**, and optionally "
                "**Index**, **LABEL**, **Group Name** (one row per answer code), then reload this page."
            )
            return 0, 0

        questions = cs.parse_cs_dictionary(dd_df)
        items = cs.setup_items(questions)
        if not items:
            st.warning("No questions could be read from this sheet. Check that FIELD NAME is filled in.")
            return 0, 0

        with st.spinner("Matching fields to the CS table..."):
            columns = _table_columns_cached(cfg["elvis_database"], cfg["elvis_table"])
        matches = cs.field_match_report(questions, columns)
        checked = columns is not None
        matched_items = [i for i in items if all(matches.get(f) for f in i["fields"])] if checked else items

        state_key = f"cs_selection_{project}"
        version_key = f"cs_editor_version_{project}"
        if state_key not in st.session_state:
            saved = cs.load_cs_setup(project)
            if saved is not None:
                chosen = set(saved["selected"])
            else:
                chosen = {i["setup_key"] for i in matched_items}
            st.session_state[state_key] = {i["setup_key"]: i["setup_key"] in chosen for i in items}
            st.session_state[version_key] = 0
        selection = st.session_state[state_key]
        for i in items:
            selection.setdefault(i["setup_key"], False)

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Dictionary rows", len(dd_df))
        m2.metric("Questions", len(items))
        m3.metric("Charts", len(questions))
        m4.metric("Found in CS table", f"{len(matched_items)} / {len(items)}" if checked else "Not checked")
        if not checked:
            st.info("The CS table could not be reached, so field matching is skipped. You can still choose and save questions.")

        f1, f2 = st.columns([2, 3])
        search = f1.text_input("Search", placeholder="Field or question text", key=f"cs_search_{project}")
        type_options = ["All"] + sorted({i["type_label"] for i in items})
        type_filter = f2.segmented_control(
            "Question type", type_options, default="All", key=f"cs_type_{project}"
        ) or "All"

        def visible(item):
            text = (item["setup_key"] + " " + item["question"]).lower()
            return (not search or search.lower() in text) and (type_filter == "All" or item["type_label"] == type_filter)

        shown = [i for i in items if visible(i)]

        b1, b2, b3, _ = st.columns([1, 1, 1.4, 3])
        if b1.button("Select all", key=f"cs_all_{project}", use_container_width=True):
            for i in shown:
                selection[i["setup_key"]] = True
            st.session_state[version_key] += 1
            st.rerun()
        if b2.button("Clear", key=f"cs_none_{project}", use_container_width=True):
            for i in shown:
                selection[i["setup_key"]] = False
            st.session_state[version_key] += 1
            st.rerun()
        if b3.button("Only found fields", key=f"cs_found_{project}", use_container_width=True, disabled=not checked):
            found = {i["setup_key"] for i in matched_items}
            for i in items:
                selection[i["setup_key"]] = i["setup_key"] in found
            st.session_state[version_key] += 1
            st.rerun()

        table = pd.DataFrame([
            {
                "Include": bool(selection.get(i["setup_key"])),
                "Question": i["question"],
                "Type": i["type_label"],
                "Answers": _answers_label(i),
                "Fields": ", ".join(i["fields"]) if len(i["fields"]) <= 3 else f"{i['fields'][0]} … {i['fields'][-1]}",
                "In CS table": _match_label(i["fields"], matches, checked),
            }
            for i in shown
        ])
        if table.empty:
            st.caption("No questions match the current filter.")
        else:
            edited = st.data_editor(
                table,
                key=f"cs_editor_{project}_{st.session_state[version_key]}_{type_filter}_{search}",
                hide_index=True,
                use_container_width=True,
                height=min(560, 38 + 35 * len(table)),
                disabled=["Question", "Type", "Answers", "Fields", "In CS table"],
                column_config={
                    "Include": st.column_config.CheckboxColumn("Include", width="small"),
                    "Question": st.column_config.TextColumn("Question", width="large"),
                    "Type": st.column_config.TextColumn("Type", width="small"),
                    "Answers": st.column_config.TextColumn("Answers", width="small"),
                    "In CS table": st.column_config.TextColumn("In CS table", width="small"),
                },
            )
            for item, include in zip(shown, edited["Include"].tolist()):
                selection[item["setup_key"]] = bool(include)

        selected_keys = [i["setup_key"] for i in items if selection.get(i["setup_key"])]
        s1, s2 = st.columns([1, 3])
        if s1.button(
            f"Save {len(selected_keys)} question{'s' if len(selected_keys) != 1 else ''}",
            type="primary",
            key=f"cs_save_{project}",
            use_container_width=True,
        ):
            if cs.save_cs_setup(project, selected_keys):
                st.session_state[f"cs_setup_saved_{project}"] = len(selected_keys)
                st.rerun()
            else:
                st.error("Could not save the question selection to S3.")
        s2.caption("Run **Sync** on Home after saving to refresh CS Demographic Review.")

        saved_count = st.session_state.pop(f"cs_setup_saved_{project}", None)
        if saved_count is not None:
            st.success(f"Saved {saved_count} question(s) for {project}. Run Sync to refresh the review page.")

        with st.expander("Preview answer labels"):
            labels = {i["setup_key"]: i["question"] for i in items}
            pick = st.selectbox(
                "Question", list(labels), format_func=lambda k: f"{k} - {labels[k][:90]}",
                key=f"cs_preview_{project}",
            )
            for q in [q for q in questions if q["setup_key"] == pick]:
                if q["slot"]:
                    st.markdown(f"**Rank {q['slot']}**")
                if q["type"] == cs.TYPE_SELECT_ALL:
                    preview = pd.DataFrame([
                        {"Field": o["field"], "Option label": o["label"], "Counted when": "1 / Yes / checked"}
                        for o in q["options"]
                    ])
                elif q["options"]:
                    preview = pd.DataFrame([
                        {"Stored code": o["code"], "Label on chart": o["label"]} for o in q["options"]
                    ])
                else:
                    preview = pd.DataFrame([{"Field": q["fields"][0], "Label on chart": "The recorded value"}])
                st.dataframe(preview, hide_index=True, use_container_width=True)
                if q["slot"]:
                    break

        return len(selected_keys), len(items)


def render_cs_setup_page(projects):
    st.title("🧾 CS Demographic Setup")
    st.markdown(
        "Connect an optional **Customer Satisfaction survey** to an OD project and choose which of its "
        "questions appear on **CS Demographic Review**. Projects without a connection are not affected."
    )
    if not cs._bucket():
        st.error("S3 bucket not configured (bucket_name).")
        return

    names = sorted(projects.keys())
    if not names:
        st.warning("No projects available.")
        return
    project = st.selectbox("Select Project", names, key="cs_setup_project")
    project_config = projects.get(project) or {}
    details_key = (project_config.get("files") or {}).get("details")

    cfg = get_cs_config_cached(project) or cs.normalize_cs_config({})
    sheets = _details_sheets_cached(details_key) if details_key else []
    sheet_found = any(s.strip().lower() == cfg["dictionary_sheet"].strip().lower() for s in sheets)
    strip = st.container()

    if not details_key:
        st.warning(f"Project **{project}** has no Details file configured. Upload one in File Management first.")

    _connection_form(project, cfg, details_key)

    selected, total = 0, 0
    if cs.is_cs_enabled(cfg):
        selected, total = _question_picker(project, cfg, details_key)
    else:
        st.info(
            "The second survey is off for this project. Turn it on above and save the connection "
            "to choose questions. Nothing else in the dashboard changes while it is off."
        )

    with strip:
        _status_strip(cfg, sheet_found, selected, total)


# ---------------------------------------------------------------------------
# Review page
# ---------------------------------------------------------------------------
def _short(text, n=70):
    text = str(text)
    return text if len(text) <= n else text[: n - 1] + "…"


def _answer_chart(qdf, select_all):
    qdf = qdf.sort_values("Answer Order")
    labels = qdf["Answer Text"].astype(str).tolist()
    pcts = qdf["Percentage"].astype(float).tolist()
    counts = qdf["Count"].astype(int).tolist()
    horizontal = len(labels) > 9 or max((len(l) for l in labels), default=0) > 22
    text = [f"{p:.1f}% ({c:,})" for p, c in zip(pcts, counts)]
    axis_max = 100 if select_all or max(pcts, default=0) > 60 else min(100, max(pcts, default=0) * 1.25 + 5)

    if horizontal:
        fig = go.Figure(go.Bar(
            x=pcts, y=labels, orientation="h", text=text, textposition="outside",
            customdata=counts, marker_color=_BAR_COLOR,
            hovertemplate="<b>%{y}</b><br>%{x:.1f}% of respondents<br>Count: %{customdata:,}<extra></extra>",
        ))
        fig.update_layout(
            height=max(320, 30 * len(labels) + 80),
            xaxis=dict(range=[0, axis_max * 1.15], ticksuffix="%", title="% of respondents"),
            yaxis=dict(autorange="reversed", title=None),
        )
    else:
        fig = go.Figure(go.Bar(
            x=labels, y=pcts, text=text, textposition="outside",
            customdata=counts, marker_color=_BAR_COLOR,
            hovertemplate="<b>%{x}</b><br>%{y:.1f}% of respondents<br>Count: %{customdata:,}<extra></extra>",
        ))
        fig.update_layout(
            height=440,
            yaxis=dict(range=[0, axis_max * 1.15], ticksuffix="%", title="% of respondents"),
            xaxis=dict(title=None, tickangle=0 if len(labels) <= 6 else -30),
        )
    fig.update_layout(
        margin=dict(t=20, b=40, l=20, r=30),
        plot_bgcolor="white",
        font=dict(color="black"),
        showlegend=False,
    )
    return fig


def render_cs_review_page(project, df, cfg):
    survey_label = (cfg or {}).get("survey_name") or "Customer Satisfaction survey"
    survey_id = (cfg or {}).get("survey_id")
    st.title("📝 CS Demographic Review")
    st.caption(f"{survey_label}{f' ({survey_id})' if survey_id else ''} · second survey for {project}")

    if df is None or df.empty or "Question Key" not in df.columns:
        st.info(
            "**No CS results yet.**\n\n"
            "A super administrator chooses questions in **CS Demographic Setup**, then the next **Sync** "
            "loads the second survey and fills this page."
        )
        return

    df = df.copy()
    for col in ("Count", "Percentage", "Respondents", "Question Order", "Answer Order"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    questions = (
        df.drop_duplicates("Question Key")
        .sort_values("Question Order")[["Question Key", "Question", "Question Type", "Respondents"]]
    )

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Questions", len(questions))
    m2.metric("Most respondents", f"{int(questions['Respondents'].max()):,}")
    m3.metric("Rating / single choice", int((questions["Question Type"] == cs.TYPE_LABELS[cs.TYPE_SINGLE]).sum()))
    m4.metric("Select all / ranked", int(questions["Question Type"].isin(
        [cs.TYPE_LABELS[cs.TYPE_SELECT_ALL], cs.RANK_TYPE_LABEL]).sum()))

    left, right = st.columns([2, 1])

    with right:
        with st.container(height=640, border=True):
            st.markdown("### Survey Questions")
            search = st.text_input("Search question", placeholder="Type to search a question...",
                                   label_visibility="collapsed", key="cs_review_search")
            types = ["All"] + [t for t in questions["Question Type"].unique().tolist()]
            type_filter = st.segmented_control("Type", types, default="All", key="cs_review_type",
                                               label_visibility="collapsed") or "All"
            shown = questions
            if search:
                shown = shown[shown["Question"].str.contains(search, case=False, regex=False)
                              | shown["Question Key"].str.contains(search, case=False, regex=False)]
            if type_filter != "All":
                shown = shown[shown["Question Type"] == type_filter]
            keys = shown["Question Key"].tolist()
            titles = dict(zip(questions["Question Key"], questions["Question"]))
            if not keys:
                st.caption("No questions match.")
                selected = None
            else:
                current = st.session_state.get("cs_review_question")
                selected = st.radio(
                    "Select a question",
                    keys,
                    index=keys.index(current) if current in keys else 0,
                    format_func=lambda k: _short(titles.get(k, k), 90),
                    label_visibility="collapsed",
                )
                st.session_state["cs_review_question"] = selected

    with left:
        with st.container(height=640, border=True):
            if not selected:
                st.info("Select a question on the right to view its results.")
            else:
                qdf = df[df["Question Key"] == selected]
                qtype = qdf["Question Type"].iloc[0]
                respondents = int(qdf["Respondents"].max())
                st.markdown(f"**{qdf['Question'].iloc[0]}**")
                note = {
                    cs.TYPE_LABELS[cs.TYPE_SELECT_ALL]:
                        "Riders could pick more than one answer, so bars do not add up to 100%.",
                    cs.RANK_TYPE_LABEL: "Share of riders who put each option in this rank.",
                }.get(qtype, "Share of riders who answered this question.")
                st.caption(f"{qtype} · {respondents:,} respondents · {note}")
                st.plotly_chart(
                    _answer_chart(qdf, qtype == cs.TYPE_LABELS[cs.TYPE_SELECT_ALL]),
                    use_container_width=True,
                    config={"displayModeBar": False},
                )
                with st.expander("Show as table"):
                    st.dataframe(
                        qdf.sort_values("Answer Order")[["Answer Text", "Count", "Percentage"]]
                        .rename(columns={"Answer Text": "Answer", "Percentage": "% of respondents"}),
                        hide_index=True,
                        use_container_width=True,
                    )

    export = df.sort_values(["Question Order", "Answer Order"])[
        ["Question Key", "Question", "Question Type", "Answer Text", "Count", "Percentage", "Respondents"]
    ]
    st.download_button(
        "Download all CS results (CSV)",
        export.to_csv(index=False).encode("utf-8"),
        file_name=f"{project}_cs_demographic_review.csv".replace(" ", "_"),
        mime="text/csv",
    )
