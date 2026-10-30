"""Optional second survey (Customer Satisfaction) attached to an OD project.

Everything here is opt-in per project: a project without a saved CS config in S3
never reaches the dictionary reader, the CS database, or the CS review table.

S3 objects (same bucket and folder as the OD demographic setup):
    config/<PROJECT>_cs_survey_config.json      connection + on/off switch
    config/<PROJECT>_cs_demographic_setup.json  questions selected for the review page
"""

from __future__ import annotations

import io
import json
import os
import re
from collections import OrderedDict

import pandas as pd

CS_CONFIG_PREFIX = "config/"
CS_CONFIG_SUFFIX = "_cs_survey_config.json"
CS_SETUP_SUFFIX = "_cs_demographic_setup.json"
CS_REVIEW_TABLE = "demographic_review_cs"
CS_DEFAULT_DICTIONARY_SHEET = "DATA DICTIONARY CS"
CS_DEFAULT_OK2INVITE_FIELD = "OK2Invite2CS"

TYPE_SINGLE = "single"
TYPE_SELECT_ALL = "select_all"
TYPE_VALUE = "value"

TYPE_LABELS = {
    TYPE_SINGLE: "Single choice",
    TYPE_SELECT_ALL: "Select all",
    TYPE_VALUE: "Open value",
}
RANK_TYPE_LABEL = "Ranked choice"

CS_REVIEW_COLUMNS = [
    "Question Key",
    "Setup Key",
    "Question Column",
    "Question",
    "Question Type",
    "Question Order",
    "Answer Code",
    "Answer Text",
    "Answer Order",
    "Count",
    "Percentage",
    "Respondents",
]

CONFIG_FIELDS = (
    "enabled",
    "survey_name",
    "survey_id",
    "elvis_database",
    "elvis_table",
    "dictionary_sheet",
    "ok2invite_field",
)

_ID_FIELDS = {"id", "elvisid", "responseid"}
_CODE_RE = re.compile(r"^\s*([^=]+?)\s*=\s*(.+?)\s*$")
_SUFFIX_RE = re.compile(r"^(.*?)[_ ]?(\d+)$")
_YES_VALUES = {"1", "yes", "y", "true", "checked", "selected"}


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------
def _bucket(bucket_name=None):
    return bucket_name or os.getenv("bucket_name")


def _s3():
    from utils import s3_client

    return s3_client


def _project_key(project, suffix):
    safe = str(project).replace(" ", "_").upper()
    return f"{CS_CONFIG_PREFIX}{safe}{suffix}"


def _read_json(bucket_name, key):
    try:
        response = _s3().get_object(Bucket=bucket_name, Key=key)
        data = json.loads(response["Body"].read().decode("utf-8"))
        return data if isinstance(data, dict) else None
    except Exception as e:
        from botocore.exceptions import ClientError

        if isinstance(e, ClientError) and e.response.get("Error", {}).get("Code") == "NoSuchKey":
            return None
        print(f"[CS survey] Could not read s3://{bucket_name}/{key}: {e}")
        return None


def _write_json(bucket_name, key, payload):
    try:
        _s3().put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(payload, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        return True
    except Exception as e:
        print(f"[CS survey] Could not save s3://{bucket_name}/{key}: {e}")
        return False


def normalize_cs_config(data):
    data = data if isinstance(data, dict) else {}
    out = {field: str(data.get(field) or "").strip() for field in CONFIG_FIELDS if field != "enabled"}
    out["enabled"] = bool(data.get("enabled"))
    if not out["dictionary_sheet"]:
        out["dictionary_sheet"] = CS_DEFAULT_DICTIONARY_SHEET
    if not out["ok2invite_field"]:
        out["ok2invite_field"] = CS_DEFAULT_OK2INVITE_FIELD
    return out


def load_cs_survey_config(project, bucket_name=None):
    """Saved CS connection for a project, or None when the project has no second survey."""
    bucket = _bucket(bucket_name)
    if not bucket or not project:
        return None
    data = _read_json(bucket, _project_key(project, CS_CONFIG_SUFFIX))
    return normalize_cs_config(data) if data is not None else None


def save_cs_survey_config(project, config, bucket_name=None):
    bucket = _bucket(bucket_name)
    if not bucket or not project:
        return False
    return _write_json(bucket, _project_key(project, CS_CONFIG_SUFFIX), normalize_cs_config(config))


def is_cs_enabled(config):
    return bool(
        config
        and config.get("enabled")
        and config.get("elvis_database")
        and config.get("elvis_table")
    )


def load_cs_setup(project, bucket_name=None):
    """{"selected": [...setup keys]} or None when the setup was never saved."""
    bucket = _bucket(bucket_name)
    if not bucket or not project:
        return None
    data = _read_json(bucket, _project_key(project, CS_SETUP_SUFFIX))
    if data is None:
        return None
    selected = data.get("selected") or []
    if not isinstance(selected, list):
        selected = []
    return {"selected": [str(x).strip() for x in selected if str(x).strip()]}


def save_cs_setup(project, selected_keys, bucket_name=None):
    bucket = _bucket(bucket_name)
    if not bucket or not project:
        return False
    payload = {"selected": [str(k) for k in selected_keys]}
    return _write_json(bucket, _project_key(project, CS_SETUP_SUFFIX), payload)


def list_details_sheets(details_key, bucket_name=None):
    """Sheet names in the project's Details workbook, or [] when it cannot be read."""
    bucket = _bucket(bucket_name)
    if not bucket or not details_key:
        return []
    try:
        body = _s3().get_object(Bucket=bucket, Key=details_key)["Body"].read()
        with pd.ExcelFile(io.BytesIO(body)) as xl:
            return list(xl.sheet_names)
    except Exception as e:
        print(f"[CS survey] Could not list sheets in {details_key}: {e}")
        return []


def read_cs_dictionary_sheet(details_key, sheet_name, bucket_name=None):
    """Raw dictionary sheet as a DataFrame, or None when the sheet is missing."""
    bucket = _bucket(bucket_name)
    if not bucket or not details_key or not sheet_name:
        return None
    try:
        body = _s3().get_object(Bucket=bucket, Key=details_key)["Body"].read()
        with pd.ExcelFile(io.BytesIO(body)) as xl:
            match = next(
                (s for s in xl.sheet_names if s.strip().lower() == str(sheet_name).strip().lower()),
                None,
            )
            if match is None:
                return None
            return xl.parse(match)
    except Exception as e:
        print(f"[CS survey] Could not read sheet {sheet_name!r} from {details_key}: {e}")
        return None


# ---------------------------------------------------------------------------
# Dictionary parsing
# ---------------------------------------------------------------------------
def _clean(s):
    return (
        str(s).replace("_", "").replace("[", "").replace("]", "")
        .replace(" ", "").replace("#", "").lower()
    )


def norm_code(value):
    """'05', '5', '5.0' -> '5'. Blank-like values -> ''."""
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(value).strip()
    if s.lower() in ("", "nan", "none", "<na>", "null"):
        return ""
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
        return str(f)
    except ValueError:
        return s


def _bracket_label(desc):
    matches = re.findall(r"\[(.*?)\]", str(desc or ""))
    return matches[-1].strip() if matches else ""


def _question_stem(desc):
    return re.sub(r"\s*\[[^\[\]]*\]\s*$", "", str(desc or "")).strip()


def _standardize_dictionary_columns(dd_df):
    aliases = {
        "FIELD NAME": ("FIELD NAME", "FIELD_NAME", "FIELDNAME", "FIELD"),
        "DESCRIPTION": ("DESCRIPTION", "QUESTION", "QUESTION TEXT"),
        "CODE VALUES": ("CODE VALUES", "CODE VALUE", "CODE_VALUES", "CODEVALUES", "CODES", "VALUES"),
        "INDEX": ("INDEX", "ORDER"),
        "LABEL": ("LABEL",),
        "GROUP NAME": ("GROUP NAME", "GROUP_NAME", "GROUPNAME", "GROUP"),
    }
    rename = {}
    for col in dd_df.columns:
        upper = str(col).strip().upper()
        for target, names in aliases.items():
            if upper in names and target not in rename.values():
                rename[col] = target
                break
    return dd_df.rename(columns=rename)


def _text(value):
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def parse_cs_dictionary(dd_df):
    """Turn the CS dictionary sheet into review questions.

    One row per answer code (FIELD NAME repeated) is the expected layout:
      * CODE VALUES like "05 = Very satisfied"  -> single choice, labels from the codes
      * CODE VALUES "Actual Value" on FIELD_01, FIELD_02 ... with [option] text -> select all
      * GROUP NAME filled and CODE VALUES coded -> ranked choice, one question per slot
      * ID / record id fields are skipped
    """
    if dd_df is None or dd_df.empty:
        return []
    dd_df = _standardize_dictionary_columns(dd_df)
    if "FIELD NAME" not in dd_df.columns:
        return []

    fields = OrderedDict()
    for row_no, row in enumerate(dd_df.to_dict("records")):
        name = _text(row.get("FIELD NAME"))
        if not name:
            continue
        info = fields.get(name)
        if info is None:
            order = pd.to_numeric(row.get("INDEX"), errors="coerce")
            info = {
                "field": name,
                "description": _text(row.get("DESCRIPTION")),
                "label": _text(row.get("LABEL")),
                "group": _text(row.get("GROUP NAME")),
                "order": float(order) if pd.notna(order) else float(row_no + 1),
                "codes": [],
            }
            fields[name] = info
        if not info["description"]:
            info["description"] = _text(row.get("DESCRIPTION"))
        if not info["label"]:
            info["label"] = _text(row.get("LABEL"))
        if not info["group"]:
            info["group"] = _text(row.get("GROUP NAME"))
        m = _CODE_RE.match(_text(row.get("CODE VALUES")))
        if m:
            code = norm_code(m.group(1))
            label = m.group(2).strip()
            if code and all(c["code"] != code for c in info["codes"]):
                info["codes"].append({"code": code, "label": label})

    questions = []
    handled = set()

    def title_for(info):
        return info["label"] or info["description"] or info["field"]

    # Group Name rows first: ranked slots when coded, otherwise one select-all question.
    groups = OrderedDict()
    for name, info in fields.items():
        if _clean(name) in _ID_FIELDS:
            handled.add(name)
            continue
        if info["group"]:
            groups.setdefault(info["group"], []).append(info)

    for group, members in groups.items():
        handled.update(m["field"] for m in members)
        if any(m["codes"] for m in members):
            for slot_no, m in enumerate(members, start=1):
                slot = _bracket_label(m["description"]) or str(slot_no)
                questions.append({
                    "key": m["field"],
                    "setup_key": group,
                    "question": title_for(m),
                    "type": TYPE_SINGLE,
                    "type_label": RANK_TYPE_LABEL,
                    "fields": [m["field"]],
                    "options": list(m["codes"] or members[0]["codes"]),
                    "order": m["order"],
                    "slot": slot,
                })
        else:
            first = members[0]
            questions.append({
                "key": group,
                "setup_key": group,
                "question": first["label"] or _question_stem(first["description"]) or group,
                "type": TYPE_SELECT_ALL,
                "type_label": TYPE_LABELS[TYPE_SELECT_ALL],
                "fields": [m["field"] for m in members],
                "options": [
                    {"field": m["field"], "label": _bracket_label(m["description"]) or m["field"]}
                    for m in members
                ],
                "order": first["order"],
                "slot": "",
            })

    # Ungrouped "Actual Value" fields that share a stem and question text -> select all.
    suffix_groups = OrderedDict()
    for name, info in fields.items():
        if name in handled or info["codes"]:
            continue
        m = _SUFFIX_RE.match(name)
        if m and _bracket_label(info["description"]):
            stem_key = (m.group(1).rstrip("_ "), _question_stem(info["description"]).lower())
            suffix_groups.setdefault(stem_key, []).append(info)

    for (prefix, _), members in suffix_groups.items():
        if len(members) < 2:
            continue
        handled.update(m["field"] for m in members)
        first = members[0]
        questions.append({
            "key": prefix,
            "setup_key": prefix,
            "question": first["label"] or _question_stem(first["description"]) or prefix,
            "type": TYPE_SELECT_ALL,
            "type_label": TYPE_LABELS[TYPE_SELECT_ALL],
            "fields": [m["field"] for m in members],
            "options": [
                {"field": m["field"], "label": _bracket_label(m["description"]) or m["field"]}
                for m in members
            ],
            "order": first["order"],
            "slot": "",
        })

    for name, info in fields.items():
        if name in handled:
            continue
        qtype = TYPE_SINGLE if info["codes"] else TYPE_VALUE
        questions.append({
            "key": name,
            "setup_key": name,
            "question": title_for(info),
            "type": qtype,
            "type_label": TYPE_LABELS[qtype],
            "fields": [name],
            "options": list(info["codes"]),
            "order": info["order"],
            "slot": "",
        })

    questions.sort(key=lambda q: q["order"])
    return questions


def setup_items(questions):
    """One row per setup choice; ranked slots collapse into their group."""
    items = OrderedDict()
    for q in questions:
        item = items.get(q["setup_key"])
        if item is None:
            question = q["question"]
            if q["type_label"] == RANK_TYPE_LABEL:
                question = _question_stem(question) or question
            items[q["setup_key"]] = {
                "setup_key": q["setup_key"],
                "question": question,
                "type_label": q["type_label"],
                "fields": list(q["fields"]),
                "options": len(q["options"]),
                "slots": [q["slot"]] if q["slot"] else [],
            }
        else:
            item["fields"].extend(f for f in q["fields"] if f not in item["fields"])
            if q["slot"]:
                item["slots"].append(q["slot"])
    return list(items.values())


# ---------------------------------------------------------------------------
# Survey data -> review rows
# ---------------------------------------------------------------------------
def resolve_column(columns, field):
    """Match a dictionary FIELD NAME to a data column (exact, case, punctuation, then ...Code)."""
    cols = [str(c) for c in columns]
    if field in cols:
        return field
    lower = {c.lower(): c for c in cols}
    if field.lower() in lower:
        return lower[field.lower()]
    cleaned = {_clean(c): c for c in cols}
    key = _clean(field)
    if key in cleaned:
        return cleaned[key]
    if key + "code" in cleaned:
        return cleaned[key + "code"]
    return None


def field_match_report(questions, columns):
    """{field: resolved column or None} for every field used by the questions."""
    if columns is None:
        return {}
    out = {}
    for q in questions:
        for f in q["fields"]:
            out[f] = resolve_column(columns, f)
    return out


def _is_checked(value, label):
    s = norm_code(value).lower()
    if not s:
        return False
    if s in _YES_VALUES:
        return True
    return bool(label) and s == str(label).strip().lower()


def prepare_cs_survey_frame(df):
    """Drop the Elvis header row and supervisor-deleted records."""
    if df is None or df.empty:
        return df
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    id_col = resolve_column(df.columns, "id")
    if id_col is not None and len(df) and pd.isna(pd.to_numeric(df[id_col].iloc[0], errors="coerce")):
        df = df.iloc[1:].reset_index(drop=True)
    status_col = resolve_column(df.columns, "ElvisStatus")
    if status_col is not None:
        df = df[df[status_col].astype(str).str.strip().str.lower() != "delete"]
    return df.reset_index(drop=True)


def build_cs_review(df, questions):
    rows = []
    if df is None or df.empty:
        return pd.DataFrame(columns=CS_REVIEW_COLUMNS)

    for q_order, q in enumerate(questions, start=1):
        base = {
            "Question Key": q["key"],
            "Setup Key": q["setup_key"],
            "Question": q["question"],
            "Question Type": q["type_label"],
            "Question Order": q_order,
        }

        if q["type"] == TYPE_SELECT_ALL:
            resolved = [(opt, resolve_column(df.columns, opt["field"])) for opt in q["options"]]
            resolved = [(opt, col) for opt, col in resolved if col is not None]
            if not resolved:
                print(f"[CS survey] No columns found for {q['key']} - skipped.")
                continue
            checked = pd.DataFrame({
                opt["field"]: df[col].map(lambda v, lab=opt["label"]: _is_checked(v, lab))
                for opt, col in resolved
            })
            answered = pd.DataFrame({
                opt["field"]: df[col].map(norm_code) != "" for opt, col in resolved
            }).any(axis=1)
            respondents = int(answered.sum())
            for a_order, (opt, col) in enumerate(resolved, start=1):
                count = int(checked[opt["field"]].sum())
                rows.append({
                    **base,
                    "Question Column": ", ".join(c for _, c in resolved),
                    "Answer Code": opt["field"],
                    "Answer Text": opt["label"],
                    "Answer Order": a_order,
                    "Count": count,
                    "Percentage": round(count / respondents * 100, 2) if respondents else 0.0,
                    "Respondents": respondents,
                })
            continue

        col = resolve_column(df.columns, q["fields"][0])
        if col is None:
            print(f"[CS survey] Column {q['fields'][0]!r} not found in CS survey data - skipped.")
            continue
        values = df[col].map(norm_code)
        values = values[values != ""]
        respondents = int(len(values))

        if q["type"] == TYPE_SINGLE and q["options"]:
            by_code = {o["code"]: o["label"] for o in q["options"]}
            by_label = {o["label"].strip().lower(): o["label"] for o in q["options"]}
            mapped = values.map(
                lambda v: by_code.get(v) or by_label.get(v.lower()) or v
            )
            counts = mapped.value_counts()
            answers = [(o["code"], o["label"]) for o in q["options"]]
            known = {label for _, label in answers}
            answers += [(label, label) for label in counts.index if label not in known]
        else:
            counts = values.value_counts()
            answers = [(v, v) for v in counts.index]

        for a_order, (code, label) in enumerate(answers, start=1):
            count = int(counts.get(label, 0))
            rows.append({
                **base,
                "Question Column": col,
                "Answer Code": code,
                "Answer Text": label,
                "Answer Order": a_order,
                "Count": count,
                "Percentage": round(count / respondents * 100, 2) if respondents else 0.0,
                "Respondents": respondents,
            })

    if not rows:
        return pd.DataFrame(columns=CS_REVIEW_COLUMNS)
    out = pd.DataFrame(rows)[CS_REVIEW_COLUMNS]
    for col in ("Question Key", "Setup Key", "Question Column", "Question", "Question Type", "Answer Code", "Answer Text"):
        out[col] = out[col].astype(str)
    return out


def fetch_cs_survey_frame(config):
    """CS survey records from MySQL, or None when the connection or table is unavailable."""
    from utils import fetch_mysql_tables

    loaded = fetch_mysql_tables(config["elvis_database"], [config["elvis_table"]])
    if not loaded:
        return None
    return prepare_cs_survey_frame(loaded[0])


def build_cs_review_for_project(project, project_config, bucket_name=None):
    """Review rows for a project's CS survey, or None when the project has no usable CS setup."""
    config = load_cs_survey_config(project, bucket_name)
    if not is_cs_enabled(config):
        return None
    setup = load_cs_setup(project, bucket_name)
    if not setup or not setup["selected"]:
        print(f"[CS survey] {project}: CS setup has no questions selected - CS review not refreshed.")
        return None
    details_key = ((project_config or {}).get("files") or {}).get("details")
    dd_df = read_cs_dictionary_sheet(details_key, config["dictionary_sheet"], bucket_name)
    if dd_df is None:
        print(f"[CS survey] {project}: sheet {config['dictionary_sheet']!r} not found in Details file.")
        return None
    selected = set(setup["selected"])
    questions = [q for q in parse_cs_dictionary(dd_df) if q["setup_key"] in selected]
    if not questions:
        return None
    survey_df = fetch_cs_survey_frame(config)
    if survey_df is None:
        print(f"[CS survey] {project}: could not load {config['elvis_database']}.{config['elvis_table']}.")
        return None
    review = build_cs_review(survey_df, questions)
    print(f"[CS survey] {project}: {len(survey_df)} CS records, {review['Question Key'].nunique() if not review.empty else 0} questions.")
    return review


def alias_ok2invite_column(df, field_name):
    """Expose a project-specific invite field under the default OK2Invite2CS name for the reports."""
    if df is None or not field_name or _clean(field_name) == _clean(CS_DEFAULT_OK2INVITE_FIELD):
        return df
    col = resolve_column(df.columns, field_name)
    if col is None or resolve_column(df.columns, CS_DEFAULT_OK2INVITE_FIELD) is not None:
        return df
    df = df.copy()
    df[CS_DEFAULT_OK2INVITE_FIELD] = df[col]
    return df
