"""
Refusal / No Answer (Blanks) report builder — used by refresh pipeline and dashboard.
"""
import pandas as pd

from automated_sync_flow_utils import _resolve_elvis_column_name, normalize_survey_columns_for_reports


def _is_blank_value(val):
    if pd.isna(val):
        return True
    s = str(val).strip()
    if not s:
        return True
    if s.lower() in ("nan", "none", "<na>"):
        return True
    return False


def _preprocess_elvis_for_blanks(elvis_df):
    """Keep only useable survey records: HAVE_5_MIN = Yes and non-test (INTERV_INIT != 999)."""
    if elvis_df is None or elvis_df.empty:
        return elvis_df
    df = normalize_survey_columns_for_reports(elvis_df.copy())
    if "INTERV_INIT" in df.columns:
        df["INTERV_INIT"] = df["INTERV_INIT"].astype(str).str.strip()
        df = df[df["INTERV_INIT"] != "999"]
    if "HAVE_5_MIN_FOR_SURVECode" in df.columns:
        df = df[df["HAVE_5_MIN_FOR_SURVECode"] == "1"]
    if len(df) > 1:
        df = df.iloc[1:].copy()
    return df.reset_index(drop=True)


def _survey_date_series(df):
    if "LocalTime" in df.columns:
        return pd.to_datetime(df["LocalTime"], errors="coerce").dt.date
    if "DATE_SUBMITTED" in df.columns:
        return pd.to_datetime(df["DATE_SUBMITTED"], errors="coerce").dt.date
    return pd.Series([pd.NaT] * len(df), index=df.index)


def _date_col_label(d):
    if d is None or (isinstance(d, float) and pd.isna(d)):
        return None
    try:
        return pd.Timestamp(d).strftime("%Y %m %d")
    except Exception:
        return None


def _iter_report_columns(demographic_config):
    if not demographic_config:
        return
    qd = demographic_config.get("question_dict") or {}
    multi_names = set(demographic_config.get("multi_select_field_names") or [])
    goc = demographic_config.get("group_option_columns") or {}

    for field_key in qd.keys():
        fk = str(field_key).strip()
        if not fk:
            continue
        if fk in multi_names or fk in goc:
            colmap = None
            for gk, cm in goc.items():
                if str(gk).strip().lower() == fk.lower():
                    colmap = cm
                    break
            if not colmap and fk in goc:
                colmap = goc[fk]
            if colmap:
                for _label, physical_col in colmap.items():
                    disp = str(physical_col).strip()
                    yield fk, physical_col, disp or physical_col
            else:
                yield fk, None, fk
        else:
            yield fk, None, fk


def create_refusal_blanks_report(elvis_df, demographic_config, project_name=None):
    """
    Build totals and daily blank-% tables for Refusal/No Answer analysis.

    Returns:
        (totals_df, daily_df)
    """
    _ = project_name
    empty_totals = pd.DataFrame(
        columns=["COLUMN_NAME", "Alert", "TOTAL_BLANKS", "TOTAL_PCT"]
    )
    empty_daily = pd.DataFrame()

    if demographic_config is None:
        return empty_totals.copy(), empty_daily.copy()
    qd = demographic_config.get("question_dict") or {}
    if not qd:
        return empty_totals.copy(), empty_daily.copy()

    df = _preprocess_elvis_for_blanks(elvis_df)
    if df is None or df.empty:
        return empty_totals.copy(), empty_daily.copy()

    alert_fields = set(demographic_config.get("alert_field_names") or [])
    survey_dates = _survey_date_series(df)
    unique_dates = sorted({d for d in survey_dates.dropna().unique() if d is not None})

    total_n = len(df)
    rows_meta = list(_iter_report_columns(demographic_config))
    if not rows_meta:
        return empty_totals.copy(), empty_daily.copy()

    totals_rows = []
    daily_rows = []
    daily_date_labels = []

    for field_key, _hint_col, display_name in rows_meta:
        resolved = _resolve_elvis_column_name(df, _hint_col or field_key)
        if resolved is None or resolved not in df.columns:
            continue

        col_series = df[resolved]
        blank_mask = col_series.map(_is_blank_value)
        total_blanks = int(blank_mask.sum())
        total_pct = round((total_blanks / total_n * 100), 2) if total_n > 0 else 0.0
        alert_label = "Alert" if field_key in alert_fields else ""

        totals_rows.append(
            {
                "COLUMN_NAME": display_name,
                "Alert": alert_label,
                "TOTAL_BLANKS": int(total_blanks),
                "TOTAL_PCT": float(total_pct),
            }
        )

        daily_row = {
            "COLUMN_NAME": display_name,
            "Alert": alert_label,
            "TOTAL_BLANKS": int(total_blanks),
            "TOTAL_PCT": float(total_pct),
        }

        for d in unique_dates:
            label = _date_col_label(d)
            if not label:
                continue
            day_mask = survey_dates == d
            day_n = int(day_mask.sum())
            day_blanks = int(blank_mask[day_mask].sum()) if day_n > 0 else 0
            day_pct = round((day_blanks / day_n * 100), 2) if day_n > 0 else 0.0
            daily_row[f"{label}_BLANKS"] = day_blanks
            daily_row[f"{label}_PCT"] = day_pct
            if label not in daily_date_labels:
                daily_date_labels.append(label)

        daily_rows.append(daily_row)

    if not totals_rows:
        return empty_totals.copy(), empty_daily.copy()

    totals_df = pd.DataFrame(totals_rows)
    totals_df = totals_df.sort_values("TOTAL_PCT", ascending=False).reset_index(drop=True)

    daily_df = pd.DataFrame(daily_rows)
    n_row = {"COLUMN_NAME": "N", "Alert": "", "TOTAL_BLANKS": "", "TOTAL_PCT": ""}
    for d in unique_dates:
        label = _date_col_label(d)
        if not label:
            continue
        day_n = int((survey_dates == d).sum())
        n_row[f"{label}_BLANKS"] = day_n
        n_row[f"{label}_PCT"] = ""

    daily_df = pd.concat([daily_df, pd.DataFrame([n_row])], ignore_index=True)
    return totals_df, daily_df
