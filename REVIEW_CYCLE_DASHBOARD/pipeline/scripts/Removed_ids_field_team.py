import pandas as pd
from datetime import date

project_name = "LACMTA_FEEDER"

# ==============================
# Load files
# ==============================
df1 = pd.read_csv("elvis_transit_ls6_733524_export_odbc.csv", dtype=str)
df1["id"] = df1["id"].astype(str)
df2 = pd.read_excel(
    "LACMTA_FEEDER_2025_KINGElvis.xlsx",
    sheet_name="Elvis_Review",
)
df2["elvis_id"] = df2["elvis_id"].astype(str)
today_date = date.today()
today_date = "".join(str(today_date).split("-"))

# ==============================
# Rename headers in first file
# ==============================
mapping_file = "request_20250708_ls6tols2-headers.xlsx"
sheet_name = "Example"

header_df = pd.read_excel(mapping_file, sheet_name=sheet_name)
header_mapping = dict(zip(header_df["Headers-ls6"], header_df["FormattedHeader-ls2"]))
df1 = df1.rename(columns=header_mapping)
print("Renamed df columns:")
print(df1.columns.tolist())

# ==============================
# Merge on ID
# ==============================
merged_df = df1.merge(
    df2,
    left_on="id",
    right_on="elvis_id",
    how="left",
    suffixes=("_db", "_review"),
)

print(f"Output of merged with {len(df1)} records.")

record_id = "id_db" if "id_db" in merged_df.columns else "id"
route_col = "ROUTE_SURVEYED_db" if "ROUTE_SURVEYED_db" in merged_df.columns else "ROUTE_SURVEYED"
route_code_col = (
    "ROUTE_SURVEYEDCode_db" if "ROUTE_SURVEYEDCode_db" in merged_df.columns else "ROUTE_SURVEYEDCode"
)
interv_col = "INTERV_INIT_db" if "INTERV_INIT_db" in merged_df.columns else "INTERV_INIT"
elvis_status_col = "ElvisStatus_db" if "ElvisStatus_db" in merged_df.columns else "ElvisStatus"


def _series(name: str) -> pd.Series:
    """Pick review column, then unsuffixed, then _db."""
    for candidate in (name, f"{name}_review", f"{name}_db"):
        if candidate in merged_df.columns:
            return merged_df[candidate]
    return pd.Series([""] * len(merged_df), index=merged_df.index)


def _db_series(name: str) -> pd.Series:
    for candidate in (f"{name}_db", name):
        if candidate in merged_df.columns:
            return merged_df[candidate]
    return pd.Series([""] * len(merged_df), index=merged_df.index)


def _review_series(name: str) -> pd.Series:
    for candidate in (f"{name}_review", name):
        if candidate in merged_df.columns:
            return merged_df[candidate]
    return pd.Series([""] * len(merged_df), index=merged_df.index)


def _valid_survey_mask(frame: pd.DataFrame) -> pd.Series:
    review_ok = (frame["INTERV_INIT_review"] != "999") & (
        frame["HAVE_5_MIN_FOR_SURVECode_review"] == "1"
    )
    db_ok = (frame["INTERV_INIT_db"] != "999") & (frame["HAVE_5_MIN_FOR_SURVECode_db"] == "1")
    return review_ok | db_ok


# Fill NaNs so filters work
merged_df["Final_Usage"] = _series("Final_Usage").fillna("").astype(str).str.strip().str.lower()
merged_df["ElvisStatus"] = _series("ElvisStatus").fillna("").astype(str).str.strip().str.lower()
merged_df["INTERV_INIT_review"] = _review_series("INTERV_INIT").fillna("0").astype(str)
merged_df["HAVE_5_MIN_FOR_SURVECode_review"] = _review_series("HAVE_5_MIN_FOR_SURVECode").fillna("0").astype(str)
merged_df["INTERV_INIT_db"] = _db_series("INTERV_INIT").fillna("0").astype(str)
merged_df["HAVE_5_MIN_FOR_SURVECode_db"] = _db_series("HAVE_5_MIN_FOR_SURVECode").fillna("0").astype(str)

valid_survey = _valid_survey_mask(merged_df)

remove_or_delete_mask = (
    (merged_df["Final_Usage"] == "remove") | (merged_df["ElvisStatus"] == "delete")
) & valid_survey

filtered_df = merged_df[remove_or_delete_mask].copy()

# ==============================
# Remove_or_Delete sheet
# ==============================
output_df = filtered_df[
    [
        record_id,
        route_col,
        route_code_col,
        interv_col,
        "FINAL_REVIEWER",
        "Final_Usage",
        "REASON FOR REMOVAL",
        "REASON FOR REMOVAL [Other]",
        elvis_status_col,
    ]
].rename(
    columns={
        record_id: "ID",
        route_col: "ROUTE_SURVEYED",
        route_code_col: "ROUTE_SURVEYEDCode",
        interv_col: "INTERV_INIT",
        elvis_status_col: "ELVIS_STATUS",
        "Final_Usage": "FINAL_USAGE",
    }
)

output_df["FIELD MANAGER NAME"] = ""
output_df["UPDATED STATUS NEEDED"] = ""
output_df["COMMENTS"] = ""

output_df = output_df[
    [
        "ID",
        "ROUTE_SURVEYED",
        "ROUTE_SURVEYEDCode",
        "INTERV_INIT",
        "FINAL_REVIEWER",
        "FINAL_USAGE",
        "REASON FOR REMOVAL",
        "REASON FOR REMOVAL [Other]",
        "ELVIS_STATUS",
        "FIELD MANAGER NAME",
        "UPDATED STATUS NEEDED",
        "COMMENTS",
    ]
]

# ==============================
# Supervisor_Remark sheet
# ==============================
remark_col = next(
    (col for col in ("ElvisRemark_db", "ElvisRemark", "ELVIS_COMMENT") if col in merged_df.columns),
    None,
)
supervisor_df = pd.DataFrame(
    columns=["ID", "FINAL_REVIEWER", "FINAL_USAGE", "ElvisRemark", "COMMENTS"]
)

if remark_col:
    remark_text = merged_df[remark_col].fillna("").astype(str).str.strip()
    removed_ids = set(filtered_df[record_id].astype(str))
    supervisor_mask = (
        (merged_df["Final_Usage"] == "use")
        & valid_survey
        & remark_text.ne("")
        & ~remark_text.str.contains(r"^ElvisRemark\s+Elvis Remark:", case=False, regex=True)
        & ~merged_df[record_id].astype(str).isin(removed_ids)
    )
    supervisor_rows = merged_df[supervisor_mask].copy()
    if not supervisor_rows.empty:
        supervisor_df = pd.DataFrame(
            {
                "ID": supervisor_rows[record_id],
                "FINAL_REVIEWER": supervisor_rows["FINAL_REVIEWER"].fillna("").astype(str),
                "FINAL_USAGE": supervisor_rows["Final_Usage"].fillna("").astype(str).str.lower(),
                "ElvisRemark": supervisor_rows[remark_col].fillna("").astype(str).str.strip(),
                "COMMENTS": "",
            }
        )

# ==============================
# Export to Excel (two sheets)
# ==============================
output_path = f"{project_name}_Removed_or_Deleted_Records_by_{today_date}.xlsx"
with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    output_df.to_excel(writer, sheet_name="Remove_or_Delete", index=False)
    supervisor_df.to_excel(writer, sheet_name="Supervisor_Remark", index=False)

print(
    f"Output generated: {output_path} "
    f"({len(output_df)} remove/delete, {len(supervisor_df)} supervisor remarks)."
)
