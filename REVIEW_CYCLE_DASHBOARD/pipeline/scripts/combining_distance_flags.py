import pandas as pd
import warnings
import numpy as np
import copy
from datetime import date

warnings.filterwarnings("ignore")

project_name='PARK_CITY'

# file_name='SEATTLE_WA_KINGElvis.xlsx'
# file_name='DENVER_OB_KINGElvis.xlsx'
# file_name='BART_CA_KINGElvis.xlsx'
# file_name='VTA_CA_OB_KINGElvis.xlsx'
# file_name='ANCHORAGE_AK_KINGElvis.xlsx'
# file_name='Buffalo_NY_OB_KINGElvis.xlsx'
# file_name='CARTA_OB_KINGElvis.xlsx'
# file_name='CulverCity_CA_KINGElvis.xlsx'
# file_name='MUNI_CA_KINGElvis.xlsx'
# file_name='BUFFALO_RAIL_KINGElvis.xlsx'
# file_name='Tucson_az_2025_KINGElvis.xlsx'
# file_name='KCATA_2025_KINGElvis.xlsx'
# file_name='ACT_2025_KINGElvis.xlsx'
# file_name='SALEM_OR_2025_KINGElvis.xlsx'
# file_name='LACMTA_FEEDER_2025_KINGElvis.xlsx'
# file_name='INDYGO_BRT_2026_KINGElvis.xlsx'
file_name='PARK_CITY_UT_2026_KINGElvis.xlsx'
today_date = date.today()
today_date=''.join(str(today_date).split('-'))

# elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')
elvis_df=pd.read_excel(file_name,sheet_name='Elvis_Review')

try:
    traditional_df=pd.read_csv(f'reviewtool_20260401_{project_name}_TraditionalTransferFlags.csv')
except:
    traditional_df=pd.DataFrame()
try:
    od_df=pd.read_csv(f'reviewtool_20260401_{project_name}_OD_Distance_Checks.csv')
except:
    od_df=pd.DataFrame()
try:
    transfer_df=pd.read_csv(f'reviewtool_20260401_{project_name}_Distance_Transfer_Flags.csv')
except:
    transfer_df=pd.DataFrame()
try:
    stoplist_df = pd.read_excel(f'{project_name}_flagged_stops_20260401.xlsx')
except:
    stoplist_df = pd.DataFrame()

# recovery_df=pd.read_excel('COTA_survey_recovery_2023-12-06.xlsx', sheet_name='_(F0) SURVEY RECOVERY')

# get data where Final_Usage== 'use'
# elvis_df=elvis_df[elvis_df['Final_Usage'].str.lower()=='use']

def check_all_characters_present(df, columns_to_check):
    # Function to clean a string by removing underscores and square brackets and converting to lowercase
    def clean_string(s):
        return s.replace('_', '').replace('[', '').replace(']', '').replace(' ','').replace('#','').lower()

    # Clean and convert all column names in df to lowercase for case-insensitive comparison
    df_columns_lower = [clean_string(column) for column in df.columns]

    # Clean and convert the columns_to_check list to lowercase for case-insensitive comparison
    columns_to_check_lower = [clean_string(column) for column in columns_to_check]

    # Use a list comprehension to filter columns
    matching_columns = [column for column in df.columns if clean_string(column) in columns_to_check_lower]

    return matching_columns


if not traditional_df.empty:
    # merge Traditional Transfer Checks with KingELvis Data
    merged_df = pd.merge(elvis_df, traditional_df['id'], on='id', how='left', indicator=True)
    # Create a new column 'Traditional_Check' based on the merge indicator
    merged_df['Traditional_Check'] = (merged_df['_merge'] == 'both').astype(int)
    # Drop the indicator column and display the resulting DataFrame
    merged_df = merged_df.drop(columns=['_merge'])
else:
    merged_df=copy.deepcopy(elvis_df)
    merged_df['Traditional_Check']=0

if not stoplist_df.empty:
    # merge StopListValidationChecks with Merged Data
    merged_df = pd.merge(merged_df, stoplist_df['id'], on='id', how='left', indicator=True)
    # Create a new column 'StopListValidation_Check' based on the merge indicator
    merged_df['StopListValidation_Check'] = (merged_df['_merge'] == 'both').astype(int)
    merged_df = merged_df.drop(columns=['_merge'])
else:
    merged_df['StopListValidation_Check'] = 0

# # merge Recovery Transfer Checks with KingELvis Data
# merged_df = pd.merge(merged_df, recovery_df['id'], on='id', how='left', indicator=True)
# # Create a new column 'Traditional_Check' based on the merge indicator
# merged_df['Recovery_Check'] = (merged_df['_merge'] == 'both').astype(int)
# # Drop the indicator column and display the resulting DataFrame
# merged_df = merged_df.drop(columns=['_merge'])

if not od_df.empty:
    # merge OD Distance Transfer Checks with Merged Data of Traditional Checks
    merged_df = pd.merge(merged_df, od_df['id'], on='id', how='left', indicator=True)
    # Create a new column 'OD_Distance_Check' based on the merge indicator
    merged_df['OD_Distance_Check'] = (merged_df['_merge'] == 'both').astype(int)
    # Drop the indicator column and display the resulting DataFrame
    merged_df = merged_df.drop(columns=['_merge'])
else:
    merged_df['OD_Distance_Check']=0

if not transfer_df.empty:
    # merge TRansfer Distance Checks with Merged Data of Traditional and OD Distance Checks
    merged_df = pd.merge(merged_df, transfer_df['id'], on='id', how='left', indicator=True)
    # Create a new column 'Transfer_Distance_Check' based on the merge indicator
    merged_df['Transfer_Distance_Check'] = (merged_df['_merge'] == 'both').astype(int)
    # Drop the indicator column and display the resulting DataFrame
    merged_df = merged_df.drop(columns=['_merge'])
else:
    merged_df['Transfer_Distance_Check']=0

# merged_df=merged_df[(merged_df['Transfer_Distance_Check']==1)  | (merged_df['OD_Distance_Check']==1) | (merged_df['Traditional_Check']==1)]

review_columns_check=['2xreviewcheck','2X_REVIEW_CHECK.1']

review_columns=check_all_characters_present(elvis_df,review_columns_check)

if review_columns:
    # create new column where all checks are 1
    merged_df.drop(columns=[*review_columns],inplace=True)
    merged_df['2X_REVIEW_CHECK'] = np.where(
    merged_df[['Transfer_Distance_Check','OD_Distance_Check','Traditional_Check','StopListValidation_Check']].any(axis=1), 1, 0
)
    # merged_df['2X_REVIEW_CHECK']=np.where(merged_df[['Transfer_Distance_Check','OD_Distance_Check','Traditional_Check','Recovery_Check']].any(axis=1),1,0)
else:
    merged_df['2X_REVIEW_CHECK'] = np.where(
    merged_df[['Transfer_Distance_Check','OD_Distance_Check','Traditional_Check','StopListValidation_Check']].any(axis=1), 1, 0
)
    # create new empty columns
    merged_df['2x_REVIEWED_BY']=None
    merged_df['2x_REVIEWED_FLAG']=None
    merged_df['ADMIN_APPROVED']=None

# merged_df[merged_df['2X_REVIEW_CHECK']==1].to_csv('Merged Checks.csv',index=False)

merged_df.drop_duplicates(subset=['id'],inplace=True)
print(merged_df.columns.tolist())

# ======================
# ENHANCED ANALYSIS SECTION
# ======================

import pandas as pd
import matplotlib.pyplot as plt
import base64
import os
from io import BytesIO
from textwrap import dedent

# === Helper Functions ===
def fig_to_html(fig):
    """Convert a Matplotlib figure to an HTML <img> tag."""
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    encoded = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    return f'<img src="data:image/png;base64,{encoded}" />'

def fig_to_base64():
    buf = BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', dpi=100)
    buf.seek(0)
    img_str = base64.b64encode(buf.read()).decode()
    plt.close()
    return img_str

# === Temporal Trends ===
def generate_temporal_trend(df, date_col):
    if not date_col or date_col not in df.columns:
        return "<p>No date column found for temporal analysis</p>"

    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    weekly_flags = df.resample('W', on=date_col)['SUM_ALL_CHECKS'].mean()
    weekly_volume = df.resample('W', on=date_col).size()

    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax1.plot(weekly_flags.index, weekly_flags, color='red', marker='o', label='Flag Rate')
    ax1.set_ylabel('Flag Rate', color='red')
    ax1.set_ylim(0, 1)
    ax1.tick_params(axis='y', labelcolor='red')

    ax2 = ax1.twinx()
    ax2.bar(weekly_volume.index, weekly_volume, color='blue', alpha=0.3, label='Survey Volume')
    ax2.set_ylabel('Survey Volume', color='blue')
    ax2.tick_params(axis='y', labelcolor='blue')

    ax1.set_title('Weekly Flag Rate & Survey Volume')
    fig.tight_layout()
    return fig_to_html(fig)

# === Root Cause Trend ===
def generate_root_cause_trend(df, date_col, flag_cols):
    if date_col not in df.columns:
        return "<p>No date column found for root cause trend</p>"

    # Ensure datetime
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

    # Drop rows with invalid dates
    df_valid = df.dropna(subset=[date_col]).copy()

    # Ensure flags are numeric
    df_valid[flag_cols] = df_valid[flag_cols].apply(pd.to_numeric, errors='coerce').fillna(0)

    # Resample weekly and calculate mean
    weekly_causes = df_valid.resample('W', on=date_col)[flag_cols].mean().fillna(0)

    # Plot
    fig, ax = plt.subplots(figsize=(10, 6))
    for col in weekly_causes.columns:
        ax.plot(weekly_causes.index, weekly_causes[col], marker='o', label=col)

    ax.set_title("Root Cause Trends Over Time")
    ax.set_xlabel("Week")
    ax.set_ylabel("Average Flag Rate")
    ax.legend()
    ax.grid(True)
    plt.tight_layout()
    return fig_to_html(fig)


# === Flag Table ===
def generate_flag_table(flagged_df, flag_cols):
    numeric_flags = flagged_df[flag_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
    counts = numeric_flags.sum().sort_values(ascending=False)
    html_table = counts.reset_index()
    html_table.columns = ['Flag', 'Count']
    return html_table.to_html(index=False)

# === Co-occurrence Matrix ===
def generate_cooccurrence_matrix(df, flag_cols):
    numeric_flags = df[flag_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
    corr = numeric_flags.corr()
    fig, ax = plt.subplots(figsize=(8, 6))
    cax = ax.matshow(corr, cmap='coolwarm')
    fig.colorbar(cax)
    ax.set_xticks(range(len(flag_cols)))
    ax.set_xticklabels(flag_cols, rotation=90)
    ax.set_yticks(range(len(flag_cols)))
    ax.set_yticklabels(flag_cols)
    ax.set_title('Flag Co-occurrence Matrix')
    plt.tight_layout()
    return fig_to_html(fig)

# === Emerging Issues ===
def generate_emerging_issues(df, date_col, flag_cols):
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    recent_threshold = df[date_col].max() - pd.Timedelta(days=7)
    df['recent'] = df[date_col] >= recent_threshold
    numeric_flags = df[flag_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
    recent_flags = numeric_flags[df['recent']]
    avg_recent_flags = recent_flags.mean().sort_values(ascending=False)
    html_table = avg_recent_flags.to_frame(name="Recent Avg Rate").to_html()
    return html_table

# === Deep Dive per Flag Type ===
def generate_flag_insights(df):
    insights_html = ""
    
    # OD Distance Check
    od_cols = [col for col in df.columns if 'O_B_Dist_Check' in col or 'A_D_Dist_Check' in col or 'O_D_Dist_Check' in col or 'B_A_Dist_Check' in col]
    if od_cols:
        od_flags = df[od_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
        top_surveyors = df.groupby('FINAL_REVIEWER')[od_cols].sum().sum(axis=1).sort_values(ascending=False).head(5)
        insights_html += "<h3>OD Distance Check Insights</h3>"
        insights_html += top_surveyors.to_frame(name="OD Flags Count").to_html()
    
    # Traditional Transfer Check
    trad_cols = [
        col for col in df.columns
        if ("Checkall" in col or "Check" in col) and "GoodTransfer" in col
    ]

    # print(repr(trad_cols))
    if trad_cols:
        
        trad_flags = df[trad_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
        top_routes = df.groupby('ROUTE_SURVEYEDCode')[trad_cols].sum().sum(axis=1).sort_values(ascending=False).head(5)
        insights_html += "<h3>Traditional Transfer Check Insights</h3>"
        insights_html += top_routes.to_frame(name="Traditional Flags Count").to_html()
    
    # Transfer Distance Check
    trans_cols = [col for col in df.columns if 'Transfer' in col and 'Distance' in col]
    if trans_cols:
        trans_flags = df[trans_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
        top_transfers = df.groupby('FINAL_REVIEWER')[trans_cols].sum().sum(axis=1).sort_values(ascending=False).head(5)
        insights_html += "<h3>Transfer Distance Check Insights</h3>"
        insights_html += top_transfers.to_frame(name="Transfer Distance Flags Count").to_html()
    
    # Stop Validation Check
    stop_cols = [col for col in df.columns if 'STOP' in col and 'LAT' in col or 'LONG' in col]
    if stop_cols:
        stop_flags = df[stop_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
        top_stops = df.groupby('FINAL_REVIEWER')[stop_cols].sum().sum(axis=1).sort_values(ascending=False).head(5)
        insights_html += "<h3>Stop Validation Check Insights</h3>"
        insights_html += top_stops.to_frame(name="Stop Validation Flags Count").to_html()
    
    return insights_html

# === Main Report ===
# === Main Report with OD Distance Insights ===
def generate_analysis_report(merged_df, project_name, today_date):
    # Keep only the selected flags
    flag_columns = ['2X_REVIEW_CHECK','Traditional_Check','OD_Distance_Check','Transfer_Distance_Check','StopListValidation_Check']
    
    # Ensure SUM_ALL_CHECKS
    merged_df['SUM_ALL_CHECKS'] = merged_df[flag_columns].any(axis=1).astype(int)

    # Find date column
    date_col = next((col for col in merged_df.columns if col.lower().replace("_","")=="elvisdate"), None)
    if date_col:
        merged_df[date_col] = pd.to_datetime(merged_df[date_col], errors='coerce')
    else:
        print("⚠️ No date column found. Skipping temporal & emerging issue analysis.")

    flagged = merged_df[merged_df['SUM_ALL_CHECKS']==1]

    # --- Stop Validation Insights ---
    stop_csv_file = f'{project_name}_flagged_stops_20260401.xlsx'
    if os.path.exists(stop_csv_file):
        stop_df = pd.read_excel(stop_csv_file)
        
        def get_stop_insights(df):
            html = "<h3>🛑 Stop Validation Insights</h3>"
            
            # Basic statistics
            total_flagged = len(df)
            if total_flagged == 0:
                return html + "<p>No stop validation flags found</p>"
            
            avg_distance = df['DISTANCE_MILES'].mean()
            max_distance = df['DISTANCE_MILES'].max()
            
            html += f"<p><strong>Total flagged stops:</strong> {total_flagged:,}</p>"
            html += f"<p><strong>Average distance:</strong> {avg_distance:.2f} miles</p>"
            html += f"<p><strong>Maximum distance:</strong> {max_distance:.2f} miles</p>"
            
            # Top routes with stop issues
            if 'ROUTE_SURVEYEDCode' in df.columns:
                route_counts = df['ROUTE_SURVEYEDCode'].value_counts().head(10)
                html += "<h4>Top Routes with Stop Validation Issues</h4>"
                html += "<table><tr><th>Route</th><th>Flag Count</th></tr>"
                for route, count in route_counts.items():
                    html += f"<tr><td>{route}</td><td>{count}</td></tr>"
                html += "</table>"
            
            # Distance distribution
            html += "<h4>Distance Distribution</h4>"
            if 'DISTANCE_MILES' in df.columns:
                distance_bins = [0, 0.1, 0.25, 0.5, 1.0, 5.0, float('inf')]
                distance_labels = ['0-0.1', '0.1-0.25', '0.25-0.5', '0.5-1.0', '1.0-5.0', '5.0+']
                
                distance_dist = pd.cut(df['DISTANCE_MILES'], bins=distance_bins, labels=distance_labels).value_counts().sort_index()
                
                html += "<table><tr><th>Distance Range (miles)</th><th>Count</th></tr>"
                for dist_range, count in distance_dist.items():
                    html += f"<tr><td>{dist_range}</td><td>{count}</td></tr>"
                html += "</table>"
            
            # Common stop types with issues
            if 'REASON' in df.columns:
                # Extract stop types from reason messages
                stop_types = []
                for reason in df['REASON'].dropna():
                    if 'Boarding' in reason:
                        stop_types.append('Boarding')
                    elif 'Alighting' in reason:
                        stop_types.append('Alighting')
                    elif 'Prev' in reason:
                        stop_types.append('Previous Transfer')
                    elif 'Next' in reason:
                        stop_types.append('Next Transfer')
                
                if stop_types:
                    type_counts = pd.Series(stop_types).value_counts()
                    html += "<h4>Stop Types with Validation Issues</h4>"
                    html += "<table><tr><th>Stop Type</th><th>Count</th></tr>"
                    for stop_type, count in type_counts.items():
                        html += f"<tr><td>{stop_type}</td><td>{count}</td></tr>"
                    html += "</table>"
            
            return html
        
        stop_insights_html = get_stop_insights(stop_df)
    else:
        print(f"⚠️ Stop Validation CSV not found: {stop_csv_file}")
        stop_insights_html = "<p>No Stop Validation data available</p>"

    # --- OD Distance Insights from separate CSV ---
    od_csv_file = f'reviewtool_20260401_{project_name}_OD_Distance_Checks.csv'
    if os.path.exists(od_csv_file):
        od_df = pd.read_csv(od_csv_file)
        
        # print(od_df.columns.tolist())
        od_flags = [col for col in od_df.columns if col not in ['id','routesurveyedcode','FINAL_REVIEWER']]
        
        def get_od_insights(df):
            # List of OD distance flags
            od_flags = ['O_B_Dist_Check1','O_B_Dist_Check2','O_B_Dist_Check3',
                        'A_D_Dist_Check1','A_D_Dist_Check2','A_D_Dist_Check3',
                        'O_D_Dist_Check1','O_D_Dist_Check2','O_D_Dist_Check3',
                        'B_A_Dist_Check1','B_A_Dist_Check2']

            # Only keep flags that exist in the DataFrame
            od_flags = [flag for flag in od_flags if flag in df.columns]

            if not od_flags:
                return "<p>No OD distance flags found in the CSV</p>"

            # Summarize counts per flag
            od_summary = {flag: df[flag].sum() for flag in od_flags}

            # Generate HTML table
            html = "<h2>📊 OD Distance Check Summary</h2><table><tr><th>OD Flag</th><th>Count</th></tr>"
            for flag, count in od_summary.items():
                html += f"<tr><td>{flag}</td><td>{count}</td></tr>"
            html += "</table>"

            return html



        # od_df = merged_df  # or load the OD CSV separately if needed
        od_insights_html = get_od_insights(od_df)
    else:
        print(f"⚠️ OD Distance CSV not found: {od_csv_file}")
        od_insights_html = "<p>No OD Distance data available</p>"

     # --- Traditional Transfer Insights ---
    trad_csv_file = f'reviewtool_20260401_{project_name}_TraditionalTransferFlags.csv'
    if os.path.exists(trad_csv_file):
        trad_df = pd.read_csv(trad_csv_file)

        # print(trad_df['Checkall.GoodTransfer'].unique())
        # print(trad_df['Checkall.GoodTransfer'].dtype)
        

        # If transf. file is empty
        if trad_df.empty:
            trad_html = "<p>No traditional transfer flags found in the dataset.</p>"
        else:
            # Normalize column names (lowercase keys for safe matching)
            cols_lower = {c.lower(): c for c in trad_df.columns}

            # Transfer columns (Transfer1..Transfer8)
            transfer_cols = [cols_lower[c] for c in cols_lower
                            if c.startswith('transfer') and c.replace('transfer','').strip().isdigit()]
            # Check columns (Check1.GoodTransfer .. Check8.GoodTransfer)
            check_cols = [cols_lower[c] for c in cols_lower
                        if c.startswith('check') and 'goodtransfer' in c]

            # Ensure id & reviewer exist
            id_col = cols_lower.get('id')
            reviewer_col = cols_lower.get('final_reviewer') or cols_lower.get('finalreviewer') or cols_lower.get('final_reviewer'.lower())

            # Compute "bad" checks: a bad transfer is where Check*.GoodTransfer == 0 (or False)
            # Make sure check_cols exists
            if check_cols:
                # coerce to numeric then test == 0
                bad_check_mask = (trad_df[check_cols].apply(pd.to_numeric, errors='coerce') == 0).any(axis=1)
            else:
                bad_check_mask = pd.Series(False, index=trad_df.index)

            # use real flags column if present (some code sets df1['real flags'])
            real_flags_col = None
            for candidate in ['real flags','real_flags','realflags','status']:
                if candidate in trad_df.columns:
                    real_flags_col = candidate
                    break

            real_flags_mask = (trad_df[real_flags_col] == 1) if real_flags_col else pd.Series(False, index=trad_df.index)

            # Final flagged rows for 'traditional' analysis:
            flagged_trad = trad_df[ real_flags_mask | bad_check_mask ].copy()

            # Build HTML
            trad_html = "<h3>🚌 Traditional Transfer Insights</h3>"

            # 1) Per-transfer-pair top failures (from Transfer1..Transfer8) among flagged rows
            failed_transfers = []
            for c in transfer_cols:
                # only take values from rows that are flagged_trad
                if c in flagged_trad.columns:
                    failed_transfers.extend(flagged_trad[c].dropna().astype(str).tolist())

            if failed_transfers:
                top_failures = pd.Series(failed_transfers).value_counts().head(10)
                trad_html += "<h4>Most Common Failed Transfers (pair)</h4>"
                trad_html += "<table><tr><th>Transfer pair</th><th>Occurrences</th></tr>"
                for t, cnt in top_failures.items():
                    trad_html += f"<tr><td>{t}</td><td>{cnt}</td></tr>"
                trad_html += "</table>"
            else:
                trad_html += "<p>No transfer pairs recorded among flagged rows.</p>"

            # 2) Per-check (leg) failure counts, e.g., how many rows had Check1.GoodTransfer == 0 etc.
            if check_cols:
                check_fail_counts = {}
                for c in check_cols:
                    # treat non-numeric as NaN; consider a failure when value == 0
                    series = pd.to_numeric(trad_df[c], errors='coerce')
                    # Count failures among rows that were considered for flags (or full df — choose flagged_trad here)
                    check_fail_counts[c] = int((series[flagged_trad.index] == 1).sum())
                trad_html += "<h4>Failures by Transfer Leg (Check*)</h4>"
                trad_html += "<table><tr><th>Check</th><th>Failures</th></tr>"
                for c, cnt in check_fail_counts.items():
                    trad_html += f"<tr><td>{c}</td><td>{cnt}</td></tr>"
                trad_html += "</table>"
            else:
                trad_html += "<p>No Check*.GoodTransfer columns found to compute per-leg failures.</p>"

            # 3) Reviewer contribution: unique flagged surveys per reviewer, and flag rate (flagged / total surveys by reviewer)
            if reviewer_col and id_col:
                # total surveys per reviewer from the full merged_df (if available)
                try:
                    totals_by_reviewer = merged_df.groupby(merged_df[reviewer_col]).id.nunique()
                except Exception:
                    # fallback: totals from trad_df
                    totals_by_reviewer = trad_df.groupby(trad_df[reviewer_col]).id.nunique()

                flagged_by_reviewer = flagged_trad.groupby(flagged_trad[reviewer_col]).id.nunique()
                reviewer_table = pd.DataFrame({
                    'flagged_surveys': flagged_by_reviewer,
                    'total_surveys': totals_by_reviewer.reindex(flagged_by_reviewer.index).fillna(0).astype(int)
                }).fillna(0)
                reviewer_table['flag_rate'] = reviewer_table['flagged_surveys'] / reviewer_table['total_surveys'].replace(0, pd.NA)

                # sort by flagged_surveys descending
                reviewer_table = reviewer_table.sort_values('flagged_surveys', ascending=False).head(10)

                trad_html += "<h4>Top Reviewers (unique flagged surveys)</h4>"
                trad_html += "<table><tr><th>Reviewer</th><th>Flagged surveys</th><th>Total surveys</th><th>Flag rate</th></tr>"
                for reviewer, row in reviewer_table.iterrows():
                    rate_str = f"{row['flag_rate']:.1%}" if pd.notna(row['flag_rate']) else "N/A"
                    trad_html += f"<tr><td>{reviewer}</td><td>{int(row['flagged_surveys'])}</td><td>{int(row['total_surveys'])}</td><td>{rate_str}</td></tr>"
                trad_html += "</table>"
            else:
                trad_html += "<p>Reviewer or id column not found — cannot compute reviewer contributions.</p>"

    else:
        print(f"⚠️ Traditional Transfer CSV not found: {trad_csv_file}")
        trad_html = "<p>No Traditional Transfer data available</p>"

        # --- Transfer Distance Insights ---
    transfer_csv_file = f'reviewtool_20260401_{project_name}_Distance_Transfer_Flags.csv'
    if os.path.exists(transfer_csv_file):
        try:
            transfer_df = pd.read_csv(transfer_csv_file)
        except pd.errors.EmptyDataError:
            transfer_df = pd.DataFrame()

        transfer_html = "<h3>🚏 Transfer Distance Insights</h3>"

        if transfer_df.empty:
            transfer_html += "<p><strong>Total transfer distance flags:</strong> 0</p>"
            transfer_html += "<p>Transfer distance file is empty, so all values were treated as 0.</p>"
        else:
            # Normalize column names for easier matching
            cols_lower = {c.lower(): c for c in transfer_df.columns}

            # Detect route column
            route_col = None
            for possible in ['routesurveyedcode', 'route_surveyed_code', 'route_surveyedcode']:
                if possible in cols_lower:
                    route_col = cols_lower[possible]
                    break

            # Create transfer pair column if possible
            transfer_pair_col = None
            if 'prev_transfers' in cols_lower and 'next_transfers' in cols_lower:
                transfer_df['TransferPair'] = (
                    transfer_df[cols_lower['prev_transfers']].astype(str) +
                    " → " +
                    transfer_df[cols_lower['next_transfers']].astype(str)
                )
                transfer_pair_col = 'TransferPair'

            transfer_html += f"<p><strong>Total transfer distance flags:</strong> {len(transfer_df)}</p>"

            # Reviewer contribution table
            if 'final_reviewer' in cols_lower:
                reviewer_counts = transfer_df[cols_lower['final_reviewer']].value_counts().reset_index()
                reviewer_counts.columns = ['Reviewer', 'Flags']
                transfer_html += "<h4>Top Reviewers</h4><table><tr><th>Reviewer</th><th>Flags</th></tr>"
                for _, row in reviewer_counts.head(5).iterrows():
                    transfer_html += f"<tr><td>{row['Reviewer']}</td><td>{row['Flags']}</td></tr>"
                transfer_html += "</table>"

            # Route breakdown table
            if route_col:
                route_counts = transfer_df[route_col].value_counts().reset_index()
                route_counts.columns = ['Route', 'Flags']
                transfer_html += "<h4>Top Routes with Transfer Distance Failures</h4><table><tr><th>Route</th><th>Flags</th></tr>"
                for _, row in route_counts.head(5).iterrows():
                    transfer_html += f"<tr><td>{row['Route']}</td><td>{row['Flags']}</td></tr>"
                transfer_html += "</table>"

            # Transfer pair breakdown table
            if transfer_pair_col:
                pair_counts = transfer_df[transfer_pair_col].value_counts().reset_index()
                pair_counts.columns = ['Transfer Pair', 'Occurrences']
                transfer_html += "<h4>Most Common Transfer Pairs with Failures</h4><table><tr><th>Transfer Pair</th><th>Occurrences</th></tr>"
                for _, row in pair_counts.head(5).iterrows():
                    transfer_html += f"<tr><td>{row['Transfer Pair']}</td><td>{row['Occurrences']}</td></tr>"
                transfer_html += "</table>"

            if not route_col and not transfer_pair_col:
                transfer_html += "<p>No route or transfer pair data found.</p>"

    else:
        print(f"⚠️ Transfer Distance CSV not found: {transfer_csv_file}")
        transfer_html = "<p>No Transfer Distance data available</p>"


    # Generate sections
    temporal_html = generate_temporal_trend(merged_df, date_col) if date_col else "<p>No date column available</p>"
    cause_trend_html = generate_root_cause_trend(merged_df, date_col, flag_columns) if date_col else "<p>No date column available</p>"
    emerging_html = generate_emerging_issues(merged_df, date_col, flag_columns) if date_col else "<p>No date column available</p>"
    flag_table_html = generate_flag_table(flagged, flag_columns)
    cooccurrence_html = generate_cooccurrence_matrix(flagged, flag_columns)
    insights_html = generate_flag_insights(merged_df)

    # Build HTML
    html_report = f"""
    <html>
    <head>
        <title>{project_name} Flag Analysis - 20260401</title>
        <style>
            body {{ font-family: Arial; margin: 20px; }}
            .card {{ background: #f9f9f9; border-radius: 10px; padding: 15px; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
            h2 {{ color: #2c3e50; border-bottom: 2px solid #3498db; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
        </style>
    </head>
    <body>
        <h1>🚩 Survey Data Quality Report: {project_name}</h1>
        <small>Generated on 20260401</small>

        <div class="card">
            <h2>📊 Overview</h2>
            <p>Total surveys: <span>{len(merged_df):,}</span></p>
            <p>Flagged records: <span>{len(flagged):,} ({len(flagged)/len(merged_df):.1%})</span></p>
            <p>Top flag: <span>{flag_columns[0]}</span></p>
        </div>

        <div class="card">{temporal_html}</div>
        <div class="card">{cause_trend_html}</div>
        <div class="card"><h2>🚩 Flag Composition</h2>{flag_table_html}</div>
        <div class="card"><h2>🔍 Co-occurrence Analysis</h2>{cooccurrence_html}</div>
        <div class="card"><h2>📈 Emerging Issues</h2>{emerging_html}</div>
        <div class="card"><h2>🧩 Flag Insights</h2>{insights_html}{od_insights_html}{trad_html}{transfer_html}{stop_insights_html}</div>
        

    </body>
    </html>
    """

    # Save HTML
    report_path = f"reviewtool_20260401_{project_name}_analysis.html"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(dedent(html_report))

    print(f"🔥 Analysis Report Generated: {report_path}")

# === Run the Analysis ===
generate_analysis_report(merged_df, project_name, today_date)



merged_df.to_csv(f'reviewtool_20260401_{project_name}_combinedflags.csv',index=False)
# merged_df.to_csv('MUNI Merged Checks(v2).csv',index=False)



print('File Generated Successfully')