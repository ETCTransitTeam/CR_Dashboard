
import os
import datetime
import numpy as np
import pandas as pd
import streamlit as st
import snowflake.connector
from automated_refresh_flow_new import fetch_and_process_data
from utils import create_csv,download_csv, render_styled_dataframe
from authentication.auth import schema_value,register_page,login,logout,is_authenticated,forgot_password,reset_password,activate_account,change_password,send_change_password_email,change_password_form,create_new_user_page,is_super_admin,accounts_management_page,create_accounts_page,password_update_page
from dotenv import load_dotenv
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import plotly.express as px
import plotly.graph_objects as go
import time
from utils import apply_lacmta_agency_filter
import boto3
from io import BytesIO

load_dotenv()
st.set_page_config(page_title="Completion REPORT DashBoard", layout='wide')

with open("path/to/key.p8", "rb") as key:
    private_key = serialization.load_pem_private_key(
        key.read(),
        password=os.environ["SNOWFLAKE_PASSPHRASE"].encode(),
        backend=default_backend(),
    )

# Serialize the private key to DER format
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

# Helper function to read Excel from S3
def read_excel_from_s3(bucket_name, file_key, sheet_name):
    """Read Excel file from S3"""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('aws_access_key_id'),
            aws_secret_access_key=os.getenv('aws_secret_access_key')
        )
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        excel_data = response['Body'].read()
        return pd.read_excel(BytesIO(excel_data), sheet_name=sheet_name)
    except Exception as e:
        print(f"Error reading Excel from S3: {e}")
        return pd.DataFrame()

# Ensure session state exists
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False

# Get existing query parameters
query_params = st.query_params
# current_page = query_params.get("page", [""])[0]  # Get 'page' value if it exists
current_page = st.query_params.get("page", "login")  # Default to "login" if not set

# button_style = """
# <style>
# div.stButton > button, div.stDownloadButton > button{
#     width: 200px;  /* Makes buttons full width of their container */
#     padding: 0.5rem 1rem;  /* Consistent padding */
#     font-size: 16px;  /* Consistent font size */
# }
# </style>
# """
# st.markdown(button_style, unsafe_allow_html=True)

# Initialize project switching states
if "show_switch_success" not in st.session_state:
    st.session_state.show_switch_success = False
if "success_project_name" not in st.session_state:
    st.session_state.success_project_name = None

if not st.session_state["logged_in"]:
    if current_page == "signup":
        register_page()  # Show the register page if the query parameter is set to "register"
    elif current_page == "login":
        login()  # Show the login page by default
    elif current_page == "forgot_password":
        forgot_password()
    elif current_page == "reset_password":
        reset_password()
    elif current_page=='activate':
        activate_account()
    elif current_page=='change_password':
        change_password_form()
    elif current_page=='create_user':
        create_new_user_page()
    else:
        st.write('Token Expired. LogIn Again')
        if st.button("Login"):
            # st.experimental_set_query_params(page="login")
            # st.rerun()
            st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=login">', unsafe_allow_html=True)
    st.stop()
else:
    if not is_authenticated():
        st.error("Your Token Expired.You need to log in first.")
        # Optionally, redirect the user to the login page
        st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=login">', unsafe_allow_html=True)
    else:
        selected_schema = st.session_state.get("schema", None)
        selected_project = str(st.session_state.get("selected_project", "")).lower()
        def create_snowflake_connection():
            # Get selected project and agency
            selected_agency = st.session_state.get("selected_agency", None)
            selected_project = st.session_state.get("selected_project", "")
            print("Selected Agency:", selected_agency)

            # Determine which schema to use
            schema_to_use = selected_schema  # Default to current schema
            
            if selected_project == "LACMTA_FEEDER" and selected_agency and selected_agency != "All":
                # Use agency-specific schema: LACMTA_FEEDER_<AGENCY_NAME>
                schema_to_use = f"LACMTA_FEEDER_{selected_agency}"
                print(f"Using agency schema: {schema_to_use}")
            
            conn = snowflake.connector.connect(
                user=os.getenv('SNOWFLAKE_USER'),
                private_key=private_key_bytes,
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                database=os.getenv('SNOWFLAKE_DATABASE'),
                authenticator="SNOWFLAKE_JWT",
                schema=schema_to_use,  # Use dynamic schema name
                role=os.getenv('SNOWFLAKE_ROLE'),
                network_timeout=120
            )
            return conn

        def get_agency_names(project):
            """Fetch unique agency names from details file for LACMTA_FEEDER project"""
            if project != "lacmta_feeder":
                return []
            
            try:
                from automated_refresh_flow_new import PROJECTS
                project = project.upper()
                project_config = PROJECTS[project]
                bucket_name = os.getenv('bucket_name')
                details_file = project_config["files"]["details"]
                # Read details file from S3
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=os.getenv('aws_access_key_id'),
                    aws_secret_access_key=os.getenv('aws_secret_access_key')
                )
                
                # Read STOPS sheet from details file
                response = s3_client.get_object(Bucket=bucket_name, Key=details_file)
                excel_data = response['Body'].read()
                details_df = pd.read_excel(BytesIO(excel_data), sheet_name='STOPS')
                
                # Get unique agency names
                if 'agency' in details_df.columns:
                    agencies = sorted(details_df['agency'].dropna().unique())
                    return agencies
                else:
                    print("Warning: agency column not found in details file")
                    return []
                    
            except Exception as e:
                print(f"Error fetching agency names: {e}")
                return []

        def get_database_records_metrics():
            """
            Calculate and return metrics for Elvis, BabyElvis/Main, and KingElvis databases
            following dashboard logic and client-approved breakdown.
            """
            from automated_refresh_flow_new import PROJECTS, fetch_data
            from automated_sync_flow_constants_maps import KCATA_HEADER_MAPPING

            metrics = {
                "project": None,

                # Raw DB totals
                "elvis_total": 0,
                "babyelvis_db_total": 0,

                # Used after DB filters
                "elvis_used": 0,
                "babyelvis_db_used": 0,

                # Aggregated DB metrics
                "db_total_records": 0,
                "db_used_records": 0,

                # Removal metrics (NEW)
                "removed_by_status_delete": 0,   # elvisstatuscode = 4
                "removed_by_review": 0,          # KingElvis Final_Usage

                # KingElvis
                "kingelvis_db_total": 0,
                "kingelvis_db_used": 0,

                # Final pipeline numbers
                "merged_records": 0,
                "filtered_records": 0,

                "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            try:
                project = st.session_state.get("selected_project")
                if not project:
                    return metrics

                metrics["project"] = project
                project_config = PROJECTS[project]

                # ------------------------
                # Fetch Elvis DB
                # ------------------------
                elvis_df = None
                if "elvis" in project_config["databases"]:
                    elvis_conf = project_config["databases"]["elvis"]
                    csv_buffer = fetch_data(elvis_conf["database"], elvis_conf["table"])
                    if csv_buffer:
                        elvis_df = pd.read_csv(csv_buffer, low_memory=False)
                        elvis_df.columns = elvis_df.columns.str.strip()
                        elvis_df = elvis_df.rename(columns=KCATA_HEADER_MAPPING)
                        metrics["elvis_total"] = len(elvis_df)

                        have5_col = check_all_characters_present(elvis_df, ["have5minforsurvecode"])
                        status_col = check_all_characters_present(elvis_df, ["elvisstatuscode"])

                        have5_col = have5_col[0] if have5_col else None
                        status_col = status_col[0] if status_col else None

                        if have5_col:
                            elvis_df = elvis_df[elvis_df[have5_col].astype(str) == "1"]

                        if "INTERV_INIT" in elvis_df.columns:
                            elvis_df = elvis_df[elvis_df["INTERV_INIT"].astype(str) != "999"]

                        if status_col:
                            status_4_mask = elvis_df[status_col].astype(str).str.lower() == "4"
                            metrics["removed_by_status_delete"] += status_4_mask.sum()
                            elvis_df = elvis_df[~status_4_mask]

                        if "id" in elvis_df.columns:
                            elvis_df = elvis_df.drop_duplicates(subset=["id"])
                        
                        # Apply LACMTA agency filter on Elvis
                        selected_agency = st.session_state.get("selected_agency")

                        if project == "LACMTA_FEEDER" and selected_agency and selected_agency != "All":
                            elvis_df, _ = apply_lacmta_agency_filter(
                                df=elvis_df,
                                project=project,
                                agency=selected_agency,
                                bucket_name=os.getenv("bucket_name"),
                                project_config=project_config
                            )

                        metrics["elvis_used"] = len(elvis_df)

                # ------------------------
                # Fetch BabyElvis / Main DB
                # ------------------------
                baby_df = None
                db_key = "baby_elvis" if "baby_elvis" in project_config["databases"] else "main"

                if db_key in project_config["databases"]:
                    conf = project_config["databases"][db_key]
                    csv_buffer = fetch_data(conf["database"], conf["table"])
                    if csv_buffer:
                        baby_df = pd.read_csv(csv_buffer, low_memory=False)

                if baby_df is not None:
                    baby_df.columns = baby_df.columns.str.strip()
                    baby_df = baby_df.rename(columns=KCATA_HEADER_MAPPING)
                    metrics["babyelvis_db_total"] = len(baby_df)

                    have5_col = check_all_characters_present(baby_df, ["have5minforsurvecode"])
                    status_col = check_all_characters_present(baby_df, ["elvisstatuscode"])

                    have5_col = have5_col[0] if have5_col else None
                    status_col = status_col[0] if status_col else None

                    if have5_col:
                        baby_df = baby_df[baby_df[have5_col].astype(str) == "1"]

                    if "INTERV_INIT" in baby_df.columns:
                        baby_df = baby_df[baby_df["INTERV_INIT"].astype(str) != "999"]

                    if status_col:
                        status_4_mask = baby_df[status_col].astype(str).str.lower() == "4"
                        metrics["removed_by_status_delete"] += status_4_mask.sum()
                        baby_df = baby_df[~status_4_mask]

                    if "id" in baby_df.columns:
                        baby_df = baby_df.drop_duplicates(subset=["id"])

                    # Apply LACMTA agency filter on BabyElvis
                    if project == "LACMTA_FEEDER" and selected_agency and selected_agency != "All":
                        _, baby_df = apply_lacmta_agency_filter(
                            df=None,
                            baby_elvis_df=baby_df,
                            project=project,
                            agency=selected_agency,
                            bucket_name=os.getenv("bucket_name"),
                            project_config=project_config
                        )

                    metrics["babyelvis_db_used"] = len(baby_df)


                # ------------------------
                # Aggregate DB metrics
                # ------------------------
                metrics["db_total_records"] = (
                    metrics["elvis_total"] + metrics["babyelvis_db_total"]
                )
                metrics["db_used_records"] = (
                    metrics["elvis_used"] + metrics["babyelvis_db_used"]
                )

                # ------------------------
                # Fetch KingElvis Sheet
                # ------------------------
                ke_df = None
                bucket_name = os.getenv("bucket_name")
                try:
                    ke_df = read_excel_from_s3(bucket_name, project_config["files"]["kingelvis"], "Elvis_Review")
                except Exception:
                    try:
                        ke_df = read_excel_from_s3(bucket_name, project_config["files"]["kingelvis"], "Sheet1")
                    except Exception:
                        ke_df = None

                if ke_df is not None:
                    metrics["kingelvis_db_total"] = len(ke_df)

                    if "Final_Usage" in ke_df.columns:
                        fu = ke_df["Final_Usage"].astype(str).str.strip().str.lower()
                        removed_mask = fu.isin(["remove", "no data"])
                        metrics["removed_by_review"] = removed_mask.sum()
                        metrics["kingelvis_db_used"] = len(ke_df[~removed_mask])
                    else:
                        metrics["kingelvis_db_used"] = metrics["kingelvis_db_total"]
                
                # ------------------------
                # Apply LACMTA Agency Filter (UI-level)
                # ------------------------
                selected_agency = st.session_state.get("selected_agency")
                project = st.session_state.get("selected_project")

                if project == "LACMTA_FEEDER" and selected_agency and selected_agency != "All":

                    project_config = PROJECTS[project]
                    bucket_name = os.getenv("bucket_name")

                    elvis_df, baby_df = apply_lacmta_agency_filter(
                        df=elvis_df,
                        baby_elvis_df=baby_df,
                        project=project,
                        agency=selected_agency,
                        bucket_name=bucket_name,
                        project_config=project_config
                    )


                # ------------------------
                # Merge Elvis + BabyElvis
                # ------------------------
                merged_df = None
                if elvis_df is not None and baby_df is not None:
                    missing_ids = set(baby_df["id"]) - set(elvis_df["id"])
                    baby_new = baby_df[baby_df["id"].isin(missing_ids)]
                    merged_df = (
                        pd.concat([elvis_df, baby_new], ignore_index=True)
                        .drop_duplicates(subset=["id"])
                        .reset_index(drop=True)
                    )
                elif elvis_df is not None:
                    merged_df = elvis_df.copy()
                elif baby_df is not None:
                    merged_df = baby_df.copy()

                metrics["merged_records"] = len(merged_df) if merged_df is not None else 0

                # ------------------------
                # Apply KingElvis filtering (FINAL DASHBOARD NUMBER)
                # ------------------------
                filtered_df = merged_df.copy() if merged_df is not None else None

                if filtered_df is not None and ke_df is not None and "Final_Usage" in ke_df.columns:

                    exclude_mask = ke_df["Final_Usage"].astype(str).str.strip().str.lower().isin(
                        ["remove", "no data"]
                    )

                    exclude_ids_df = ke_df[exclude_mask]

                    id_col = "elvis_id" if "elvis_id" in ke_df.columns else "id"
                    exclude_ids = exclude_ids_df[id_col].dropna().unique()

                    # ---- AUDIT COUNTS ----
                    total_before = len(filtered_df)
                    excluded_count = filtered_df["id"].isin(exclude_ids).sum()
                    included_count = total_before - excluded_count

                    print(f"[KingElvis Filter] Total before filter : {total_before}")
                    print(f"[KingElvis Filter] Excluded IDs       : {excluded_count}")
                    print(f"[KingElvis Filter] Included IDs       : {included_count}")

                    # ---- APPLY FILTER ----
                    filtered_df = filtered_df[~filtered_df["id"].isin(exclude_ids)]

                metrics["filtered_records"] = (
                    len(filtered_df) if filtered_df is not None else metrics["merged_records"]
                )

                return metrics

            except Exception as e:
                import traceback
                traceback.print_exc()
                print(f"Error calculating metrics: {e}")
                return metrics

        def show_database_metrics_popup():
            """
            Display database metrics with summary cards and expandable detailed table
            (client-approved minimal cards + detailed expander)
            """
            metrics = get_database_records_metrics()

            if not metrics:
                st.error("Unable to calculate database metrics")
                return

            # ---------- CSS ----------
            st.markdown("""
            <style>
            .metric-card {
                background: #f9fafb;
                border-radius: 10px;
                padding: 16px;
                margin-bottom: 10px;
                border-left: 4px solid #4f46e5;
                height: 100%;
            }

            .metric-title {
                font-size: 0.9rem;
                font-weight: 600;
                color: #374151;
                margin-bottom: 6px;
            }

            .metric-value {
                font-size: 1.6rem;
                font-weight: 700;
                color: #111827;
                line-height: 1.2;
            }

            .section-title {
                font-size: 1rem;
                font-weight: 600;
                margin: 18px 0 10px 0;
                color: #111827;
            }
            </style>
            """, unsafe_allow_html=True)

            # ---------- TOP SUMMARY CARDS (NUMBERS ONLY) ----------
            col1, col2, col3 = st.columns(3)

            with col1:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">All DB Records</div>
                    <div class="metric-value">{metrics['db_total_records']:,}</div>
                </div>
                """, unsafe_allow_html=True)

            with col2:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-title">Complete / Used Surveys</div>
                    <div class="metric-value">{metrics['db_used_records']:,}</div>
                </div>
                """, unsafe_allow_html=True)

            with col3:
                st.markdown(f"""
                <div class="metric-card" style="border-left-color:#dc2626;">
                    <div class="metric-title">Final Dashboard Records</div>
                    <div class="metric-value">{metrics['filtered_records']:,}</div>
                </div>
                """, unsafe_allow_html=True)

            # ---------- DETAILED METRICS EXPANDER ----------
            with st.expander("Detailed Metrics Breakdown", expanded=True):

                detailed_rows = [
                    {
                        "Category": "All Total Records",
                        "Count": metrics["db_total_records"],
                        "Details": "Raw combined records from Elvis DB and BabyElvis/Main DB before applying any filters."
                    },
                    {
                        "Category": "Complete / Used Surveys",
                        "Count": metrics["db_used_records"],
                        "Details": (
                            "Records remaining after DB-level filters: "
                            "time availability, INTERV_INIT != 999, "
                            "status != Delete (4), and deduplication."
                        )
                    },
                    {
                        "Category": "Removed by Status = Delete",
                        "Count": metrics["removed_by_status_delete"],
                        "Details": "Records removed from DBs where elvisstatuscode = 4."
                    },
                    {
                        "Category": "Removed by Review",
                        "Count": metrics["removed_by_review"],
                        "Details": (
                            "Records removed based on ReviewTeam sheet "
                            "(Final_Usage = remove / no data)."
                        )
                    },
                    {
                        "Category": "Merged Records",
                        "Count": metrics["merged_records"],
                        "Details": "Union of Elvis and BabyElvis records after DB filters and deduplication."
                    },
                    {
                        "Category": "Final Dashboard Records",
                        "Count": metrics["filtered_records"],
                        "Details": (
                            "Final authoritative dataset used by the dashboard "
                            "after applying ReviewTeam exclusions."
                        )
                    },
                ]

                detailed_df = pd.DataFrame(detailed_rows)
                st.dataframe(detailed_df, use_container_width=True)

            # ---------- FOOTER ----------
            st.markdown(f"""
            <div style="text-align:center; font-size:0.75rem; color:#6b7280; margin-top:14px;">
                Last updated: {metrics['timestamp']}
            </div>
            """, unsafe_allow_html=True)

        def export_elvis_data():
            """
            Connect to the database and download Elvis table as CSV directly
            """
            try:
                from automated_refresh_flow_new import PROJECTS, fetch_data
                
                project_config = PROJECTS[st.session_state["selected_project"]]
                elvis_config = project_config['databases']["elvis"]
                table_name = elvis_config['table']
                database_name = elvis_config["database"]
                
                with st.spinner("🔄 Downloading Elvis data..."):
                    csv_buffer = fetch_data(database_name, table_name)
                    
                    if csv_buffer:
                        # Create filename
                        project_name = st.session_state["selected_project"].lower().replace(" ", "_")
                        file_name = f"{project_name}_elvis_db.csv"
                        
                        # Use the existing CSV buffer directly - no conversion needed
                        st.download_button(
                            label="✅ Download Complete - Click to Save",
                            data=csv_buffer.getvalue(),
                            file_name=file_name,
                            mime="text/csv",
                            key="elvis_download"
                        )
                        
                    else:
                        st.error("❌ Failed to fetch data from Elvis table")
                        
            except Exception as e:
                st.error(f"❌ Error exporting Elvis data: {str(e)}")

        pinned_column='ROUTE_SURVEYEDCode'

        # @st.cache(allow_output_mutation=True)  # Use st.cache in Streamlit 1.6.0
        def fetch_dataframes_from_snowflake(cache_key):
            """
            Fetches data from Snowflake tables and returns them as a dictionary of DataFrames.
            If a table is missing or an error occurs, it returns an empty DataFrame.

            Returns:
                dict: A dictionary where keys are DataFrame names and values are DataFrames.
            """
            # Snowflake connection details
            conn = create_snowflake_connection()
            cur = conn.cursor()

            # Table-to-DataFrame mapping
            table_to_df_mapping = {
                'wkday_stationwise_comparison': 'wkday_stationwise_df',
                'wkend_stationwise_comparison': 'wkend_stationwise_df',
                'wkday_comparison': 'wkday_df',
                'wkday_dir_comparison': 'wkday_dir_df',
                'wkend_comparison': 'wkend_df',
                'wkend_dir_comparison': 'wkend_dir_df',
                'wkend_time_data': 'wkend_time_df',
                'wkday_time_data': 'wkday_time_df',
                'wkend_raw': 'wkend_raw_df',
                'wkday_raw': 'wkday_raw_df',
                'TOD': 'detail_df',
                'by_interv_totals': 'by_interv_totals_df',
                'by_route_totals': 'by_route_totals_df',
                'survey_detail_totals': 'survey_detail_totals_df',
                'surveyor_report_trends': 'surveyor_report_trends_df',
                'route_report_trends': 'route_report_trends_df',
                'surveyor_report_date_trends': 'surveyor_report_date_trends_df',
                'route_report_date_trends': 'route_report_date_trends_df',
                'route_comparison': 'route_comparison_df',
                'reverse_routes': 'reverse_routes_df',
                'reverse_routes_difference': 'reverse_routes_difference_df',
                'low_response_questions': 'low_response_questions_df',
                'refusal_analysis_report': 'refusal_analysis_df',
                'refusal_race_report': 'refusal_race_df',
                'demographic_review': 'demographic_review_df',
                'last_survey_date': 'last_survey_date_df',
            }

            # Initialize an empty dictionary to hold DataFrames
            dataframes = {}

            try:
                # Loop through each table, fetch its data, and store it in the corresponding DataFrame
                for table_name, df_name in table_to_df_mapping.items():
                    try:
                        # Query to fetch data
                        query = f"SELECT * FROM {table_name}"
                        
                        # Execute query and fetch data
                        cur.execute(query)
                        data = cur.fetchall()

                        # Get column names from the cursor description
                        columns = [desc[0] for desc in cur.description] if cur.description else []

                        # Convert data to DataFrame
                        df = pd.DataFrame(data, columns=columns) if data else pd.DataFrame(columns=columns)
                        dataframes[df_name] = df

                        print(f"Data fetched and stored in DataFrame: {df_name}")

                    except Exception as e:
                        # Handle cases where the table doesn't exist or another error occurs
                        print(f"Error fetching data from {table_name}: {e}")
                        dataframes[df_name] = pd.DataFrame()  # Return an empty DataFrame instead of None

            finally:
                # Close cursor and connection
                cur.close()
                conn.close()

            return dataframes

        # Fetch dataframes from Snowflake
        if "cache_key" not in st.session_state:
            st.session_state["cache_key"] = 0
        dataframes = fetch_dataframes_from_snowflake(st.session_state["cache_key"] )

        # Access DataFrames from the fetched dataframes dictionary
        wkday_df = dataframes.get('wkday_df', pd.DataFrame())
        wkday_dir_df = dataframes.get('wkday_dir_df', pd.DataFrame())
        wkend_df = dataframes.get('wkend_df', pd.DataFrame())
        wkend_dir_df = dataframes.get('wkend_dir_df', pd.DataFrame())
        wkend_time_df = dataframes.get('wkend_time_df', pd.DataFrame())
        wkday_time_df = dataframes.get('wkday_time_df', pd.DataFrame())
        wkend_raw_df = dataframes.get('wkend_raw_df', pd.DataFrame())
        wkday_raw_df = dataframes.get('wkday_raw_df', pd.DataFrame())
        detail_df = dataframes.get('detail_df', pd.DataFrame())
        surveyor_report_trends_df = dataframes.get('surveyor_report_trends_df', pd.DataFrame())
        route_report_trends_df = dataframes.get('route_report_trends_df', pd.DataFrame())
        surveyor_report_date_trends_df = dataframes.get('surveyor_report_date_trends_df', pd.DataFrame())
        route_report_date_trends_df = dataframes.get('route_report_date_trends_df', pd.DataFrame())

        wkday_stationwise_df = dataframes.get('wkday_stationwise_df', pd.DataFrame())
        wkend_stationwise_df = dataframes.get('wkend_stationwise_df', pd.DataFrame())

        by_interv_totals_df = dataframes.get('by_interv_totals_df', pd.DataFrame())
        by_route_totals_df = dataframes.get('by_route_totals_df', pd.DataFrame())
        survey_detail_totals_df = dataframes.get('survey_detail_totals_df', pd.DataFrame())

        route_comparison_df = dataframes.get('route_comparison_df', pd.DataFrame())
        reverse_routes_df = dataframes.get('reverse_routes_df', pd.DataFrame())
        reverse_routes_difference_df = dataframes.get('reverse_routes_difference_df', pd.DataFrame())
        low_response_questions_df = dataframes.get('low_response_questions_df', pd.DataFrame())

        refusal_analysis_df = dataframes.get('refusal_analysis_df', pd.DataFrame())
        refusal_race_df = dataframes.get('refusal_race_df', pd.DataFrame())
        demographic_review_df = dataframes.get('demographic_review_df', pd.DataFrame())
        last_survey_date_df = dataframes.get('last_survey_date_df', pd.DataFrame())

        ####################################################################################################

        # Page mapping (shared for forward and reverse lookup)
        PAGE_MAPPING = {
            "🏠︎   Home": "main",
            "🗓︎   WEEKDAY-OVERALL": "weekday",
            "☀︎   WEEKEND-OVERALL": "weekend",
            "🕒  Time Of Day Details": "timedetails",
            "🗺️  Location Maps": "location_maps",
            "⤓    LOW RESPONSE QUESTIONS": "low_response_questions_tab",
            "↺   Clone Records": "reverse_routes",
            "⌗  DAILY TOTALS": "dailytotals",
            "∆   Surveyor/Route/Trend Reports": "surveyreport",
            "◉  WEEKDAY StationWise Comparison": "weekday_station",
            "⦾  WEEKEND StationWise Comparison": "weekend_station",
            "🚫  Refusal Analysis": "refusal",
            "👥  Demographic Review": "demographic",
            "⚙️  Accounts Management": "accounts_management",
            "➕  Create Accounts": "create_accounts",
            "🔐  Password Update": "password_update",
        }
        
        # Reverse mapping (page key -> menu item)
        REVERSE_PAGE_MAPPING = {v: k for k, v in PAGE_MAPPING.items()}
        
        def get_page_key(selected_page):
            return PAGE_MAPPING.get(selected_page, "main")
        

        user = st.session_state["user"]
        username = user["username"]
        email = user["email"]
        role = user["role"]

        

        # Sidebar Styling
        st.markdown("""
<style>
    /* === SIDEBAR CONTAINER === */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, 
            color-mix(in srgb, var(--secondary-background-color) 95%, var(--background-color)) 0%, 
            var(--background-color) 100%) !important;
        color: var(--text-color);
        box-shadow: 2px 0 15px rgba(0,0,0,0.12);
        border-right: 1px solid rgba(128,128,128,0.15);
        padding-top: 1rem !important;
        animation: slideIn 0.5s cubic-bezier(0.4, 0, 0.2, 1);
        backdrop-filter: blur(10px);
    }

    /* === ANIMATIONS === */
    @keyframes slideIn {
        from { transform: translateX(-30px); opacity: 0; }
        to { transform: translateX(0); opacity: 1; }
    }

    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(10px); }
        to { opacity: 1; transform: translateY(0); }
    }

    @keyframes pulse {
        0%, 100% { transform: scale(1); }
        50% { transform: scale(1.05); }
    }

    /* === PROFILE CARD === */
    .profile-card {
        background: radial-gradient(circle at top right, rgb(255 255 255), #ffffff 70%) !important;
        margin: -12px 16px 20px;
        border-radius: 16px;
        padding: 18px;
        text-align: center;
        transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
        border: 1px solid rgba(66, 133, 244, 0.15);
        box-shadow: 0 4px 15px rgba(0,0,0,0.08);
        max-width: calc(100% - 32px);
        box-sizing: border-box;
        animation: fadeIn 0.6s ease-out;
        position: relative;
        overflow: hidden;
    }

    .profile-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: -100%;
        width: 100%;
        height: 100%;
        background: linear-gradient(90deg, 
            transparent, 
            rgba(255,255,255,0.1), 
            transparent);
        transition: left 0.5s;
    }

    .profile-card:hover::before {
        left: 100%;
    }

    .profile-card:hover {
        transform: translateY(-4px) scale(1.02);
        box-shadow: 0 8px 25px rgba(66,133,244,0.3);
        border-color: var(--primary-color);
    }

    /* === PROFILE INITIAL === */
    .profile-initial {
        width: 60px;
        height: 60px;
        border-radius: 50%;
        background: rgb(0 104 148) !important;
        color: white !important;
        display: flex;
        align-items: center;
        justify-content: center;
        font-weight: 700;
        font-size: 1.5rem;
        margin: 0 auto 12px;
        transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
        border: 3px solid rgba(255,255,255,0.4);
        box-shadow: 0 4px 15px rgba(66, 133, 244, 0.4);
        position: relative;
    }

    .profile-initial::after {
        content: '';
        position: absolute;
        width: 100%;
        height: 100%;
        border-radius: 50%;
        border: 2px solid var(--primary-color);
        opacity: 0;
        animation: pulse 2s infinite;
    }

    .profile-card:hover .profile-initial {
        transform: rotate(360deg) scale(1.1);
        box-shadow: 0 6px 20px rgba(66, 133, 244, 0.6);
    }

    /* === PROJECT BADGE === */
    .profile-card > div:last-child {
        border: 1px solid rgba(66, 133, 244, 0.3);
        backdrop-filter: blur(5px);
        transition: all 0.3s ease;
    }

    .profile-card > div:last-child:hover {
        transform: scale(1.05);
        box-shadow: 0 2px 8px rgba(66, 133, 244, 0.3);
    }

    /* === SECTION LABELS === */
    .section-label {
        text-transform: uppercase;
        font-size: 15px;
        font-weight: 600;
        opacity: 0.7;
        margin-top: 16px;
        transition: all 0.3s ease;
        position: relative;
    }

    .section-label::before {
        content: '';
        position: absolute;
        left: 0;
        top: 50%;
        transform: translateY(-50%);
        width: 3px;
        height: 12px;
        background: var(--primary-color);
        border-radius: 2px;
    }

    /* === INPUTS === */
    input[type="text"] {
        background-color: color-mix(in srgb, var(--secondary-background-color) 95%, var(--background-color)) !important;
        color: var(--text-color) !important;
        border: 1px solid rgba(128,128,128,0.25) !important;
        border-radius: 8px !important;
        transition: all 0.3s ease !important;
    }

    input[type="text"]:focus {
        border-color: var(--primary-color) !important;
        box-shadow: 0 0 0 3px rgba(66, 133, 244, 0.1) !important;
        transform: translateY(-1px);
    }

    /* === DROPDOWN === */
    div[data-baseweb="select"] {
        background: color-mix(in srgb, var(--secondary-background-color) 92%, var(--background-color));
        border-radius: 10px;
        transition: all 0.3s ease;
        border: 1px solid rgba(128,128,128,0.2);
    }

    div[data-baseweb="select"]:hover {
        border-color: var(--primary-color);
        box-shadow: 0 2px 8px rgba(66, 133, 244, 0.15);
    }

    /* === COLUMNS === */
    .stColumn {
        flex: 1 !important;
    }

    /* === BUTTONS === */
    .stButton > button, .stDownloadButton > button {
        width: 100% !important;
        color: var(--text-color);
        border-radius: 10px;
        border: 1px solid rgba(128,128,128,0.25) !important;
        font-weight: 500;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        background: color-mix(in srgb, var(--secondary-background-color) 96%, var(--background-color)) !important;
        position: relative;
        overflow: hidden;
    }

    .stButton > button::before, .stDownloadButton > button::before {
        content: '';
        position: absolute;
        top: 50%;
        left: 50%;
        width: 0;
        height: 0;
        border-radius: 50%;
        background: rgba(255,255,255,0.3);
        transform: translate(-50%, -50%);
        transition: width 0.6s, height 0.6s;
    }

    .stButton > button:hover::before, .stDownloadButton > button:hover::before {
        width: 300px;
        height: 300px;
    }

    .stButton > button:hover, .stDownloadButton > button:hover {
        background-color: var(--primary-color) !important;
        color: #fff !important;
        transform: translateY(-2px);
        box-shadow: 0 6px 15px rgba(66,133,244,0.4);
        border-color: var(--primary-color) !important;
    }

    /* === ACTIVE BUTTON === */
    .active-button > button {
        border-left: 4px solid var(--primary-color);
        background: linear-gradient(90deg, 
            color-mix(in srgb, var(--primary-color) 15%, var(--background-color)) 0%,
            color-mix(in srgb, var(--primary-color) 5%, var(--background-color)) 100%) !important;
        font-weight: 600;
        box-shadow: inset 0 0 10px rgba(66, 133, 244, 0.1);
    }

    /* === BOTTOM BUTTONS === */
    .bottom-buttons {
        position: fixed;
        bottom: 20px;
        left: 0;
        width: 15.5rem;
        padding: 16px;
        background: linear-gradient(180deg,
            transparent 0%,
            color-mix(in srgb, var(--secondary-background-color) 97%, var(--background-color)) 20%);
        z-index: 999;
        transition: all 0.3s ease;
    }

    /* === SPECIAL BUTTONS === */
    #ChangePasswordButton {
        background: linear-gradient(135deg, #2ecc71 0%, #27ae60 100%) !important;
        color: white !important;
        border: none !important;
    }

    #ChangePasswordButton:hover {
        background: linear-gradient(135deg, #27ae60 0%, #229954 100%) !important;
        box-shadow: 0 6px 15px rgba(46, 204, 113, 0.4) !important;
    }

    #LogoutButton {
        background: linear-gradient(135deg, #e63946 0%, #c62828 100%) !important;
        color: white !important;
        border: none !important;
    }

    #LogoutButton:hover {
        background: linear-gradient(135deg, #c62828 0%, #b71c1c 100%) !important;
        box-shadow: 0 6px 15px rgba(230, 57, 70, 0.4) !important;
    }

    /* === SCROLLBAR === */
    [data-testid="stSidebar"]::-webkit-scrollbar {
        width: 8px;
    }

    [data-testid="stSidebar"]::-webkit-scrollbar-thumb {
        background: var(--primary-color);
        border-radius: 10px;
        transition: background 0.3
        }

        /* --- Logout and Change Password Buttons --- */
        #ChangePasswordButton {
            background-color: #2ecc71 !important;
            color: white !important;
        }
        #ChangePasswordButton:hover {
            background-color: #27ae60 !important;
        }

        #LogoutButton {
            background-color: #e63946 !important;
            color: white !important;
        }
        #LogoutButton:hover {
            background-color: #c62828 !important;
        }

        /* --- Ensure text doesn't overflow in collapsed state --- */
        [data-testid="stSidebar"][aria-expanded="false"] * {
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        /* Allow profile card text to wrap properly */
        [data-testid="stSidebar"][aria-expanded="false"] .profile-card * {
            white-space: normal !important;
            word-break: break-word !important;
        }

        </style>
        """, unsafe_allow_html=True)

        st.markdown("""
        <style>
        /* 🌐 GLOBAL BUTTON DESIGN — works for all buttons (sidebar + main page) */
        .stButton > button {
            width: 100%;
            border: none !important;
            padding: 0.6rem 1rem !important;
            border-radius: 10px !important;
            font-weight: 500 !important;
            transition: all 0.25s ease !important;
            cursor: pointer !important;
            font-size: 0.95rem !important;
        }

        /* 🌞 LIGHT THEME STYLE */
        @media (prefers-color-scheme: light) {
            .stButton > button {
                background: #f5f6f7 !important;          /* clean soft gray */
                color: #222 !important;                  /* dark text */
                border: 1px solid #dcdcdc !important;
                box-shadow: 0 2px 5px rgba(0,0,0,0.05);
            }
            .stButton > button:hover {
                background: linear-gradient(90deg, #4285f4, #6fa8ff) !important; /* blue gradient */
                color: #fff !important;                  /* white text on hover */
                border: none !important;
                box-shadow: 0 6px 18px rgba(66,133,244,0.25);
                transform: translateY(-2px);
            }
        }

        /* 🌙 DARK THEME STYLE */
        @media (prefers-color-scheme: dark) {
            .stButton > button {
                background: #2b2f3a !important;           /* deep gray */
                color: #f4f4f4 !important;                /* bright text */
                border: 1px solid rgba(255,255,255,0.08);
                box-shadow: 0 2px 5px rgba(0,0,0,0.25);
            }
            .stButton > button:hover {
                background: linear-gradient(90deg, #4285f4, #6fa8ff) !important; /* blue gradient */
                color: #fff !important;
                box-shadow: 0 6px 18px rgba(66,133,244,0.35);
                transform: translateY(-2px);
            }
        }

        /* 💫 Active button effect (when pressed) */
        .stButton > button:active {
            transform: scale(0.98);
            box-shadow: 0 2px 6px rgba(0,0,0,0.1);
        }

        /* 🔘 Rounded focus ring */
        .stButton > button:focus {
            outline: 2px solid rgba(66,133,244,0.5);
            outline-offset: 2px;
        }

        /* 🔻Bottom fixed buttons section (sidebar only) */
        [data-testid="stSidebar"] .bottom-buttons {
            position: fixed !important;
            bottom: 20px !important;
            left: 12px !important;
            right: 12px !important;
            padding-top: 10px !important;
        }

        /* 🟢 Change Password button */
        #ChangePasswordButton button {
            background-color: #2ecc71 !important;
            color: white !important;
        }
        #ChangePasswordButton button:hover {
            background-color: #27ae60 !important;
        }

        /* 🔴 Logout button */
        #LogoutButton button {
            background-color: #e63946 !important;
            color: white !important;
        }
        #LogoutButton button:hover {
            background-color: #c62828 !important;
        }
        </style>
        """, unsafe_allow_html=True)


        st.markdown("""
        <style>
        /* Sidebar background & theme-aware text */
        [data-testid="stSidebar"] {
            background-color: var(--background-color);
            color: var(--text-color);
        }

        /* Profile Card - theme aware */
        .sidebar-profile {
            background: radial-gradient(circle at top right, rgb(0 98 132), #1988af 70%);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 20px;
            padding: 1.2rem;
            text-align: center;
            margin: 1rem 0;
            box-shadow: 0 0 15px rgba(0, 0, 0, 0.1);
        }
        body[data-theme="light"] .sidebar-profile {
            background-color: rgba(240, 248, 255, 0.6);
            border: 1px solid rgba(0, 123, 255, 0.15);
        }
        .sidebar-profile:hover {
            box-shadow: 0 0 20px rgba(0, 123, 255, 0.2);
        }

        /* Text inside profile */
        .sidebar-profile h3 {
            color: var(--text-color);
            font-weight: 600;
            margin-bottom: 0.4rem;
        }
        .sidebar-profile small {
            color: var(--secondary-text-color);
            font-size: 0.8rem;
        }

        /* Search box */
        [data-testid="stTextInput"] input {
            border-radius: 10px;
            border: 1px solid rgba(0, 123, 255, 0.4);
        }

        /* Buttons */
        div[data-testid="stButton"] > button {
            width: 100%;
            border-radius: 12px;
            background-color: #007BFF;
            color: white;
            font-weight: 500;
            transition: all 0.2s ease;
        }
        div[data-testid="stButton"] > button:hover {
            background-color: #fff;
        }

        /* Section labels */
        .sidebar-section-label {
            color: var(--text-color);
            font-size: 0.8rem;
            opacity: 0.7;
            text-transform: uppercase;
            margin-top: 1rem;
        }
        .stButton > button, .stDownloadButton > button {
            background: #006894 !important;
            color: #ffffff !important;
            border: 1px solid #dcdcdc !important;
            border-radius: 12px !important;
            padding: 0.6rem 1.5rem !important;
            font-weight: 500 !important;
            font-size: 0.95rem !important;
            transition: all 0.25s ease !important;
            height: 2.6rem !important;
            white-space: nowrap !important; /* keeps text in one line */
            text-align: center !important;
        }

        .stButton > button:hover, .stDownloadButton > button:hover {
            background: #007aa1 !important;
            color: white !important;
            border: none !important;
            box-shadow: 0 6px 18px rgba(66,133,244,0.25);
            transform: translateY(-2px);
        }
        </style>
        """, unsafe_allow_html=True)

        st.markdown("""
        <style>
        /* Project switcher styles */
        .project-switcher-card {
            background: rgba(255,255,255,0.05);
            border-radius: 10px;
            padding: 12px;
            margin: 8px 0;
            border: 1px solid rgba(255,255,255,0.1);
        }

        .current-project-badge {
            background: linear-gradient(135deg, #667eea, #764ba2);
            color: white;
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: 600;
            display: inline-block;
            margin: 4px 0;
        }

        [data-testid="stSidebar"][aria-expanded="false"] .project-switcher-card {
            padding: 8px !important;
            margin: 4px 0 !important;
        }
        </style>
        """, unsafe_allow_html=True)
                



        # --- Sidebar Layout ---
        with st.sidebar:
            # --- Profile Section ---
            st.markdown(f"""
            <div class="profile-card">
                <div class="profile-initial">{username[0].upper()}</div>
                <div><strong>{username}</strong></div>
                <div style="font-size:12px;color:black;">{email}</div>
                <div style="font-size:12px;color:black;">Role: {role}</div>
                <div style="font-size: 11px;color: rgb(255, 255, 255);margin-top: 8px;padding: 4px 8px;background: rgb(0 104 148);border-radius: 6px; !important;">
                    Project: <strong>{st.session_state.get("selected_project", "None")}</strong>
                </div>
            </div>
            """, unsafe_allow_html=True)
            st.markdown('<hr style="border: 0.2px solid black; margin-top: 24px; margin-bottom: 0;">', unsafe_allow_html=True)
            st.markdown("<div class='section-label'>Filters</div>", unsafe_allow_html=True)
            search_query = st.text_input("Search", placeholder="Search here...", label_visibility="collapsed")

            # === ENHANCED PROJECT SWITCHER WITH MODAL ===
            if role.upper() != "CLIENT":
                st.markdown("---")
                st.markdown("<div class='section-label'>Admin Controls</div>", unsafe_allow_html=True)
                
                current_project = st.session_state.get("selected_project", "")
                available_projects = list(schema_value.keys())
                
                # Display current project
                # st.info(f"**Current:** {current_project}")
                
                # Project selection dropdown
                selected_new_project = st.selectbox(
                    "Switch to Project",
                    available_projects,
                    index=available_projects.index(current_project) if current_project in available_projects else 0,
                    key="project_selector",
                    help="Select a project to switch to",
                    label_visibility="collapsed"
                )
                
                #  Direct switch button (no confirmation)
                if selected_new_project != current_project:
                    if st.button("🔄 Switch Project", use_container_width=True, key="switch_project_direct"):
                        # Perform the switch immediately
                        st.session_state["selected_project"] = selected_new_project
                        st.session_state["schema"] = schema_value[selected_new_project]
                        
                        # Clear cached data
                        keys_to_clear = ['wkday_raw_df', 'wkend_raw_df', 'filtered_wkday_df', 'filtered_wkend_df']
                        for key in keys_to_clear:
                            if key in st.session_state:
                                del st.session_state[key]
                        
                        # Show success message
                        st.session_state["show_switch_success"] = True
                        st.session_state["success_project_name"] = selected_new_project
                        st.rerun()
                
                # === AGENCY DROPDOWN FOR LACMTA_FEEDER ===
                if selected_project == "lacmta_feeder":
                    st.markdown("<div class='section-label'>Agency Filter</div>", unsafe_allow_html=True)
                    
                    # Fetch agency names from details file
                    agency_names = get_agency_names("lacmta_feeder")

                    if not agency_names:
                        st.info("No agencies found in details file.")
                    else:
                        # Add "All" option at the beginning
                        agency_options = ["All"] + agency_names
                        
                        # Initialize selected agency in session state
                        if "selected_agency" not in st.session_state:
                            st.session_state.selected_agency = "All"
                        
                        # Agency dropdown
                        selected_agency = st.selectbox(
                            "Select Agency",
                            agency_options,
                            index=agency_options.index(st.session_state.selected_agency) if st.session_state.selected_agency in agency_options else 0,
                            key="agency_selector",
                            help="Filter data by agency",
                            label_visibility="collapsed"
                        )
                        
                        # Update session state if agency changed
                        if selected_agency != st.session_state.get("selected_agency"):
                            st.session_state.selected_agency = selected_agency
                            st.rerun()
                        
                        # Show current selection
                        if st.session_state.selected_agency != "All":
                            st.info(f"**Filtering by:** {st.session_state.selected_agency}")

            

            st.markdown("<div class='section-label'>Dashboard Pages</div>", unsafe_allow_html=True)

            # --- Menu Items ---
            if role.upper() == "CLIENT":
                # Show only these 4 pages for CLIENT role
                menu_items = [
                    "🏠︎   Home",
                    "🗓︎   WEEKDAY-OVERALL", 
                    "☀︎   WEEKEND-OVERALL",
                    "🕒  Time Of Day Details",
                    "🗺️  Location Maps",
                    "👥  Demographic Review",
                    "🚫  Refusal Analysis"
                ]
            else:
                menu_items = [
                    "🏠︎   Home",
                    "🗓︎   WEEKDAY-OVERALL",
                    "☀︎   WEEKEND-OVERALL",
                    "🕒  Time Of Day Details"
                ]

                if 'actransit' in selected_project or 'salem' in selected_project or 'lacmta_feeder' in selected_project:
                    menu_items.extend(["🚫  Refusal Analysis", "⤓    LOW RESPONSE QUESTIONS",
                    "🗺️  Location Maps","👥  Demographic Review"])

                if 'kcata' in selected_project or ('actransit' in selected_project or 'salem' in selected_project or 'lacmta_feeder' in selected_project and 'rail' not in selected_schema.lower()):
                    menu_items.append("↺   Clone Records")

                if any(p in selected_project for p in ['lacmta_feeder', 'kcata', 'actransit', 'salem']):
                    menu_items.extend(["⌗  DAILY TOTALS", "∆   Surveyor/Route/Trend Reports"])

                if 'rail' in selected_schema.lower():
                    menu_items.extend(["◉  WEEKDAY StationWise Comparison", "⦾  WEEKEND StationWise Comparison"])

            # --- Session State ---
            if "selected_page" not in st.session_state:
                st.session_state.selected_page = "🏠︎   Home"
            
            # Check if current page is a management page
            management_page_keys = ["accounts_management", "create_accounts", "password_update"]
            is_management_page = current_page in management_page_keys
            
            # Management page mapping
            management_mapping = {
                "accounts_management": "⚙️  Accounts Management",
                "create_accounts": "➕  Create Accounts",
                "password_update": "🔐  Password Update"
            }
            
            # --- Sync state from URL/current_page BEFORE rendering widgets ---
            if not is_management_page:
                # dashboard page
                if current_page in REVERSE_PAGE_MAPPING:
                    st.session_state.selected_page = REVERSE_PAGE_MAPPING[current_page]
                if "selected_management_page" in st.session_state:
                    del st.session_state.selected_management_page
            else:
                # management page
                st.session_state.selected_page = "🏠︎   Home"
                if current_page in management_mapping:
                    st.session_state.selected_management_page = management_mapping[current_page]

            # Ensure widget keys exist and are consistent
            if "sidebar_menu" not in st.session_state:
                st.session_state.sidebar_menu = st.session_state.selected_page

            def on_dashboard_change():
                # Called when dashboard dropdown changes
                new_val = st.session_state.sidebar_menu
                if new_val != st.session_state.selected_page:
                    st.session_state.selected_page = new_val
                    if "selected_management_page" in st.session_state:
                        del st.session_state.selected_management_page
                    st.query_params["page"] = get_page_key(new_val)
                    st.rerun()

            # --- Dashboard dropdown (NO dynamic index) ---
            st.session_state.sidebar_menu = st.session_state.selected_page
            st.selectbox(
                "",
                menu_items,
                key="sidebar_menu",
                label_visibility="collapsed",
                on_change=on_dashboard_change,
            )

            # --- Management section for super admins ---
            current_user_email = st.session_state.get("user", {}).get("email", "")
            if is_super_admin(current_user_email):
                st.markdown(
                    '<hr style="border: 0.2px solid black; margin-top: 24px; margin-bottom: 12px;">',
                    unsafe_allow_html=True,
                )
                st.markdown("<div class='section-label'>Management</div>", unsafe_allow_html=True)

                # Single button to go to Accounts Management page
                if st.button("Accounts Management", use_container_width=True, type="primary"):
                    st.session_state.selected_page = "🏠︎   Home"
                    if "selected_management_page" in st.session_state:
                        del st.session_state.selected_management_page
                    st.query_params["page"] = "accounts_management"
                    st.rerun()

            # --- Bottom Buttons ---
            st.markdown('<hr style="border: 0.2px solid black; margin-top: 24px; margin-bottom: 0;">', unsafe_allow_html=True)
            st.markdown('<div class="bottom-buttons">', unsafe_allow_html=True)
            if st.button('Change Password', key='ChangePasswordButton'):
                send_change_password_email(st.session_state['user']['email'])
            if st.button("Logout", key='LogoutButton'):
                logout()
            st.markdown('</div>', unsafe_allow_html=True) 


        # === ADD PROFESSIONAL HEADER HERE - REPLACE YOUR CURRENT HEADER ===
        def create_professional_header():
            from zoneinfo import ZoneInfo

            # =====================================================
            # GET LAST SYNC DATE FROM SNOWFLAKE
            # =====================================================
            # Try to get Last_Sync_Date from last_survey_date_df
            if not last_survey_date_df.empty and 'Last_Sync_Date' in last_survey_date_df.columns:
                last_sync_value = last_survey_date_df['Last_Sync_Date'].iloc[0]
                if pd.notna(last_sync_value):
                    try:
                        # Convert to datetime - handle both string and datetime objects
                        last_sync_datetime = pd.to_datetime(last_sync_value, errors='coerce')
                        
                        if pd.notna(last_sync_datetime):
                            # If it's a pandas Timestamp, convert to datetime
                            if isinstance(last_sync_datetime, pd.Timestamp):
                                last_sync_datetime = last_sync_datetime.to_pydatetime()
                            
                            # Convert to America/Chicago timezone if needed
                            if last_sync_datetime.tzinfo is None:
                                last_sync_datetime = last_sync_datetime.replace(tzinfo=ZoneInfo("America/Chicago"))
                            else:
                                last_sync_datetime = last_sync_datetime.astimezone(ZoneInfo("America/Chicago"))
                            
                            formatted_date = last_sync_datetime.strftime("%Y-%m-%d %H:%M:%S %Z")
                        else:
                            # Fallback to current time if parsing fails
                            last_refresh_utc = datetime.datetime.now(ZoneInfo("America/Chicago"))
                            formatted_date = last_refresh_utc.strftime("%Y-%m-%d %H:%M:%S %Z")
                    except Exception as e:
                        # Fallback to current time if any error occurs
                        print(f"Error parsing Last_Sync_Date: {e}")
                        last_refresh_utc = datetime.datetime.now(ZoneInfo("America/Chicago"))
                        formatted_date = last_refresh_utc.strftime("%Y-%m-%d %H:%M:%S %Z")
                else:
                    # Fallback to current time if value is NaN
                    last_refresh_utc = datetime.datetime.now(ZoneInfo("America/Chicago"))
                    formatted_date = last_refresh_utc.strftime("%Y-%m-%d %H:%M:%S %Z")
            else:
                # Fallback to current time if dataframe is empty or column doesn't exist
                last_refresh_utc = datetime.datetime.now(ZoneInfo("America/Chicago"))
                formatted_date = last_refresh_utc.strftime("%Y-%m-%d %H:%M:%S %Z")

            # === DATE SETUP ===
            current_date = datetime.datetime.now()

            # Get most recent "Completed" date
            if 'kcata' in selected_project or 'kcata_rail' in selected_project or 'actransit' in selected_project or 'salem' in selected_project or 'lacmta_feeder' in selected_project:
                if 'LocalTime' in wkday_raw_df.columns and 'LocalTime' in wkend_raw_df.columns:
                    completed_dates = pd.concat([wkday_raw_df['LocalTime'], wkend_raw_df['LocalTime']])
                else:
                    # Fallback to DATE_SUBMITTED if LocalTime doesn't exist
                    completed_dates = pd.concat([wkday_raw_df['DATE_SUBMITTED'], wkend_raw_df['DATE_SUBMITTED']])
            else:
                completed_dates = pd.concat([wkday_raw_df['Completed'], wkend_raw_df['Completed']])
            most_recent_completed_date = pd.to_datetime(completed_dates).max()

            # Determine total records (based on current page)
            if current_page == "weekend":
                total_records = int(wkend_df["# of Surveys"].sum())
            else:
                total_records = int(wkday_df["# of Surveys"].sum())

            # === STYLING ===
            st.markdown("""
            <style>
            .professional-header {
                background: linear-gradient(135deg, #356AE6 0%, #7AB8FF 100%);
                border-radius: 18px;
                padding: 2rem 2.5rem;
                margin-bottom: 2.2rem;
                color: #fff;
                position: relative;
                box-shadow: 0 6px 20px rgba(0, 0, 0, 0.08);
                border: 1px solid rgba(170, 200, 255, 0.6);
                overflow: hidden;
                margin-top: -80px;
            }
           
             .professional-header::before {
                content: '';
                position: absolute;
                inset: 0;
                background: rgb(0, 104, 148);;
                z-index: 0;
            }

            .header-content {
                display: flex;
                justify-content: space-between;
                align-items: center;
                position: relative;
                z-index: 1;
                gap: 1rem;
            }

            .header-left h1 {
                font-size: 2.2rem;
                font-weight: 700;
                letter-spacing: -0.5px;
                margin-bottom: 0.3rem;
                color: #fff;
            }

            .header-left p {
                font-size: 1.05rem;
                font-weight: 400;
                opacity: 0.9;
                margin: 0;
                color: #fff;
            }

            .metric-group {
                display: flex;
                gap: 0.8rem;
                flex-wrap: wrap;
                justify-content: flex-end;
            }

            .metric-card {
                background: rgba(255,255,255,0.6);
                backdrop-filter: blur(12px);
                border-radius: 10px;
                border: 1px solid rgba(180, 200, 255, 0.5);
                padding: 0.75rem 1rem;
                text-align: center;
                min-width: 160px;
                transition: all 0.25s ease;
                box-shadow: 0 2px 6px rgba(0,0,0,0.05);
        }

        .metric-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.08);
            background: rgba(255,255,255,0.7);
        }

        .metric-label {
            font-size: 0.8rem;
            font-weight: 500;
            color: #123f72;
            margin-bottom: 4px;
        }

        .metric-value {
            font-size: 1.05rem;
            font-weight: 600;
            color: #1e3a5f;
            background: #ffffff;
            padding: 4px 10px;
            border-radius: 6px;
            display: inline-block;
            font-family: 'Roboto Mono', 'Menlo', monospace;
            letter-spacing: -0.3px;
        }

        @media (max-width: 1100px) {
            .header-content {
                flex-direction: column;
                align-items: flex-start;
                gap: 1.2rem;
            }
            .metric-group {
                justify-content: flex-start;
            }
            .header-left h1 {
                font-size: 1.9rem;
            }
        }
        </style>
        """, unsafe_allow_html=True)

            # === HEADER CONTENT ===
            st.markdown(f"""
            <div class="professional-header">
                <div class="header-content">
                    <div class="header-left">
                        <h1>Completion Report</h1>
                        <p>Comprehensive Route Performance Overview</p>
                    </div>
                    <div class="metric-group">
                        <div class="metric-card">
                            <p class="metric-label">Total Records</p>
                            <p class="metric-value">{total_records:,}</p>
                        </div>
                        <div class="metric-card">
                            <p class="metric-label">⏱ Last Refresh</p>
                            <p class="metric-value">{formatted_date}</p>
                        </div>
                        <div class="metric-card">
                            <p class="metric-label">Last Completed</p>
                            <p class="metric-value">{most_recent_completed_date.strftime('%Y-%m-%d %H:%M:%S')}</p>
                        </div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        # Call the function right after your sidebar
        create_professional_header()
        # === END PROFESSIONAL HEADER ===

        # === SUCCESS MESSAGE (Auto-disappearing) ===
        if st.session_state.get("show_switch_success", False):
            success_project = st.session_state.get("success_project_name", "")
            
            # Success message that auto-disappears
            success_placeholder = st.empty()
            with success_placeholder.container():
                st.success(f"✅ Successfully switched to **{success_project}**! Loading new data...")
            
            # Auto-remove after 2 seconds
            import time
            time.sleep(2)
            success_placeholder.empty()
            
            # Clear the success state
            del st.session_state["show_switch_success"]
            del st.session_state["success_project_name"]
####################################################################################################



        def filter_dataframe(df, query):
            if query:
                df = df[df.apply(lambda row: row.astype(str).str.contains(query, case=False).any(), axis=1)]

            return df

        def time_details(details_df):
            if 'kcata' in selected_project:
                # Check if columns exist before renaming
                column_mapping = {
                    'OPPO_TIME[CODE]': 'Time Period Code',
                    'TIME_ON[Code]': 'Time Code',
                    'TIME_ON': 'Time Description',
                    'TIME_PERIOD[Code]': 'Period Code',
                    'TIME_PERIOD': 'Period Description',
                    'START_TIME': 'Start Time',
                    'AGENCY': 'Agency',
                    'WKEND_TIME_PERIOD[Code]': 'Weekend Period Code',
                    'WKEND_TIME_PERIOD': 'Weekend Period Description'
                }
                details_df = details_df.rename(columns=column_mapping)
            
            st.dataframe(details_df, height=670, use_container_width=True)
            
            if st.button("Home Page"):
                st.query_params["page"] = "main"
                st.rerun()


        
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

        def main_page(data1, data2, data3):
            """Main page display with dynamic data"""
            # -------------------------------
            # Overall Dashboard Description with full table
            # -------------------------------
            if selected_project == "lacmta_feeder":
                with st.expander(" Overview of Time Periods and Column Definitions", expanded=False):
                    st.markdown("**This table explains how each time period (1–4) maps to actual survey codes and times:**")

                    # Full table as Jason wanted
                    time_table = pd.DataFrame([
                        [1, 'AM PEAK', 'AM1', 'Before 5:00 am'],
                        [1, 'AM PEAK', 'AM2', '5:00 am - 6:00 am'],
                        [1, 'AM PEAK', 'AM3', '6:00 am - 7:00 am'],
                        [1, 'AM PEAK', 'MID1', '7:00 am - 8:00 am'],
                        [1, 'AM PEAK', 'MID2', '8:00 am - 9:00 am'],
                        [2, 'MIDDAY', 'MID7', '9:00 am - 10:00 am'],
                        [2, 'MIDDAY', 'MID3', '10:00 am - 11:00 am'],
                        [2, 'MIDDAY', 'MID4', '11:00 am - 12:00 pm'],
                        [2, 'MIDDAY', 'MID5', '12:00 pm - 1:00 pm'],
                        [2, 'MIDDAY', 'MID6', '1:00 pm - 2:00 pm'],
                        [3, 'PM PEAK', 'PM1', '2:00 pm - 3:00 pm'],
                        [3, 'PM PEAK', 'PM2', '3:00 pm - 4:00 pm'],
                        [3, 'PM PEAK', 'PM3', '4:00 pm - 5:00 pm'],
                        [3, 'PM PEAK', 'PM4', '5:00 pm - 6:00 pm'],
                        [3, 'PM PEAK', 'PM5', '6:00 pm - 7:00 pm'],
                        [4, 'EVENING', 'PM6', '7:00 pm - 8:00 pm'],
                        [4, 'EVENING', 'PM7', '8:00 pm - 9:00 pm'],
                        [4, 'EVENING', 'PM8', '9:00 pm - 10:00 pm'],
                        [4, 'EVENING', 'PM9', 'After 10:00 pm'],
                    ], columns=["TIME_PERIOD[Code]", "TIME_PERIOD", "TIME_ON[Code]", "TIME_ON"])

                    st.dataframe(time_table, use_container_width=True, hide_index=True)

                    # Description of Goal / Collect / Remain
                    st.markdown("""
                    **Column Definitions (applies to all tables below):**  
                    - **Goal** → Planned number of surveys for this time period  
                    - **Collect** → Surveys actually completed  
                    - **Remain** → Surveys still needed to meet the goal (Goal - Collect, minimum 0)  
                    """)

            # -------------------------------
            # Columns Layout
            # -------------------------------
            col1, col2 = st.columns([2, 1])  # Left column is wider

            # Display the first dataframe on the left full screen (col1)
            with col1:
                if current_page=='main':
                    st.subheader('Route Direction Level Comparison (WeekDAY)')
                else:
                    st.subheader("Route Direction Level Comparison")
                filtered_df1 = filter_dataframe(data1, search_query)
                # render_aggrid(filtered_df1, height=500, pinned_column='ROUTE_SURVEYEDCode', key='grid1')
                render_styled_dataframe(filtered_df1, height=500, key='grid1')


                # st.dataframe(filtered_df1, use_container_width=True, hide_index=True)
                def append_total_row(df):
                    if 'Remaining' not in df.columns:
                        return df

                    total_remaining = int(df['Remaining'].fillna(0).sum())

                    total_row = {}

                    for col in df.columns:
                        if col == 'Remaining':
                            total_row[col] = total_remaining
                        elif df[col].dtype.kind in 'if':  # int or float columns
                            total_row[col] = 0
                        else:
                            total_row[col] = ''

                    # Put label in first column
                    total_row[df.columns[0]] = 'TOTAL'

                    return pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)


                
                filtered_df3 = filter_dataframe(data3, search_query)

                # Append TOTAL row
                filtered_df3 = append_total_row(filtered_df3)
                st.subheader("Route Level Comparison")
                render_styled_dataframe(filtered_df3, height=400, key='grid3')
                # 🔒 Locked summary (TOP)
                # total_remaining = get_total_remaining(filtered_df3)
                # st.metric(
                #     label="Total Remaining Surveys (All Routes)",
                #     value=f"{total_remaining:,}"
                # )
                # st.dataframe(filtered_df3, use_container_width=True, hide_index=True)


            # Display buttons and dataframes in the second column (col2)
            # with col2:

            #     st.subheader("Time Range Data")
            #     # Convert relevant columns in both dataframes to numeric values, handling errors
            #     data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']] = data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']].apply(pd.to_numeric, errors='coerce')
            #     data2[['1', '2', '3', '4', '5']] = data2[['1', '2', '3', '4', '5']].apply(pd.to_numeric, errors='coerce')

            #     # Fill any NaN values with 0 (or handle them differently if needed)
            #     data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']] = data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']].fillna(0)
            #     data2[['1', '2', '3', '4', '5']] = data2[['1', '2', '3', '4', '5']].fillna(0)

            #     # Calculate the sums for expected and collected totals
            #     expected_totals = data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']].sum()
            #     collected_totals = data2[['1', '2', '3', '4', '5']].sum()

            #     # Calculate the difference, ensuring no negative values
            #     difference = np.maximum(expected_totals.values - collected_totals.values, 0)
            #     result_df = pd.DataFrame({
            #         'Time Period':  [ '1', '2', '3', '4', '5'],
            #         'Collected Totals': collected_totals.values.astype(int),
            #         'Expected Totals': expected_totals.values.astype(int),
            #         'Remaining': difference.astype(int),
            #     })



            #     filtered_df2 = filter_dataframe(data2, search_query)
            #     # render_aggrid(filtered_df2, height=500, pinned_column='Display_Text', key='grid2')
            #     render_styled_dataframe(filtered_df2, height=500, key='grid2')
            #     # st.dataframe(filtered_df2, use_container_width=True, hide_index=True)

            #     filtered_df4 = filter_dataframe(result_df, search_query)
            
            #     # Render AgGrid
            #     st.subheader("Time Period OverAll Data")
            #     render_styled_dataframe(filtered_df4, height=400, key='grid4')
            with col2:
                st.subheader("Time Range Data")

                # -------------------------------
                # Dynamically detect time-period columns
                # -------------------------------
                expected_cols = [col for col in data1.columns if col.endswith("Goal")]
                collected_cols = [col for col in data2.columns if col.isdigit()]

                # Sort columns numerically to keep correct order
                expected_cols = sorted(
                    expected_cols,
                    key=lambda x: int(x.split("(")[1].split(")")[0])
                )
                collected_cols = sorted(collected_cols, key=lambda x: int(x))

                # -------------------------------
                # Convert to numeric safely
                # -------------------------------
                data1[expected_cols] = data1[expected_cols].apply(
                    pd.to_numeric, errors='coerce'
                )
                data2[collected_cols] = data2[collected_cols].apply(
                    pd.to_numeric, errors='coerce'
                )

                # Fill NaNs
                data1[expected_cols] = data1[expected_cols].fillna(0)
                data2[collected_cols] = data2[collected_cols].fillna(0)

                # -------------------------------
                # Calculate totals
                # -------------------------------
                expected_totals = data1[expected_cols].sum()
                collected_totals = data2[collected_cols].sum()

                # Ensure no negative remaining values
                difference = np.maximum(
                    expected_totals.values - collected_totals.values, 0
                )

                # -------------------------------
                # Build result dataframe dynamically
                # -------------------------------
                time_periods = [str(i + 1) for i in range(len(expected_cols))]

                result_df = pd.DataFrame({
                    'Time Period': time_periods,
                    'Collected Totals': collected_totals.values.astype(int),
                    'Expected Totals': expected_totals.values.astype(int),
                    'Remaining': difference.astype(int),
                })

                # -------------------------------
                # Render dataframes (unchanged behavior)
                # -------------------------------
                filtered_df2 = filter_dataframe(data2, search_query)
                render_styled_dataframe(filtered_df2, height=500, key='grid2')

                filtered_df4 = filter_dataframe(result_df, search_query)

                st.subheader("Time Period OverAll Data")
                render_styled_dataframe(filtered_df4, height=400, key='grid4')


        def weekday_page():
            st.title("Weekday OverAll Data")
            if 'uta' in selected_project:
                wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED','STATION_ID',  '(0) Collect', '(0) Remain','(1) Collect', '(1) Remain',
                                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                                        '(0) Goal','(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal','(5) Goal']
                wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4','5']
            elif 'tucson' in selected_project:
                wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                        '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']
            elif 'lacmta_feeder' in selected_project:
                wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                        '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']
            elif 'kcata' in selected_project or 'actransit' in selected_project or 'salem' in selected_project:
                wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                        '(5) Collect', '(5) Remain',
                                        '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']
                wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4','5']
            else:
                wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(0) Collect', '(0) Remain','(1) Collect', '(1) Remain',
                                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                                        '(0) Goal','(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal','(5) Goal']
                wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4','5']
                
            # day_column_present = check_all_characters_present(wkday_dir_df, ["day"])
            # if day_column_present:
            #     wkday_dir_columns.insert(2,day_column_present[0])
            main_page(wkday_dir_df[wkday_dir_columns],
                        wkday_time_df[wkday_time_columns],
                        wkday_df[['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', 'Route Level Goal', '# of Surveys', 'Remaining']])
            if st.button("Home Page"):
                st.query_params["page"] = "main"
                st.rerun()

        def weekend_page():
            st.title("Weekend OverAll Data")
            day_column_present = check_all_characters_present(wkend_dir_df, ["day"])
            route_level_day_column_present = check_all_characters_present(wkend_df, ["day"])
            if 'uta' in selected_project:

                # day_column_present = check_all_characters_present(wkend_dir_df, ["day"])
                print(day_column_present)
                if day_column_present:
                    # print(wkend_dir_columns)
                    print("Inside If Condition")
                    wkend_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED',day_column_present[0],'STATION_ID',  '(0) Collect', '(0) Remain','(1) Collect', '(1) Remain',
                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                        '(0) Goal','(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal','(5) Goal']
                else:
                    wkend_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED','STATION_ID',  '(0) Collect', '(0) Remain','(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                                            '(0) Goal','(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal','(5) Goal']
                wkend_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4','5']
                if route_level_day_column_present:
                    wkend_df_columns=['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED',route_level_day_column_present[0],'Route Level Goal', '# of Surveys', 'Remaining']
                else:
                    wkend_df_columns=['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED','Route Level Goal', '# of Surveys', 'Remaining']
            elif 'tucson' in selected_project:
                if day_column_present:
                    wkend_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', day_column_present[0],'(1) Collect', '(1) Remain',
                                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                        '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                else:
                    wkend_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                            '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                wkend_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']
                if route_level_day_column_present:
                    wkend_df_columns=['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED',route_level_day_column_present[0],'Route Level Goal', '# of Surveys', 'Remaining']
                else: 
                    wkend_df_columns=['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED','Route Level Goal', '# of Surveys', 'Remaining']
                # wkend_df_columns=['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED','Day' ,'Route Level Goal', '# of Surveys', 'Remaining']
            elif 'lacmta_feeder' in selected_project:
                wkend_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                        '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                wkend_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']
                wkend_df_columns=['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED','Route Level Goal', '# of Surveys', 'Remaining']
            elif 'kcata' in selected_project or 'actransit' in selected_project or 'salem' in selected_project:
                for col in ['(5) Collect', '(5) Remain', '(5) Goal']:
                    if col not in wkend_dir_df.columns:
                        wkend_dir_df[col] = 0

                wkend_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                        '(2) Collect', '(2) Remain', 
                                        '(3) Collect', '(3) Remain', 
                                        '(4) Collect', '(4) Remain',
                                        '(5) Collect', '(5) Remain',
                                        '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']
                wkend_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4','5']
                wkend_df_columns=['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED','Route Level Goal', '# of Surveys', 'Remaining']
            else:
                if day_column_present:
                    wkend_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', day_column_present[0],'(0) Collect', '(0) Remain','(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                                            '(0) Goal','(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal','(5) Goal']
                else:
                    wkend_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(0) Collect', '(0) Remain','(1) Collect', '(1) Remain',
                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                        '(0) Goal','(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal','(5) Goal']
                wkend_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4','5']
                if route_level_day_column_present:
                    wkend_df_columns=['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED',route_level_day_column_present,'Route Level Goal', '# of Surveys', 'Remaining']
                else:
                    wkend_df_columns=['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED','Route Level Goal', '# of Surveys', 'Remaining']

            main_page(wkend_dir_df[wkend_dir_columns],
                    wkend_time_df[wkend_time_columns],
                    wkend_df[wkend_df_columns])

            if st.button("Home Page"):
                st.query_params["page"] = "main"
                st.rerun()

        # if "page_type" not in st.session_state:
        #     st.session_state["page_type"] = "weekday"  # Default page
        def weekday_station_page():
            st.subheader('Route StationWise Comparison(WeekDAY)')
            if 'tucson' in selected_project:
                wkday_stationwise_columns=[ 'ROUTE_SURVEYEDCode','ROUTE_SURVEYED','STATION_ID','STATION_NAME','(1) Collect', '(1) Remain',
                                    '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', 
                                     '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
            elif 'kcata rail' in selected_project:    
                wkday_stationwise_columns=[ 'ROUTE_SURVEYEDCode','ROUTE_SURVEYED','STATION_ID','STATION_NAME','(1) Collect', '(1) Remain',
                                    '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(5) Collect', '(5) Remain',
                                     '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']
            else:
                wkday_stationwise_columns=[ 'ROUTE_SURVEYEDCode','ROUTE_SURVEYED','STATION_ID','STATION_NAME', '(0) Collect', '(0) Remain', '(1) Collect', '(1) Remain',
                    '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(5) Collect', '(5) Remain',
                    '(0) Goal', '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']
            filtered_df = filter_dataframe(wkday_stationwise_df[wkday_stationwise_columns], search_query)

            # render_aggrid(filtered_df,500,'ROUTE_SURVEYEDCode',1)
            st.dataframe(filtered_df, use_container_width=True, hide_index=True)

            if st.button("Home Page"):
                st.query_params()
                st.rerun()

        def weekend_station_page():
            st.subheader('Route StationWise Comparison(WeekEND)')
            if 'tucson' in selected_project or 'kcata_rail' in selected_project:
                wkend_stationwise_columns=[ 'ROUTE_SURVEYEDCode','ROUTE_SURVEYED','STATION_ID','STATION_NAME','(1) Collect', '(1) Remain',
                                    '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', 
                                     '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
            elif 'kcata rail' in selected_project:    
                wkend_stationwise_columns=[ 'ROUTE_SURVEYEDCode','ROUTE_SURVEYED','STATION_ID','STATION_NAME','(1) Collect', '(1) Remain',
                                    '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(5) Collect', '(5) Remain',
                                     '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']
            else:
                wkend_stationwise_columns=[ 'ROUTE_SURVEYEDCode','ROUTE_SURVEYED','STATION_ID','STATION_NAME', '(0) Collect', '(0) Remain', '(1) Collect', '(1) Remain',
                    '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(5) Collect', '(5) Remain',
                    '(0) Goal', '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']

            filtered_df = filter_dataframe(wkend_stationwise_df[wkend_stationwise_columns], search_query)

            # render_aggrid(filtered_df,500,'ROUTE_SURVEYEDCode',1)
            st.dataframe(filtered_df, use_container_width=True, hide_index=True)
            if st.button("Home Page"):
                st.query_params()
                st.rerun()

        def low_response_questions_page():
            st.title("📋 LOW RESPONSE QUESTIONS")
            # Load the low response questions dataframe
            low_response_questions_df = dataframes['low_response_questions_df']
            
            # Check if the dataframe exists and is not empty
            if low_response_questions_df is not None and not low_response_questions_df.empty:
                # st.subheader("📋 LOW RESPONSE QUESTIONS")
                st.dataframe(low_response_questions_df, use_container_width=True, hide_index=True)
            else:
                st.warning("No low response questions data available.")

            # Navigation
            if st.button("🔙 Home Page"):
                st.query_params["page"] = "main"
                st.rerun()

        def daily_totals_page():
            if 'stl' in selected_project or 'kcata' in selected_project or 'actransit' in selected_project or 'salem' in selected_project or 'lacmta_feeder' in selected_project:
                st.title("Daily Totals - Interviewer and Route Level")
                
                # Create tabs
                tab1, tab2 = st.tabs(["Daily Totals Summary", "Interviewer Records"])
                
                with tab1:
                    # Load Snowflake-extracted DataFrames
                    by_interv_totals_df = dataframes['by_interv_totals_df']
                    by_route_totals_df = dataframes['by_route_totals_df']
                    survey_detail_totals_df = dataframes['survey_detail_totals_df']

                    # Standardize column names to uppercase for consistent access
                    survey_detail_totals_df.columns = survey_detail_totals_df.columns.astype(str).str.strip().str.upper()

                    # Ensure DATE column exists
                    if 'DATE' not in survey_detail_totals_df.columns:
                        st.error("❌ 'DATE' column not found in survey_detail_totals_df.")
                        st.stop()

                    # Convert DATE column to datetime.date and handle errors
                    survey_detail_totals_df['DATE'] = pd.to_datetime(survey_detail_totals_df['DATE'], errors='coerce').dt.date

                    # Extract unique values for filters
                    all_dates = sorted(survey_detail_totals_df['DATE'].dropna().astype(str).unique())
                    all_intervs = sorted(survey_detail_totals_df['INTERV_INIT'].dropna().unique())

                    # Summary cards
                    total_surveys = survey_detail_totals_df['COUNT'].sum()
                    unique_interviewers = survey_detail_totals_df['INTERV_INIT'].nunique()
                    active_routes = survey_detail_totals_df['ROUTE'].nunique()

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Surveys", f"{total_surveys:,}")
                    with col2:
                        st.metric("Unique Interviewers", unique_interviewers)
                    with col3:
                        st.metric("Active Routes", active_routes)

                    # Create filter controls in a single row at the top - Interviewer first, then Date
                    filter_col1, filter_col2 = st.columns(2)
                    with filter_col1:
                        selected_interv = st.selectbox(
                            "Filter by Interviewer:",
                            options=[None] + list(all_intervs),
                            format_func=lambda x: x if x else "— All Interviewers —"
                        )
                    with filter_col2:
                        selected_date = st.selectbox(
                            "Filter by Date:",
                            options=[None] + list(all_dates),
                            format_func=lambda x: x if x else "— All Dates —"
                        )

                    # Function to add total row
                    def add_total_row(df, index_col):
                        if not df.empty:
                            total_row = df.select_dtypes(include=['number']).sum()
                            total_row[index_col] = 'Total'
                            return pd.concat([df, total_row.to_frame().T], ignore_index=True)
                        return df

                    # Function to filter columns while keeping all rows
                    def filter_date_columns(df, date_col, selected_date_str):
                        if selected_date_str is None:
                            return df
                        
                        # Keep index column, selected date column (if exists), and Total column
                        cols_to_keep = [df.columns[0]]  # First column (index)
                        
                        # Find the matching date column
                        date_cols = [col for col in df.columns if str(col).split()[0] == selected_date_str]
                        if date_cols:
                            cols_to_keep.append(date_cols[0])
                        
                        # Keep Total column if it exists
                        if 'Total' in df.columns:
                            cols_to_keep.append('Total')
                        
                        return df[cols_to_keep]

                    # Function to format date columns
                    def format_date_columns(df):
                        # Format date columns (those that can be parsed as dates)
                        for col in df.columns:
                            try:
                                # Try to parse the column name as a date
                                pd.to_datetime(col)
                                # If successful, format it
                                df = df.rename(columns={col: str(col).split()[0]})
                            except:
                                continue
                        return df

                    # Process data based on filters
                    if selected_date or selected_interv:
                        # Filter the detail data based on selections
                        filtered_detail = survey_detail_totals_df.copy()
                        
                        if selected_date:
                            filtered_detail = filtered_detail[filtered_detail['DATE'].astype(str) == selected_date]
                        
                        if selected_interv:
                            filtered_detail = filtered_detail[filtered_detail['INTERV_INIT'] == selected_interv]

                        # Process interviewer data
                        if not filtered_detail.empty:
                            # Pivot interviewer data
                            interv_filtered = filtered_detail.pivot_table(
                                index='INTERV_INIT',
                                columns='DATE',
                                values='COUNT',
                                aggfunc='sum',
                                fill_value=0
                            ).reset_index()
                            
                            # Add Total column
                            interv_filtered['Total'] = interv_filtered.select_dtypes(include=['number']).sum(axis=1)
                            
                            # Format date columns
                            interv_filtered = format_date_columns(interv_filtered)
                            
                            # Add Total row
                            interv_filtered_with_total = add_total_row(interv_filtered, 'INTERV_INIT')
                            
                            # Filter columns if date is selected
                            if selected_date:
                                interv_filtered_with_total = filter_date_columns(interv_filtered_with_total, 'DATE', selected_date)
                        else:
                            # Create empty DataFrame with correct structure
                            columns = ['INTERV_INIT'] + ([selected_date] if selected_date else []) + ['Total']
                            interv_filtered_with_total = pd.DataFrame(columns=columns)

                        # Process route data
                        if not filtered_detail.empty:
                            # Pivot route data
                            route_filtered = filtered_detail.pivot_table(
                                index='ROUTE',
                                columns='DATE',
                                values='COUNT',
                                aggfunc='sum',
                                fill_value=0
                            ).reset_index()
                            
                            # Add Total column
                            route_filtered['Total'] = route_filtered.select_dtypes(include=['number']).sum(axis=1)
                            
                            # Format date columns
                            route_filtered = format_date_columns(route_filtered)
                            
                            # Add Total row
                            route_filtered_with_total = add_total_row(route_filtered, 'ROUTE')
                            
                            # Filter columns if date is selected
                            if selected_date:
                                route_filtered_with_total = filter_date_columns(route_filtered_with_total, 'DATE', selected_date)
                        else:
                            # Create empty DataFrame with correct structure
                            columns = ['ROUTE'] + ([selected_date] if selected_date else []) + ['Total']
                            route_filtered_with_total = pd.DataFrame(columns=columns)
                    else:
                        
                        # Format date columns in the original dataframes if needed
                        interv_filtered_with_total = format_date_columns(by_interv_totals_df)
                        route_filtered_with_total = format_date_columns(by_route_totals_df)
                        

                    # Main content layout
                    col1, col2 = st.columns([1.3, 2])
                    
                    with col1:
                        st.subheader("👤 Interviewer Totals")
                        st.dataframe(interv_filtered_with_total, use_container_width=True, hide_index=True)

                    with col2:
                        st.subheader("🛣️ Route Totals")
                        st.dataframe(route_filtered_with_total, use_container_width=True, hide_index=True)

                    # Navigation
                    if st.button("🔙 Home Page", key="daily_totals_home"):
                        st.query_params["page"] = "main"
                        st.rerun()
                
                with tab2:
                    st.subheader("Interviewer Records - Field Supervisor View")
                    st.markdown("**View all records created by an interviewer for a specific day**")
                    
                    try:
                        # Fetch the dataset from elvis database
                        from automated_refresh_flow_new import PROJECTS, fetch_data
                        from automated_sync_flow_constants_maps import KCATA_HEADER_MAPPING
                        
                        project_config = PROJECTS[st.session_state["selected_project"]]
                        
                        # Use elvis database
                        if "elvis" in project_config["databases"]:
                            conf = project_config["databases"]["elvis"]
                            csv_buffer = fetch_data(conf["database"], conf["table"])
                            
                            if csv_buffer:
                                with st.spinner("🔄 Loading interviewer records data..."):
                                    csv_buffer.seek(0)
                                    interviewer_records_df = pd.read_csv(csv_buffer, low_memory=False)
                                    interviewer_records_df = interviewer_records_df.drop(index=0).reset_index(drop=True)
                                    
                                    # Apply header mapping if needed
                                    if selected_project in ["KCATA", "KCATA RAIL", "ACTRANSIT", "SALEM", "LACMTA_FEEDER"]:
                                        interviewer_records_df.columns = interviewer_records_df.columns.str.strip()
                                        interviewer_records_df = interviewer_records_df.rename(columns=KCATA_HEADER_MAPPING)
                                        # Remove first row if it's a header row
                                        if not interviewer_records_df.empty and interviewer_records_df.iloc[0].isnull().all():
                                            interviewer_records_df = interviewer_records_df.drop(index=0)
                                    
                                    interviewer_records_df = interviewer_records_df.reset_index(drop=True)
                                    
                                    # Apply LACMTA agency filter if needed
                                    selected_agency = st.session_state.get("selected_agency")
                                    if selected_project == "LACMTA_FEEDER" and selected_agency and selected_agency != "All":
                                        interviewer_records_df, _ = apply_lacmta_agency_filter(
                                            df=interviewer_records_df,
                                            project=selected_project,
                                            agency=selected_agency,
                                            bucket_name=os.getenv("bucket_name"),
                                            project_config=project_config
                                        )
                                    
                                    st.success(f"✅ Loaded {len(interviewer_records_df):,} records")
                            else:
                                st.error("❌ Failed to fetch data from database")
                                interviewer_records_df = pd.DataFrame()
                        else:
                            st.error("❌ Database configuration not found")
                            interviewer_records_df = pd.DataFrame()
                        
                        if not interviewer_records_df.empty:
                            # Get column names using check_all_characters_present
                            id_col = check_all_characters_present(interviewer_records_df, ['id'])
                            id_col = id_col[0] if id_col else None
                            
                            interv_init_col = check_all_characters_present(interviewer_records_df, ['intervinit'])
                            interv_init_col = interv_init_col[0] if interv_init_col else None
                            
                            localtime_col = check_all_characters_present(interviewer_records_df, ['localtime'])
                            localtime_col = localtime_col[0] if localtime_col else None
                            
                            route_code_col = check_all_characters_present(interviewer_records_df, ['routesurveyedcode'])
                            route_code_col = route_code_col[0] if route_code_col else None
                            
                            route_name_col = check_all_characters_present(interviewer_records_df, ['routesurveyed'])
                            route_name_col = route_name_col[0] if route_name_col else None
                            
                            have5min_code_col = check_all_characters_present(interviewer_records_df, ['have5minforsurvecode'])
                            have5min_code_col = have5min_code_col[0] if have5min_code_col else None
                            
                            have5min_col = check_all_characters_present(interviewer_records_df, ['have5minforsurve'])
                            have5min_col = have5min_col[0] if have5min_col else None
                            
                            timeon_code_col = check_all_characters_present(interviewer_records_df, ['timeoncode'])
                            timeon_code_col = timeon_code_col[0] if timeon_code_col else None
                            
                            timeon_col = check_all_characters_present(interviewer_records_df, ['timeon'])
                            timeon_col = timeon_col[0] if timeon_col else None
                            
                            register_win_code_col = check_all_characters_present(interviewer_records_df, ['registertowinyncode'])
                            register_win_code_col = register_win_code_col[0] if register_win_code_col else None
                            
                            register_win_col = check_all_characters_present(interviewer_records_df, ['registertowinyn'])
                            register_win_col = register_win_col[0] if register_win_col else None
                            
                            survey_lang_code_col = check_all_characters_present(interviewer_records_df, ['surveylanguagecode'])
                            survey_lang_code_col = survey_lang_code_col[0] if survey_lang_code_col else None
                            
                            survey_lang_col = check_all_characters_present(interviewer_records_df, ['surveylanguage'])
                            survey_lang_col = survey_lang_col[0] if survey_lang_col else None
                            
                            lat_col = check_all_characters_present(interviewer_records_df, ['elvisuserloc1lat'])
                            lat_col = lat_col[0] if lat_col else None
                            
                            lon_col = check_all_characters_present(interviewer_records_df, ['elvisuserloc1long'])
                            lon_col = lon_col[0] if lon_col else None
                            
                            # Check if required columns exist
                            required_cols = [id_col, interv_init_col, localtime_col]
                            missing_cols = [col for col in required_cols if col is None]
                            
                            if missing_cols:
                                st.error(f"❌ Missing required columns: {missing_cols}")
                                st.write("Available columns:", interviewer_records_df.columns.tolist()[:20])
                            else:
                                # Prepare the dataframe with standardized column names
                                display_df = interviewer_records_df.copy()
                                
                                # Convert LocalTime to datetime for filtering
                                if localtime_col:
                                    display_df['LocalTime'] = pd.to_datetime(display_df[localtime_col], errors='coerce')
                                    display_df['Date'] = display_df['LocalTime'].dt.date
                                
                                # Get unique dates and interviewers
                                if 'Date' in display_df.columns:
                                    all_dates = sorted(display_df['Date'].dropna().astype(str).unique())
                                else:
                                    all_dates = []
                                
                                if interv_init_col:
                                    all_intervs = sorted(display_df[interv_init_col].dropna().unique())
                                else:
                                    all_intervs = []
                                
                                # Filters
                                filter_col1, filter_col2 = st.columns(2)
                                with filter_col1:
                                    selected_date = st.selectbox(
                                        "Select Date:",
                                        options=[None] + list(all_dates),
                                        format_func=lambda x: x if x else "— Select Date —",
                                        key="interviewer_records_date"
                                    )
                                
                                with filter_col2:
                                    selected_interv = st.selectbox(
                                        "Select Interviewer (Initial):",
                                        options=[None] + list(all_intervs),
                                        format_func=lambda x: x if x else "— Select Interviewer —",
                                        key="interviewer_records_interv"
                                    )
                                
                                # Filter data
                                filtered_display_df = display_df.copy()
                                
                                if selected_date:
                                    filtered_display_df = filtered_display_df[filtered_display_df['Date'].astype(str) == selected_date]
                                
                                if selected_interv and interv_init_col:
                                    filtered_display_df = filtered_display_df[filtered_display_df[interv_init_col] == selected_interv]
                                
                                # Show record count
                                total_records = len(filtered_display_df)
                                st.info(f"📊 Showing {total_records:,} records")
                                
                                # Summary table for Have5MinForSurve breakdown
                                if not filtered_display_df.empty and have5min_col:
                                    st.markdown("---")
                                    
                                    # Get value counts for Have5MinForSurve
                                    participation_counts = filtered_display_df[have5min_col].value_counts()
                                    
                                    # Create summary data
                                    summary_data = []
                                    for value, count in participation_counts.items():
                                        if pd.notna(value) and str(value).strip() != '':
                                            percentage = (count / total_records * 100) if total_records > 0 else 0
                                            summary_data.append({
                                                'Response': str(value).strip(),
                                                'Count': int(count),
                                                'Percentage': f"{percentage:.1f}%"
                                            })
                                    
                                    if summary_data:
                                        summary_df = pd.DataFrame(summary_data)
                                        summary_df = summary_df.sort_values('Count', ascending=False)
                                        
                                        # Calculate participation rate (Yes responses)
                                        yes_responses = summary_df[
                                            summary_df['Response'].str.contains('Yes|yes|participate|1', case=False, na=False)
                                        ]['Count'].sum()
                                        participation_rate = (yes_responses / total_records * 100) if total_records > 0 else 0
                                        
                                        refusals = total_records - yes_responses
                                        refusal_rate = (refusals / total_records * 100) if total_records > 0 else 0
                                        
                                        # ============================================
                                        # QUICK SNAPSHOT - Always Visible
                                        # ============================================
                                        st.subheader("📋 Quick Snapshot - Participation Summary")
                                        
                                        # Top metrics row
                                        metric_col1, metric_col2, metric_col3 = st.columns(3)
                                        
                                        with metric_col1:
                                            st.metric("Total Records", f"{total_records:,}")
                                        
                                        with metric_col2:
                                            st.metric("Participations", f"{yes_responses:,}", f"{participation_rate:.1f}%")
                                        
                                        with metric_col3:
                                            st.metric("Refusals", f"{refusals:,}", f"{refusal_rate:.1f}%")
                                        
                                        # Prepare display dataframe (outside expanders for reuse)
                                        display_summary_df = summary_df.copy()
                                        display_summary_df.columns = ['Response Type', 'Count', 'Percentage']
                                        
                                        # Add category for color coding
                                        def get_category(response):
                                            response_lower = str(response).lower()
                                            if 'yes' in response_lower or 'participate' in response_lower or (response_lower.startswith('1') and len(response_lower) == 1):
                                                return 'yes'
                                            elif 'no' in response_lower or 'refused' in response_lower or 'refusal' in response_lower:
                                                return 'no'
                                            else:
                                                return 'other'
                                        
                                        display_summary_df['Category'] = display_summary_df['Response Type'].apply(get_category)
                                        
                                        # ============================================
                                        # DETAILED BREAKDOWN - In Expander
                                        # ============================================
                                        with st.expander("📊 Detailed Breakdown (Visual Cards)", expanded=False):
                                            # Display as cards in columns for better visual appeal
                                            num_rows = len(display_summary_df)
                                            cols_per_row = 2
                                            
                                            for i in range(0, num_rows, cols_per_row):
                                                row_cols = st.columns(cols_per_row)
                                                for j, col in enumerate(row_cols):
                                                    if i + j < num_rows:
                                                        row = display_summary_df.iloc[i + j]
                                                        category = row['Category']
                                                        
                                                        with col:
                                                            # Determine colors based on category
                                                            if category == 'yes':
                                                                bg_color = "#d1fae5"
                                                                text_color = "#065f46"
                                                                border_color = "#10b981"
                                                            elif category == 'no':
                                                                bg_color = "#fee2e2"
                                                                text_color = "#991b1b"
                                                                border_color = "#ef4444"
                                                            else:
                                                                bg_color = "#f3f4f6"
                                                                text_color = "#374151"
                                                                border_color = "#6b7280"
                                                            
                                                            st.markdown(f"""
                                                            <div style="
                                                                background-color: {bg_color};
                                                                border-left: 4px solid {border_color};
                                                                padding: 1rem;
                                                                border-radius: 8px;
                                                                margin-bottom: 1rem;
                                                                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                                                            ">
                                                                <div style="font-size: 0.85rem; color: {text_color}; font-weight: 600; margin-bottom: 0.5rem;">
                                                                    {row['Response Type']}
                                                                </div>
                                                                <div style="display: flex; justify-content: space-between; align-items: center;">
                                                                    <span style="font-size: 1.5rem; font-weight: 700; color: #1e3a5f;">
                                                                        {row['Count']:,}
                                                                    </span>
                                                                    <span style="font-size: 1rem; color: #6b7280; font-weight: 500;">
                                                                        {row['Percentage']}
                                                                    </span>
                                                                </div>
                                                            </div>
                                                            """, unsafe_allow_html=True)
                                        
                                        # ============================================
                                        # SUMMARY TABLE - In Expander
                                        # ============================================
                                        with st.expander("📋 Summary Table", expanded=False):
                                            # Remove category column before display
                                            table_df = display_summary_df[['Response Type', 'Count', 'Percentage']].copy()
                                            st.dataframe(
                                                table_df,
                                                use_container_width=True,
                                                hide_index=True,
                                                height=min(400, len(table_df) * 50 + 50)
                                            )
                                
                                if not filtered_display_df.empty:
                                    # Prepare columns for display
                                    display_columns = []
                                    column_mapping = {}
                                    
                                    if id_col:
                                        display_columns.append(id_col)
                                        column_mapping[id_col] = 'id'
                                    if interv_init_col:
                                        display_columns.append(interv_init_col)
                                        column_mapping[interv_init_col] = 'IntervInit'
                                    if localtime_col:
                                        display_columns.append(localtime_col)
                                        column_mapping[localtime_col] = 'LocalTime'
                                    if route_code_col:
                                        display_columns.append(route_code_col)
                                        column_mapping[route_code_col] = 'RouteSurveyedCode'
                                    if route_name_col:
                                        display_columns.append(route_name_col)
                                        column_mapping[route_name_col] = 'RouteSurveyed'
                                    if have5min_code_col:
                                        display_columns.append(have5min_code_col)
                                        column_mapping[have5min_code_col] = 'Have5MinForSurveCode'
                                    if have5min_col:
                                        display_columns.append(have5min_col)
                                        column_mapping[have5min_col] = 'Have5MinForSurve'
                                    if timeon_code_col:
                                        display_columns.append(timeon_code_col)
                                        column_mapping[timeon_code_col] = 'TimeOnCode'
                                    if timeon_col:
                                        display_columns.append(timeon_col)
                                        column_mapping[timeon_col] = 'TimeOn'
                                    if register_win_code_col:
                                        display_columns.append(register_win_code_col)
                                        column_mapping[register_win_code_col] = 'RegisterToWinYNCode'
                                    if register_win_col:
                                        display_columns.append(register_win_col)
                                        column_mapping[register_win_col] = 'RegisterToWinYN'
                                    if survey_lang_code_col:
                                        display_columns.append(survey_lang_code_col)
                                        column_mapping[survey_lang_code_col] = 'SurveyLanguageCode'
                                    if survey_lang_col:
                                        display_columns.append(survey_lang_col)
                                        column_mapping[survey_lang_col] = 'SurveyLanguage'
                                    
                                    # Create display dataframe with renamed columns
                                    display_data = filtered_display_df[display_columns].copy()
                                    display_data = display_data.rename(columns=column_mapping)
                                    
                                    # Display table
                                    st.subheader("📋 Records Table")
                                    st.dataframe(display_data, use_container_width=True, height=400)
                                    
                                    # Map section
                                    if lat_col and lon_col:
                                        st.subheader("🗺️ Interviewer Location Map")
                                        
                                        # Prepare map data
                                        map_data = filtered_display_df[[lat_col, lon_col, id_col if id_col else interv_init_col, localtime_col if localtime_col else interv_init_col]].copy()
                                        
                                        # Rename columns for map
                                        map_data.columns = ['latitude', 'longitude', 'id', 'localtime']
                                        
                                        # Convert to numeric
                                        map_data['latitude'] = pd.to_numeric(map_data['latitude'], errors='coerce')
                                        map_data['longitude'] = pd.to_numeric(map_data['longitude'], errors='coerce')
                                        
                                        # Drop rows with invalid coordinates
                                        map_data = map_data.dropna(subset=['latitude', 'longitude'])
                                        
                                        if not map_data.empty:
                                            # Format LocalTime for tooltip
                                            if 'localtime' in map_data.columns:
                                                map_data['localtime'] = pd.to_datetime(map_data['localtime'], errors='coerce')
                                                map_data['localtime_str'] = map_data['localtime'].dt.strftime('%Y-%m-%d %H:%M:%S')
                                            else:
                                                map_data['localtime_str'] = ''
                                            
                                            # Create hover text with ID and LocalTime
                                            map_data['hover_text'] = map_data.apply(
                                                lambda row: f"ID: {row['id']}<br>Time: {row['localtime_str']}" if pd.notna(row['id']) else f"Time: {row['localtime_str']}",
                                                axis=1
                                            )
                                            
                                            # Create interactive map with Plotly
                                            try:
                                                # Prepare customdata with proper formatting
                                                map_data['id_str'] = map_data['id'].astype(str).replace('nan', 'N/A')
                                                map_data['time_str'] = map_data['localtime_str'].fillna('N/A')
                                                
                                                fig = px.scatter_mapbox(
                                                    map_data,
                                                    lat='latitude',
                                                    lon='longitude',
                                                    zoom=10,
                                                    height=500,
                                                    mapbox_style='open-street-map'
                                                )
                                                
                                                # Update hover template to show ID and LocalTime
                                                fig.update_traces(
                                                    hovertemplate='<b>ID:</b> %{customdata[0]}<br><b>LocalTime:</b> %{customdata[1]}<extra></extra>',
                                                    customdata=map_data[['id_str', 'time_str']].values,
                                                    marker=dict(
                                                        size=10,
                                                        color='red',
                                                        opacity=0.7
                                                    )
                                                )
                                                
                                                # Update layout
                                                fig.update_layout(
                                                    margin=dict(l=0, r=0, t=0, b=0),
                                                    mapbox=dict(
                                                        center=dict(
                                                            lat=map_data['latitude'].mean(),
                                                            lon=map_data['longitude'].mean()
                                                        ),
                                                        zoom=10
                                                    )
                                                )
                                                
                                                st.plotly_chart(fig, use_container_width=True)
                                                
                                            except Exception as e:
                                                # Fallback to Streamlit map if Plotly fails
                                                st.warning(f"Interactive map unavailable, using basic map: {str(e)}")
                                                st.map(
                                                    map_data,
                                                    latitude='latitude',
                                                    longitude='longitude',
                                                    size=100,
                                                    use_container_width=True
                                                )
                                            
                                            st.caption(f"📍 Showing {len(map_data)} location points on map. Hover over points to see ID and LocalTime.")
                                        else:
                                            st.info("📍 No valid location data available for the selected filters")
                                    else:
                                        st.info("📍 Location columns (ElvisUserLoc1_LAT/LONG) not found in dataset")
                                    
                                    # Download button
                                    csv_data = display_data.to_csv(index=False)
                                    st.download_button(
                                        label="📥 Download Records as CSV",
                                        data=csv_data,
                                        file_name=f"interviewer_records_{selected_date}_{selected_interv}.csv" if selected_date and selected_interv else "interviewer_records.csv",
                                        mime="text/csv",
                                        key="download_interviewer_records"
                                    )
                                else:
                                    st.warning("⚠️ No records found for the selected filters")
                        else:
                            st.warning("⚠️ No data available")
                    
                    except Exception as e:
                        st.error(f"❌ Error loading interviewer records: {str(e)}")
                        import traceback
                        with st.expander("🔧 Technical Details"):
                            st.code(traceback.format_exc())
                    
                    # Navigation
                    if st.button("🔙 Home Page", key="interviewer_records_home"):
                        st.query_params["page"] = "main"
                        st.rerun()

        # Function to extract date and clean a column
        # This function extracts the date from a column and creates a new column with the cleaned data
        def extract_date_and_clean_column(df, column_name, new_column_name, split_char='_'):
            df = df.copy()
            df['date'] = df[column_name].str.split(split_char).str[0]
            df[new_column_name] = df[column_name].str.split(split_char).str[1]
            return df


        def convert_percentage_columns(df: pd.DataFrame) -> pd.DataFrame:
            df = df.copy()

            percent_cols = [col for col in df.columns if '%' in col]

            for col in percent_cols:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace('%', '', regex=False)
                    .replace('', None)
                )

                # Convert safely (invalid values → NaN, not crash)
                df[col] = pd.to_numeric(df[col], errors='coerce')

            return df

        # ===============================
        # THRESHOLD CONFIG
        # ===============================
        PERCENT_THRESHOLD_RULES = {
            "% of Female": {"low": 40, "high": 60},
            "% of Male": {"low": 40, "high": 60},
            "% of White": {"low": 30, "high": 80},
            "% of Black": {"low": 5, "high": 30},
            "% of Hispanic": {"low": 10, "high": 40},
            "% of LowIncome": {"low": 20, "high": 60},
            "% of No Income": {"low": 5, "high": 25},
            "% of Follow-Up Survey": {"low": 5, "high": 20},
            "% of Contest - Yes": {"low": 0, "high": 100},
            "% of Contest - (Yes & Good Info)/Overall # of Records": {"low": 0, "high": 100}
        }

        TIME_THRESHOLD_RULES = {
            "SurveyTime (All)": {"warn": 6, "critical": 8},
            "SurveyTime (TripLogic)": {"warn": 5, "critical": 7},
            "SurveyTime (DemoLogic)": {"warn": 4, "critical": 6},
        }


        # ===============================
        # HELPERS
        # ===============================
        def convert_percentage_columns(df: pd.DataFrame) -> pd.DataFrame:
            df = df.copy()
            for col in df.columns:
                if "%" in col:
                    df[col] = (
                        df[col]
                        .astype(str)
                        .str.replace('%', '', regex=False)
                        .replace('', None)
                    )
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            return df


        def time_to_minutes(time_str):
            try:
                h, m, s = map(int, time_str.split(":"))
                return h * 60 + m + s / 60
            except:
                return None


        def style_percentage(val, col):
            if pd.isna(val):
                return ""

            rule = PERCENT_THRESHOLD_RULES.get(col)
            if not rule:
                return ""

            try:
                val_num = pd.to_numeric(val, errors="coerce")
            except Exception:
                return ""

            if pd.isna(val_num):
                return ""

            if val_num < rule["low"]:
                return "background-color:#e6f0ff"
            elif val_num > rule["high"]:
                return "background-color:#4d79ff;color:white"
            return ""



        def style_time(val, col):
            rule = TIME_THRESHOLD_RULES.get(col)
            if not rule or not isinstance(val, str):
                return ""
            minutes = time_to_minutes(val)
            if minutes is None:
                return ""
            if minutes >= rule["critical"]:
                return "background-color:#ff4d4d;color:white"
            if minutes >= rule["warn"]:
                return "background-color:#ffe6e6"
            return ""


        # FIXED: Added counter to ensure unique keys for sorting
        _sort_counter = 0

        def apply_column_sorting(df, section_title):
            global _sort_counter
            sort_options = ["None"]

            # Build SORT_RULES dynamically based on available columns
            SORT_RULES = {
                "percent": {
                    "columns": [col for col in PERCENT_THRESHOLD_RULES.keys() if col in df.columns],
                    "ascending": False
                },
                "time": {
                    "columns": [col for col in TIME_THRESHOLD_RULES.keys() if col in df.columns],
                    "ascending": False
                },
                "count": {
                    "columns": [
                        col for col in [
                            "# of Records",
                            "# of Records Reviewed",
                            "# of Records Not Reviewed",
                            "# of Supervisor Delete",
                            "# of Errors",
                            "# of Issues"
                        ] if col in df.columns
                    ],
                    "ascending": False
                }
            }

            for group, cfg in SORT_RULES.items():
                for col in cfg["columns"]:
                    if col in df.columns:
                        sort_options.append(col)

            # Use a unique key by combining section_title with a counter
            _sort_counter += 1
            unique_key = f"{section_title}_sort_{_sort_counter}"
            
            selected_sort = st.selectbox(
                "Sort by",
                sort_options,
                key=unique_key
            )

            if selected_sort != "None":
                if selected_sort in TIME_THRESHOLD_RULES and selected_sort in df.columns:
                    df = df.copy()
                    df["_sort_time"] = df[selected_sort].apply(time_to_minutes)
                    df = df.sort_values("_sort_time", ascending=False).drop(columns="_sort_time")
                elif selected_sort in df.columns:
                    df = df.sort_values(selected_sort, ascending=False)

            return df


        def render_metrics(row: dict, title: str):
            st.markdown(f"### {title}")

            excluded_columns = [
                "INTERV_INIT", "Route",
                "# of Records", "# of Supervisor Delete",
                "# of Records Remove", "# of Records Reviewed",
                "# of Records Not Reviewed",
                "% of LowIncome",
                "% of Contest - Yes",
                "% of Follow-Up Survey",
                "% of Contest - (Yes & Good Info)/Overall # of Records"
            ]

            filtered_items = [(k, v) for k, v in row.items() if k not in excluded_columns]

            for i in range(0, len(filtered_items), 4):
                cols = st.columns(min(4, len(filtered_items) - i))
                for col, (field, value) in zip(cols, filtered_items[i:i + 4]):
                    with col:
                        st.markdown(
                            f"""
                            <div style="padding:2px 0; margin-bottom:4px;">
                                <span style="font-size:0.65rem; font-weight:600; color:#000;">{field}</span><br>
                                <span style="font-size:0.8rem; color:#000;">{value}</span>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

        # FIXED: Added unique key generator for date selectboxes
        _date_counter = 0

        def display_filtered_or_unfiltered_report(
            unfiltered_df: pd.DataFrame,
            filtered_df: pd.DataFrame,
            filter_column_name: str,
            display_column_name: str,
            section_title: str,
            date_label: str
        ):
            global _date_counter
            st.subheader(section_title)

            # Convert % columns safely
            unfiltered_df = convert_percentage_columns(unfiltered_df)
            filtered_df = convert_percentage_columns(filtered_df)

            # Ensure date column exists
            original_filter_column = filter_column_name
            if filter_column_name not in filtered_df.columns:
                for col in ['Date', 'DATE', 'date', 'Survey_Date']:
                    if col in filtered_df.columns:
                        filter_column_name = col
                        break
                else:
                    st.error(f"Date column not found for {section_title}")
                    return

            temp_df = filtered_df.copy()
            temp_df['date'] = pd.to_datetime(temp_df[filter_column_name], errors='coerce')
            temp_df = temp_df.dropna(subset=['date'])
            temp_df['date'] = temp_df['date'].dt.strftime('%Y-%m-%d')

            unique_dates = sorted(temp_df['date'].unique())
            
            # Create a unique key for the date selectbox
            _date_counter += 1
            date_key = f"{section_title}_date_{_date_counter}"
            
            selected_date = st.selectbox(
                f"{date_label} Date",
                ["All"] + unique_dates,
                key=date_key
            )

            if selected_date == "All":
                df_to_show = unfiltered_df
            else:
                df_to_show = temp_df[temp_df['date'] == selected_date].drop(columns=[filter_column_name])
                df_to_show = df_to_show.rename(columns={
                    display_column_name: display_column_name.upper(),
                    'date': 'Date'
                })

                first_cols = ['Date', display_column_name.upper()]
                remaining_cols = [c for c in df_to_show.columns if c not in first_cols]
                df_to_show = df_to_show[first_cols + remaining_cols]

            # Apply sorting
            df_to_show = apply_column_sorting(df_to_show, section_title)

            # Build style matrix - FIXED: Ensure all values are properly styled
            styles = pd.DataFrame("", index=df_to_show.index, columns=df_to_show.columns)

            for col in df_to_show.columns:
                if "%" in col:
                    # Apply styling to all rows for percentage columns
                    styles[col] = df_to_show[col].apply(lambda v: style_percentage(v, col))
                elif col in TIME_THRESHOLD_RULES:
                    # Apply styling to all rows for time columns
                    styles[col] = df_to_show[col].apply(lambda v: style_time(v, col))

            styled_df = df_to_show.style.format(
                {col: "{:.2f}" for col in df_to_show.select_dtypes(include="number").columns}
            )
            # Display the dataframe with styling
            st.dataframe(
                styled_df.apply(lambda _: styles, axis=None),
                use_container_width=True
            )




        def route_comparison_page():
            st.title("Route Comparison")
            
            # Use the pre-fetched dataframe
            if not route_comparison_df.empty:
                # Display the dataframe
                st.dataframe(route_comparison_df, use_container_width=True, height=600)
                
                # Download button
                csv_data, file_name = create_csv(route_comparison_df, "route_comparison.csv")
                download_csv(csv_data, file_name, "Download Route Comparison Data")
            else:
                st.warning("No route comparison data available")
            
            if st.button("Home Page"):
                st.query_params["page"] = "main"
                st.rerun()

        def reverse_routes_page():
            st.title("Reverse Routes Comparison")
            
            # Create tabs for different views
            tab1, tab2, tab3 = st.tabs(["Reverse Routes", "Route Shortages", "Clonable IDs by Route"])
            
            with tab1:
                # Use the pre-fetched dataframes
                if not reverse_routes_df.empty or not reverse_routes_difference_df.empty:
                    
                    # Create a merged view of both dataframes
                    if not reverse_routes_df.empty and not reverse_routes_difference_df.empty:
                        merged_df = pd.concat([reverse_routes_df, reverse_routes_difference_df], ignore_index=True)
                        
                        # Remove exact duplicates (keeping first occurrence)
                        merged_df = merged_df.drop_duplicates(keep='first')
                        
                        # Sort by ID for better organization
                        if 'id' in merged_df.columns:
                            merged_df = merged_df.sort_values('id')
                        
                    elif not reverse_routes_df.empty:
                        merged_df = reverse_routes_df.copy()
                    elif not reverse_routes_difference_df.empty:
                        merged_df = reverse_routes_difference_df.copy()
                    
                    # Function to extract main route name (before any dash)
                    def get_main_route_name(route_string):
                        if pd.isna(route_string):
                            return None
                        
                        route_str = str(route_string).strip()
                        
                        # Get the part before the first dash
                        if ' - ' in route_str:
                            return route_str.split(' - ')[0]
                        else:
                            return route_str
                    
                    # ------------------------------------------
                    # SIMPLIFIED DIRECTION EXTRACTION USING FINAL_DIRECTION_NAME
                    # ------------------------------------------
                    direction_code_mapping = {}

                    if 'FINAL_DIRECTION_CODE' in merged_df.columns:
                        # First, try to use FINAL_DIRECTION_NAME if available
                        if 'FINAL_DIRECTION_NAME' in merged_df.columns:
                            for idx, row in merged_df.iterrows():
                                direction_code = row.get('FINAL_DIRECTION_CODE')
                                direction_name = row.get('FINAL_DIRECTION_NAME')
                                
                                if pd.notna(direction_code) and pd.notna(direction_name):
                                    code_str = str(direction_code)
                                    direction_code_mapping[code_str] = str(direction_name)
                        
                        # If FINAL_DIRECTION_NAME is not available or not fully populated,
                        # fall back to extracting from ROUTE_SURVEYED
                        if not direction_code_mapping and 'ROUTE_SURVEYED' in merged_df.columns:
                            from collections import defaultdict, Counter
                            import re

                            code_to_directions = defaultdict(Counter)

                            for idx, row in merged_df.iterrows():
                                route = row.get('ROUTE_SURVEYED')
                                direction_code = row.get('FINAL_DIRECTION_CODE')

                                if pd.isna(direction_code):
                                    continue

                                code_str = str(direction_code)

                                if pd.notna(route):
                                    route_str = str(route).strip()

                                    direction_text = None

                                    # 1️⃣ Check for square brackets [] first
                                    brackets = re.findall(r'\[([^\]]+)\]', route_str)
                                    if brackets:
                                        direction_text = brackets[-1].strip()  # take last occurrence
                                    # 2️⃣ If no brackets, fallback to last part after dash
                                    elif ' - ' in route_str:
                                        parts = route_str.split(' - ')
                                        if len(parts) >= 2:
                                            direction_text = parts[-1].strip()
                                    
                                    if direction_text:
                                        code_to_directions[code_str][direction_text] += 1

                            # Assign most common extracted text per direction code
                            for code_str, counter in code_to_directions.items():
                                if len(counter) > 0:
                                    direction_code_mapping[code_str] = counter.most_common(1)[0][0]
                    # ------------------------------------------
                    # END SIMPLIFIED DIRECTION EXTRACTION
                    # ------------------------------------------


                    # ------------------------------------------
                    # DEFAULT MAPPING — UNCHANGED AS YOU REQUESTED
                    # ------------------------------------------
                    if not direction_code_mapping and 'FINAL_DIRECTION_CODE' in merged_df.columns:
                        default_mapping = {
                            '_00': 'Outbound',
                            '_01': 'Inbound',
                            '_02': 'Loop',
                            '_03': 'Clockwise',
                            '_04': 'Counterclockwise',
                            '0': 'Outbound',
                            '1': 'Inbound',
                            '2': 'Loop',
                            '3': 'Clockwise',
                            '4': 'Counterclockwise',
                        }
                        
                        for code in merged_df['FINAL_DIRECTION_CODE'].dropna().unique():
                            code_str = str(code)
                            if code_str in default_mapping:
                                direction_code_mapping[code_str] = default_mapping[code_str]
                            elif '_00' in code_str:
                                direction_code_mapping[code_str] = 'Outbound'
                            elif '_01' in code_str:
                                direction_code_mapping[code_str] = 'Inbound'
                            elif '_02' in code_str:
                                direction_code_mapping[code_str] = 'Loop'
                            else:
                                direction_code_mapping[code_str] = code_str
                    # ------------------------------------------
                    # END DEFAULT MAPPING
                    # ------------------------------------------

                    # Create mapping for main route names
                    main_routes = set()
                    if 'ROUTE_SURVEYED' in merged_df.columns:
                        for route in merged_df['ROUTE_SURVEYED'].dropna().unique():
                            main_route = get_main_route_name(route)
                            if main_route:
                                main_routes.add(main_route)
                    
                    # Reset index to show row numbers
                    merged_df = merged_df.reset_index(drop=True)
                    merged_df.index = merged_df.index + 1
                    
                    # Display the merged table
                    st.subheader("Reverse Routes View")
                    
                    # Create filters - removed DAY_TYPE, TIME_PERIOD, and REVERSE_TRIPS_STATUS filters, adjusted to 3 columns
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        type_options = ['All'] + sorted(merged_df['Type'].dropna().unique().tolist())
                        selected_type = st.selectbox("Filter by Type:", type_options)
                    
                    with col2:
                        # Filter by Final Route Code (FINAL_DIRECTION_CODE)
                        if 'FINAL_DIRECTION_CODE' in merged_df.columns:
                            route_code_options = ['All'] + sorted(merged_df['FINAL_DIRECTION_CODE'].dropna().unique().tolist())
                            selected_route_code = st.selectbox("Filter by Final Route Code:", route_code_options)
                        else:
                            selected_route_code = 'All'
                    
                    with col3:
                        # Filter by Final Direction Route Name (FINAL_DIRECTION_NAME)
                        if 'FINAL_DIRECTION_NAME' in merged_df.columns:
                            # Get unique route names from FINAL_DIRECTION_NAME
                            route_names = merged_df['FINAL_DIRECTION_NAME'].dropna().unique()
                            route_names = [str(r) for r in route_names if str(r).strip() != '']
                            if route_names:
                                route_name_options = ['All'] + sorted(route_names)
                                selected_route_name = st.selectbox("Filter by Final Direction Route Name:", route_name_options)
                            else:
                                selected_route_name = 'All'
                        else:
                            selected_route_name = 'All'
                    
                    # Apply filters
                    filtered_df = merged_df.copy()
                    
                    if selected_type != 'All':
                        filtered_df = filtered_df[filtered_df['Type'] == selected_type]
                    
                    # Filter by Final Route Code (FINAL_DIRECTION_CODE)
                    if selected_route_code != 'All' and 'FINAL_DIRECTION_CODE' in filtered_df.columns:
                        filtered_df = filtered_df[filtered_df['FINAL_DIRECTION_CODE'].astype(str) == str(selected_route_code)]
                    
                    # Filter by Final Direction Route Name (FINAL_DIRECTION_NAME)
                    if selected_route_name != 'All' and 'FINAL_DIRECTION_NAME' in filtered_df.columns:
                        filtered_df = filtered_df[filtered_df['FINAL_DIRECTION_NAME'].astype(str) == selected_route_name]
                    
                    # Additional search filter
                    search_term = st.text_input("Search across all columns:", "")
                    if search_term:
                        mask = pd.Series(False, index=filtered_df.index)
                        for col in filtered_df.columns:
                            if filtered_df[col].dtype == 'object':
                                mask = mask | filtered_df[col].astype(str).str.contains(search_term, case=False, na=False)
                        filtered_df = filtered_df[mask]
                    
                    # Reset index for display and drop unwanted columns
                    display_df = filtered_df.reset_index(drop=True)
                    display_df.index = display_df.index + 1
                    
                    # Drop columns that should not be displayed
                    columns_to_hide = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', 'TIME_PERIOD', 'DAY_TYPE', 'REVERSE_TRIPS_STATUS']
                    # Also hide the index column name if it exists
                    if display_df.index.name:
                        display_df.index.name = None
                    
                    # Drop the unwanted columns if they exist
                    for col in columns_to_hide:
                        if col in display_df.columns:
                            display_df = display_df.drop(columns=[col])
                    
                    st.dataframe(display_df, use_container_width=True, height=400)
                    
                    # Statistics - updated to remove TIME_PERIOD and DAY_TYPE references
                    st.subheader("Statistics")
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Total Records", len(filtered_df))
                    
                    with col2:
                        if 'Type' in filtered_df.columns and len(filtered_df) > 0:
                            type_counts = filtered_df['Type'].value_counts()
                            if len(type_counts) > 0:
                                st.metric("Most Common Type", type_counts.index[0])
                    
                    with col3:
                        # Show Final Route Code (FINAL_DIRECTION_CODE) if available
                        if selected_route_code != 'All':
                            st.metric("Selected Final Route Code", str(selected_route_code))
                        elif 'FINAL_DIRECTION_CODE' in filtered_df.columns and len(filtered_df) > 0:
                            route_code_counts = filtered_df['FINAL_DIRECTION_CODE'].value_counts()
                            if len(route_code_counts) > 0:
                                st.metric("Most Common Final Route Code", str(route_code_counts.index[0]))
                    
                    with col4:
                        # Show Final Direction Route Name (FINAL_DIRECTION_NAME) if available
                        if selected_route_name != 'All':
                            st.metric("Selected Final Direction Route Name", selected_route_name)
                        elif 'FINAL_DIRECTION_NAME' in filtered_df.columns and len(filtered_df) > 0:
                            route_name_counts = filtered_df['FINAL_DIRECTION_NAME'].value_counts()
                            if len(route_name_counts) > 0:
                                st.metric("Most Common Final Direction Route Name", route_name_counts.index[0])
                    
                    # Download buttons
                    st.subheader("Download Data")
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if not reverse_routes_df.empty:
                            csv1, file1 = create_csv(reverse_routes_df, "reverse_routes.csv")
                            download_csv(csv1, file1, "Download Reverse Routes")
                    
                    with col2:
                        if not reverse_routes_difference_df.empty:
                            csv2, file2 = create_csv(reverse_routes_difference_df, "reverse_routes_difference.csv")
                            download_csv(csv2, file2, "Download Reverse Routes Difference")
                    
                    with col3:
                        csv3, file3 = create_csv(filtered_df, "merged_reverse_routes.csv")
                        download_csv(csv3, file3, "Download Filtered View")
                
                else:
                    st.warning("No reverse routes data available")
            
            with tab2:
                st.header("Route Shortages Summary")
                st.markdown("**Simple view of what needs to be cloned**")
                
                # Check if we have the route_comparison_df (which contains the shortages info)
                if not route_comparison_df.empty:
                    # Filter to routes that need clones (Total_DIFFERENCE > 0)
                    shortages_df = route_comparison_df[route_comparison_df['Total_DIFFERENCE'] > 0].copy()
                    
                    if not shortages_df.empty:
                        # Simplify the data for Mansi's team
                        simple_data = []
                        
                        # Get route names if available (you might need to adjust this based on your data structure)
                        route_names = {}
                        # If you have a mapping of route codes to names, use it here
                        # For now, assuming route_comparison_df has ROUTE_SURVEYEDCode column
                        
                        for _, row in shortages_df.iterrows():
                            route_code = row.get('ROUTE_SURVEYEDCode', 'Unknown')
                            
                            # Build the "What's Needed" description
                            needs = []
                            
                            if row.get('EARLY_AM_DIFFERENCE', 0) > 0:
                                needs.append(f"{int(row['EARLY_AM_DIFFERENCE'])} Early AM")
                            if row.get('AM_DIFFERENCE', 0) > 0:
                                needs.append(f"{int(row['AM_DIFFERENCE'])} AM")
                            if row.get('Midday_DIFFERENCE', 0) > 0:
                                needs.append(f"{int(row['Midday_DIFFERENCE'])} Midday")
                            if row.get('PM_DIFFERENCE', 0) > 0:
                                needs.append(f"{int(row['PM_DIFFERENCE'])} PM")
                            if row.get('Evening_DIFFERENCE', 0) > 0:
                                needs.append(f"{int(row['Evening_DIFFERENCE'])} Evening")
                            
                            if needs:
                                needs_text = ", ".join(needs) + " record"
                                if row.get('Total_DIFFERENCE', 0) > 1:
                                    needs_text += "s"
                                
                                simple_data.append({
                                    'Route Code': route_code,
                                    'What\'s Needed': needs_text,
                                    'Total Needed': int(row.get('Total_DIFFERENCE', 0))
                                })
                        
                        # Create the simple dataframe
                        if simple_data:
                            simple_df = pd.DataFrame(simple_data)
                            
                            # Sort by total needed (descending) and then by route name
                            simple_df = simple_df.sort_values(['Total Needed', 'Route Code'], ascending=[False, True])
                            
                            # Display in a clean format
                            st.dataframe(
                                simple_df[['Route Code', 'What\'s Needed']],
                                use_container_width=True,
                                height=400,
                                hide_index=True
                            )
                            
                            # Summary statistics
                            st.subheader("Summary")
                            col1, col2, col3 = st.columns(3)
                            
                            with col1:
                                st.metric("Routes with Shortages", len(simple_df))
                            
                            with col2:
                                total_needed = simple_df['Total Needed'].sum()
                                st.metric("Total Records Needed", total_needed)
                            
                            with col3:
                                if not simple_df.empty:
                                    max_route = simple_df.iloc[0]  # First row has highest need
                                    st.metric("Highest Need Route", f"{max_route['Route Code']} ({max_route['Total Needed']})")
                            
                            # Download button
                            csv_data = simple_df.to_csv(index=False)
                            st.download_button(
                                label="Download Route Shortages",
                                data=csv_data,
                                file_name="route_shortages_mansi_team.csv",
                                mime="text/csv",
                                help="Download the simplified route shortages list"
                            )
                            
                            # Visual breakdown
                            st.subheader("Breakdown by Time Period")
                            
                            # Calculate totals by time period
                            time_period_totals = {
                                'Early AM': shortages_df['EARLY_AM_DIFFERENCE'].sum(),
                                'AM': shortages_df['AM_DIFFERENCE'].sum(),
                                'Midday': shortages_df['Midday_DIFFERENCE'].sum(),
                                'PM': shortages_df['PM_DIFFERENCE'].sum(),
                                'Evening': shortages_df['Evening_DIFFERENCE'].sum()
                            }
                            
                            # Display as metrics
                            cols = st.columns(5)
                            time_periods = ['Early AM', 'AM', 'Midday', 'PM', 'Evening']
                            
                            for i, period in enumerate(time_periods):
                                with cols[i]:
                                    st.metric(period, int(time_period_totals[period]))
                            
                            # Show top 5 routes with highest needs
                            st.subheader("Top 5 Routes with Highest Needs")
                            top_5_df = simple_df.head(5).copy()
                            top_5_df.index = range(1, 6)  # Show ranking numbers
                            
                            st.dataframe(
                                top_5_df[['Route Code', 'What\'s Needed', 'Total Needed']],
                                use_container_width=True,
                                height=200
                            )
                        else:
                            st.success("✅ All route targets have been met! No shortages.")
                    else:
                        st.success("✅ All route targets have been met! No shortages.")
                else:
                    st.warning("Route comparison data not available. Please check if the data has been processed.")
                
                # Help/Instructions section
                with st.expander("📋 How to use this information"):
                    st.markdown("""
                    **Quick Guide:**
                    
                    1. **Route Code**: Technical identifier for reference
                    2. **What's Needed**: Specific time periods and quantities required
                    
                    **Example:** If it says "6 Early AM, 3 AM, 15 Midday records":
                    - Route 9 needs 6 records during Early AM time period
                    - Route 9 needs 3 records during AM time period  
                    - Route 9 needs 15 records during Midday time period
                    
                    **Action Steps:**
                    - Focus on routes with highest "Total Needed" first
                    - Use the time period breakdown to plan your surveyors' schedules
                    - Check the "Reverse Routes" tab for specific records that can be cloned
                    """)
            
            with tab3:
                st.header("Clonable IDs by Route")
                st.markdown("**List of IDs that can be cloned, grouped by route**")
                
                try:
                    # Import the fetch_data function if not already imported
                    from automated_refresh_flow_new import PROJECTS, fetch_data
                    
                    project_config = PROJECTS[st.session_state["selected_project"]]
                    elvis_config = project_config['databases']["elvis"]
                    table_name = elvis_config['table']
                    database_name = elvis_config["database"]
                    
                    with st.spinner("🔄 Loading survey data for clonable IDs analysis..."):
                        # Load data directly from the database
                        csv_buffer = fetch_data(database_name, table_name)
                        
                        if not csv_buffer:
                            st.error("❌ Failed to fetch data from Elvis table")
                            return
                        
                        # Read the data
                        csv_buffer.seek(0)
                        survey_df = pd.read_csv(csv_buffer, low_memory=False)
                        
                        # Safely drop junk header row if present
                        if not survey_df.empty and survey_df.iloc[0].isnull().all():
                            survey_df = survey_df.drop(index=0)
                        
                        survey_df = survey_df.reset_index(drop=True)
                        
                        st.success(f"✅ Loaded {len(survey_df)} records for analysis")
                        # ------------------------
                        # Apply LACMTA Agency Filter (INITIAL FILTER)
                        # ------------------------
                        if 'RouteSurveyedCode' in survey_df.columns:
                            survey_df = survey_df.rename(columns={'RouteSurveyedCode': 'ROUTE_SURVEYEDCode'})
                        selected_project = st.session_state.get("selected_project")
                        selected_agency = st.session_state.get("selected_agency")

                        if selected_project == "LACMTA_FEEDER" and selected_agency and selected_agency != "All":

                            survey_df, _ = apply_lacmta_agency_filter(
                                df=survey_df,
                                project=selected_project,
                                agency=selected_agency,
                                bucket_name=os.getenv("bucket_name"),
                                project_config=project_config
                            )

                            st.info(f"🎯 Agency filter applied: {selected_agency}")
                    
                    # Now use survey_df instead of baby_elvis_df_merged
                    # Get the column names using your existing function
                    trip_oppo_dir_column = check_all_characters_present(survey_df, ['tripinoppodircode'])
                    elvis_status_code_column = check_all_characters_present(survey_df, ['elvisstatuscode'])
                    reverse_trips_column = check_all_characters_present(survey_df, ['reversetrips'])
                    route_survey_column = check_all_characters_present(survey_df, ['routesurveyedcode'])
                    route_survey_name_column = check_all_characters_present(survey_df, ['routesurveyed'])
                    
                    # NEW: Get additional column names for the new filters
                    interv_init_column = check_all_characters_present(survey_df, ['intervinit', 'interv_init', 'interviewer'])
                    have_5_min_column = check_all_characters_present(survey_df, ['have5minforsurvecode', 'have_5_min_for_survecode', 'participationcode'])
                    
                    # Debug: Show available columns (optional)
                    # st.write("Available columns:", survey_df.columns.tolist()[:20])
                    
                    # Ensure all required columns exist
                    required_columns = []
                    column_info = []
                    
                    if trip_oppo_dir_column:
                        required_columns.append(trip_oppo_dir_column[0])
                        column_info.append(f"✅ Using column '{trip_oppo_dir_column[0]}' for TripInOppoDir")
                    
                    if elvis_status_code_column:
                        required_columns.append(elvis_status_code_column[0])
                        column_info.append(f"✅ Using column '{elvis_status_code_column[0]}' for ElvisStatus")
                    
                    if reverse_trips_column:
                        required_columns.append(reverse_trips_column[0])
                        column_info.append(f"✅ Using column '{reverse_trips_column[0]}' for ReverseTrips")
                    
                    if route_survey_column:
                        required_columns.append(route_survey_column[0])
                        column_info.append(f"✅ Using column '{route_survey_column[0]}' for Route Surveyed Code")
                    
                    if interv_init_column:
                        column_info.append(f"✅ Using column '{interv_init_column[0]}' for Interviewer")
                    else:
                        column_info.append("⚠️ Interviewer column not found - skipping interviewer filter")
                    
                    if have_5_min_column:
                        column_info.append(f"✅ Using column '{have_5_min_column[0]}' for Participation")
                    else:
                        column_info.append("⚠️ Participation column not found - skipping participation filter")
                    
                    if 'id' not in survey_df.columns:
                        # Try to find ID column with different names
                        id_columns = check_all_characters_present(survey_df, ['id', 'recordid', 'record_id', 'respondent_id'])
                        if id_columns:
                            column_info.append(f"✅ Using column '{id_columns[0]}' for ID")
                            survey_df = survey_df.rename(columns={id_columns[0]: 'id'})
                            required_columns.append('id')
                        else:
                            st.error("❌ ID column not found. Please check your data.")
                            return
                    else:
                        required_columns.append('id')
                        column_info.append("✅ Using column 'id' for Record ID")
                    
                    # Show column mapping info
                    with st.expander("📋 Column Mapping Information"):
                        for info in column_info:
                            st.write(info)
                    
                    missing_cols = [col for col in required_columns if col not in survey_df.columns]
                    if missing_cols:
                        st.error(f"❌ Missing required columns: {missing_cols}")
                        st.write("Available columns:")
                        st.write(survey_df.columns.tolist())
                        return
                    
                    # Display sample of the data
                    with st.expander("🔍 View sample data (first 5 records)"):
                        sample_cols = ['id']
                        if trip_oppo_dir_column:
                            sample_cols.append(trip_oppo_dir_column[0])
                        if elvis_status_code_column:
                            sample_cols.append(elvis_status_code_column[0])
                        if reverse_trips_column:
                            sample_cols.append(reverse_trips_column[0])
                        if route_survey_column:
                            sample_cols.append(route_survey_column[0])
                        if interv_init_column:
                            sample_cols.append(interv_init_column[0])
                        if have_5_min_column:
                            sample_cols.append(have_5_min_column[0])
                        
                        st.dataframe(survey_df[sample_cols].head(), use_container_width=True)
                    
                    # Apply the filtering logic
                    st.subheader("Filtering Criteria")
                    st.markdown("""
                    **Filters applied:**
                    1. ✅ **TripInOppoDirCode** = 1 or 'Yes' (participant willing for reverse direction)
                    2. ✅ **ElvisStatusCode** ≠ 4 (not rejected/closed)
                    3. ✅ **If ElvisStatusCode** = 6 → **ReverseTrips** must be empty (not already cloned)
                    4. ✅ **Interviewer** ≠ 999 (not system/test records)
                    5. ✅ **Have5MinForSurveCode** = 1 (participant agreed to survey)
                    """)
                    
                    # Start with all records as eligible
                    total_records = len(survey_df)
                    st.info(f"Total records loaded: {total_records:,}")
                    
                    # Apply filters step by step
                    current_mask = pd.Series(True, index=survey_df.index)
                    
                    # NEW FILTER 1: Interviewer not equal to 999
                    if interv_init_column:
                        interviewer_mask = (
                            survey_df[interv_init_column[0]].astype(str) != '999'
                        )
                        current_mask = current_mask & interviewer_mask
                        excluded_interv = (~interviewer_mask).sum()
                    else:
                        st.warning("⚠️ Interviewer column not found - skipping interviewer filter")
                    
                    # NEW FILTER 2: Have5MinForSurveCode = 1 (participant agreed to survey)
                    if have_5_min_column:
                        participation_mask = (
                            survey_df[have_5_min_column[0]].astype(str) == '1'
                        )
                        current_mask = current_mask & participation_mask
                        excluded_participation = (~participation_mask).sum()
                    else:
                        st.warning("⚠️ Participation column not found - skipping participation filter")
                    
                    # Condition 1: TripInOppoDirCode = 1 or 'yes' or 'Yes'
                    if trip_oppo_dir_column:
                        oppo_dir_mask = (
                            survey_df[trip_oppo_dir_column[0]].astype(str).str.lower().isin(['1', 'yes'])
                        )
                        current_mask = current_mask & oppo_dir_mask
                        excluded_oppo_dir = (~oppo_dir_mask).sum()
                    else:
                        st.warning("⚠️ TripInOppoDir column not found - skipping this condition")
                    
                    # Condition 2: ElvisStatusCode != 4
                    if elvis_status_code_column:
                        status_mask = (
                            survey_df[elvis_status_code_column[0]].astype(str) != '4'
                        )
                        current_mask = current_mask & status_mask
                        excluded_status = (~status_mask).sum()
                    else:
                        st.warning("⚠️ ElvisStatusCode column not found - skipping this condition")
                    
                    # Condition 3: If ElvisStatusCode == 6, then ReverseTrips must be empty
                    if elvis_status_code_column and reverse_trips_column:
                        status_6_mask = (
                            (survey_df[elvis_status_code_column[0]].astype(str) != '6') |
                            (
                                (survey_df[elvis_status_code_column[0]].astype(str) == '6') &
                                (
                                    survey_df[reverse_trips_column[0]].isna() |
                                    (survey_df[reverse_trips_column[0]].astype(str).str.strip() == '')
                                )
                            )
                        )
                        current_mask = current_mask & status_6_mask
                        excluded_status_6 = (~status_6_mask).sum()
                    else:
                        if not elvis_status_code_column:
                            st.warning("⚠️ ElvisStatusCode column not found - skipping ReverseTrips condition")
                        elif not reverse_trips_column:
                            st.warning("⚠️ ReverseTrips column not found - skipping ReverseTrips condition")
                    
                    # Get clonable records
                    clonable_df = survey_df[current_mask].copy()
                    
                    # Show filtering summary
                    st.subheader("📊 Filtering Summary")
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("Total Records", f"{total_records:,}")
                    
                    with col2:
                        excluded = total_records - len(clonable_df)
                        st.metric("Records Excluded", f"{excluded:,}")
                    
                    with col3:
                        st.metric("Clonable Records", f"{len(clonable_df):,}")
                    
                    # Show breakdown of exclusions
                    with st.expander("📈 Detailed Exclusion Breakdown"):
                        if 'excluded_interv' in locals():
                            st.write(f"• Interviewer = 999: {excluded_interv:,} records")
                        if 'excluded_participation' in locals():
                            st.write(f"• Non-participating (code ≠ 1): {excluded_participation:,} records")
                        if 'excluded_oppo_dir' in locals():
                            st.write(f"• Not willing for reverse direction: {excluded_oppo_dir:,} records")
                        if 'excluded_status' in locals():
                            st.write(f"• Elvis status = 4: {excluded_status:,} records")
                        if 'excluded_status_6' in locals():
                            st.write(f"• Already cloned (status 6 with ReverseTrips): {excluded_status_6:,} records")
                        
                        # Show remaining after each filter (if available)
                        st.write("---")
                        st.write("**Remaining after each filter:**")
                        remaining_counts = {}
                        
                        # Calculate step-by-step remaining counts
                        temp_mask = pd.Series(True, index=survey_df.index)
                        
                        if interv_init_column:
                            temp_mask = temp_mask & (survey_df[interv_init_column[0]].astype(str) != '999')
                            remaining_counts["After Interviewer filter"] = temp_mask.sum()
                        
                        if have_5_min_column:
                            temp_mask = temp_mask & (survey_df[have_5_min_column[0]].astype(str) == '1')
                            remaining_counts["After Participation filter"] = temp_mask.sum()
                        
                        if trip_oppo_dir_column:
                            temp_mask = temp_mask & survey_df[trip_oppo_dir_column[0]].astype(str).str.lower().isin(['1', 'yes'])
                            remaining_counts["After Reverse Willing filter"] = temp_mask.sum()
                        
                        if elvis_status_code_column:
                            temp_mask = temp_mask & (survey_df[elvis_status_code_column[0]].astype(str) != '4')
                            remaining_counts["After Status ≠ 4 filter"] = temp_mask.sum()
                        
                        if elvis_status_code_column and reverse_trips_column:
                            status_6_mask_temp = (
                                (survey_df[elvis_status_code_column[0]].astype(str) != '6') |
                                (
                                    (survey_df[elvis_status_code_column[0]].astype(str) == '6') &
                                    (
                                        survey_df[reverse_trips_column[0]].isna() |
                                        (survey_df[reverse_trips_column[0]].astype(str).str.strip() == '')
                                    )
                                )
                            )
                            temp_mask = temp_mask & status_6_mask_temp
                            remaining_counts["After Already Cloned filter"] = temp_mask.sum()
                        
                        for step, count in remaining_counts.items():
                            st.write(f"• {step}: {count:,} records")
                    
                    if not clonable_df.empty:
                        # Group by route and collect IDs
                        route_groups = {}
                        
                        for idx, row in clonable_df.iterrows():
                            route_code = row.get(route_survey_column[0], 'Unknown') if route_survey_column else 'Unknown'
                            route_name = row.get(route_survey_name_column[0], 'Unknown') if route_survey_name_column else 'Unknown'
                            record_id = row.get('id', '')
                            
                            if pd.notna(route_code) and pd.notna(record_id):
                                route_key = str(route_code)
                                if route_key not in route_groups:
                                    route_groups[route_key] = {
                                        'route_code': route_code,
                                        'route_name': route_name if pd.notna(route_name) else route_code,
                                        'ids': []
                                    }
                                route_groups[route_key]['ids'].append(str(record_id))
                        
                        # Convert to DataFrame
                        clonable_list = []
                        for route_data in route_groups.values():
                            # Sort IDs for better readability
                            try:
                                sorted_ids = sorted(route_data['ids'], key=lambda x: int(x) if x.isdigit() else x)
                            except:
                                sorted_ids = sorted(route_data['ids'])
                            
                            # Join IDs with comma separator
                            ids_string = ', '.join(sorted_ids)
                            
                            clonable_list.append({
                                'Route Code': route_data['route_code'],
                                'Route Name': route_data['route_name'],
                                'Clonable IDs': ids_string,
                                'Count': len(route_data['ids'])
                            })
                        
                        # Create the final dataframe
                        if clonable_list:
                            clonable_summary_df = pd.DataFrame(clonable_list)
                            
                            # Sort by route code
                            def sort_route_key(route):
                                if pd.isna(route):
                                    return (2, '')
                                route_str = str(route)
                                # Try to extract numeric part for sorting
                                import re
                                numbers = re.findall(r'\d+', route_str)
                                if numbers:
                                    return (0, int(numbers[0]), route_str)
                                else:
                                    return (1, route_str)
                            
                            clonable_summary_df['sort_key'] = clonable_summary_df['Route Code'].apply(sort_route_key)
                            clonable_summary_df = clonable_summary_df.sort_values('sort_key').drop(columns=['sort_key'])
                            
                            # Reset index
                            clonable_summary_df = clonable_summary_df.reset_index(drop=True)
                            clonable_summary_df.index = clonable_summary_df.index + 1
                            
                            # Display statistics
                            st.subheader("📈 Clonable IDs Summary")
                            col1, col2, col3, col4 = st.columns(4)
                            
                            with col1:
                                st.metric("Routes with Clonable IDs", len(clonable_summary_df))
                            
                            with col2:
                                total_ids = clonable_summary_df['Count'].sum()
                                st.metric("Total Clonable IDs", total_ids)
                            
                            with col3:
                                avg_per_route = total_ids / len(clonable_summary_df) if len(clonable_summary_df) > 0 else 0
                                st.metric("Avg IDs per Route", f"{avg_per_route:.1f}")
                            
                            with col4:
                                max_ids = clonable_summary_df['Count'].max() if not clonable_summary_df.empty else 0
                                st.metric("Max IDs in a Route", max_ids)
                            
                            # Show top routes with most clonable IDs
                            if not clonable_summary_df.empty:
                                top_routes = clonable_summary_df.nlargest(5, 'Count')
                                st.write("**Top 5 Routes with Most Clonable IDs:**")
                                for i, (_, row) in enumerate(top_routes.iterrows(), 1):
                                    st.write(f"{i}. **{row['Route Code']}** - {row['Count']} IDs")
                            
                            # Search and filter
                            st.subheader("🔍 Search and Filter Results")
                            
                            # Add search functionality
                            search_term = st.text_input("Search Routes:", "", key="clonable_search")
                            
                            if search_term:
                                filtered_df = clonable_summary_df[
                                    clonable_summary_df['Route Code'].astype(str).str.contains(search_term, case=False, na=False) |
                                    clonable_summary_df['Route Name'].astype(str).str.contains(search_term, case=False, na=False) |
                                    clonable_summary_df['Clonable IDs'].astype(str).str.contains(search_term, case=False, na=False)
                                ]
                            else:
                                filtered_df = clonable_summary_df.copy()
                            
                            # Display the table
                            st.dataframe(
                                filtered_df[['Route Code', 'Route Name', 'Clonable IDs', 'Count']],
                                use_container_width=True,
                                height=600,
                                column_config={
                                    "Clonable IDs": st.column_config.Column(
                                        width="large",
                                        help="IDs that can be cloned for this route"
                                    ),
                                    "Count": st.column_config.NumberColumn(
                                        format="%d",
                                        help="Number of clonable IDs"
                                    )
                                }
                            )
                            
                            # Download button
                            csv_data = clonable_summary_df.to_csv(index=False)
                            st.download_button(
                                label="📥 Download Clonable IDs",
                                data=csv_data,
                                file_name=f"{st.session_state['selected_project'].lower()}_clonable_ids_by_route.csv",
                                mime="text/csv",
                                help="Download the list of clonable IDs grouped by route"
                            )
                            
                            # Additional analysis
                            with st.expander("📊 Additional Analysis"):
                                import matplotlib.pyplot as plt
                                # Show distribution of clonable IDs by route
                                if len(clonable_summary_df) > 1:
                                    st.write("**Distribution of Clonable IDs per Route:**")
                                    fig, ax = plt.subplots(figsize=(10, 4))
                                    ax.hist(clonable_summary_df['Count'], bins=20, edgecolor='black')
                                    ax.set_xlabel('Number of Clonable IDs')
                                    ax.set_ylabel('Number of Routes')
                                    ax.set_title('Distribution of Clonable IDs per Route')
                                    st.pyplot(fig)
                                
                                # Show sample of clonable records
                                st.write("**Sample of Clonable Records (10 records):**")
                                sample_cols = ['id']
                                if route_survey_column:
                                    sample_cols.append(route_survey_column[0])
                                if route_survey_name_column:
                                    sample_cols.append(route_survey_name_column[0])
                                if interv_init_column:
                                    sample_cols.append(interv_init_column[0])
                                if have_5_min_column:
                                    sample_cols.append(have_5_min_column[0])
                                
                                st.dataframe(clonable_df[sample_cols].head(10), use_container_width=True)
                        else:
                            st.info("No clonable IDs found based on the criteria.")
                    else:
                        st.info("No records meet the clonable criteria.")
                        
                except Exception as e:
                    st.error(f"❌ Error loading or processing clonable IDs: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())
                    
                    # Fallback: Try using existing dataframes if direct fetch fails
                    st.info("⚠️ Trying fallback to existing dataframes...")
                    
                    # You can add fallback logic here if needed

            # Home button at the bottom
            if st.button("Home Page"):
                st.query_params["page"] = "main"
                st.rerun()


        def show_refusal_analysis(refusal_analysis_df, refusal_race_df):
            """
            Display comprehensive refusal analysis statistics using the master tables
            """
            st.title("📊 Refusal Analysis Dashboard")
            refusal_analysis_df['INTERV_INIT'] = refusal_analysis_df['INTERV_INIT'].astype(str)
            refusal_analysis_df = refusal_analysis_df[refusal_analysis_df['INTERV_INIT'] != "999"]
            refusal_analysis_df = refusal_analysis_df.iloc[1:].copy()
            
            if refusal_analysis_df.empty:
                st.warning("No refusal data available. Please sync data first.")
                if st.button("Sync Data"):
                    st.query_params["page"] = "main"
                    st.rerun()
                return
            
            # Filter for refusals only (HAVE_5_MIN_FOR_SURVECode != '1')
            refusal_df = refusal_analysis_df[refusal_analysis_df['HAVE_5_MIN_FOR_SURVECode'].astype(str) != '1'].copy()
            
            if refusal_df.empty:
                st.info("No refusal records found in the dataset (all responses were participations).")
                return
            
            # FIX: Convert DATE_SUBMITTED to datetime safely
            # Use LocalTime if available, otherwise use DATE_SUBMITTED
            if 'LocalTime' in refusal_df.columns:
                refusal_df['Survey_Date'] = pd.to_datetime(refusal_df['LocalTime'], errors='coerce')
            elif 'DATE_SUBMITTED' in refusal_df.columns:
                refusal_df['Survey_Date'] = pd.to_datetime(refusal_df['DATE_SUBMITTED'], errors='coerce')
            
            # Create tabs for different refusal statistics
            tab1, tab2, tab3, tab4 = st.tabs([
                "📋 Refusal Overview", "👥 Interviewer Refusals", "🛣️ Route Refusals", "🧑‍🤝‍🧑 Demographics"
            ])
            
            with tab1:
                st.subheader("Refusal Overview")
                
                # Overall refusal statistics
                total_refusals = len(refusal_df)
                total_approaches = len(refusal_analysis_df)
                refusal_rate = (total_refusals / total_approaches * 100) if total_approaches > 0 else 0
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Refusals", f"{total_refusals:,}")
                with col2:
                    st.metric("Total Approaches", f"{total_approaches:,}")
                with col3:
                    st.metric("Overall Refusal Rate", f"{refusal_rate:.1f}%")
                
                # Refusal reasons breakdown
                st.subheader("Refusal Reasons Breakdown")
                
                if 'HAVE_5_MIN_LABEL' in refusal_df.columns:
                    refusal_reasons = refusal_df['HAVE_5_MIN_LABEL'].value_counts().reset_index()
                    refusal_reasons.columns = ['Reason', 'Count']
                    refusal_reasons['Percentage'] = (refusal_reasons['Count'] / refusal_reasons['Count'].sum() * 100).round(2)
                    
                    col1, col2 = st.columns([1, 2])
                    
                    with col1:
                        st.dataframe(refusal_reasons, use_container_width=True, hide_index=True)
                    
                    with col2:
                        if not refusal_reasons.empty:
                            fig_reasons = px.pie(refusal_reasons, values='Count', names='Reason',
                                            title="Refusal Reasons Distribution")
                            st.plotly_chart(fig_reasons, use_container_width=True)
                else:
                    st.warning("Refusal reason data not available.")
                
                # --- Daily refusal trend (FULLY FIXED) ---
                st.subheader("Daily Refusal Trend")

                # Ensure DATE_SUBMITTED is converted to datetime safely
                if 'DATE_SUBMITTED' in refusal_df.columns:
                    refusal_df['DATE_SUBMITTED'] = pd.to_datetime(refusal_df['DATE_SUBMITTED'], errors='coerce')

                if 'DATE_SUBMITTED' in refusal_df.columns and not refusal_df['DATE_SUBMITTED'].isna().all():
                    # Remove rows with invalid or missing DATE_SUBMITTED
                    valid_dates_df = refusal_df.dropna(subset=['DATE_SUBMITTED'])

                    if not valid_dates_df.empty:
                        # Group by calendar date
                        daily_refusals = (
                            valid_dates_df
                            .groupby(valid_dates_df['DATE_SUBMITTED'].dt.date)
                            .size()
                            .reset_index(name="Refusals")
                        )

                        daily_refusals.columns = ['Date', 'Refusals']

                        if not daily_refusals.empty:
                            fig_trend = px.line(
                                daily_refusals,
                                x='Date',
                                y='Refusals',
                                title="Daily Refusal Count Trend"
                            )
                            st.plotly_chart(fig_trend, use_container_width=True)
                        else:
                            st.info("No valid date data available for trend analysis.")
                    else:
                        st.info("No valid dates found in the data.")
                else:
                    st.info("Date data not available for trend analysis.")
            
            with tab2:
                st.subheader("Interviewer Refusal Analysis")
                
                # FIX: Check if required columns exist
                if 'INTERV_INIT' not in refusal_analysis_df.columns:
                    st.warning("Interviewer data not available.")
                else:
                    # Get ALL interviewers from the full dataset (including those with 0 refusals)
                    all_interviewers = refusal_analysis_df['INTERV_INIT'].unique()
                    
                    # Interviewer-level refusal statistics - include ALL interviewers
                    interviewer_refusals = refusal_df.groupby('INTERV_INIT').size().reset_index(name='Total Refusals')
                    
                    # Calculate success rates for comparison - for ALL interviewers
                    interviewer_success = refusal_analysis_df.groupby('INTERV_INIT').agg({
                        'HAVE_5_MIN_LABEL': lambda x: (x == 'Yes I can participate in the survey (have 5 min+)').sum() if 'HAVE_5_MIN_LABEL' in refusal_analysis_df.columns else 0,
                        'HAVE_5_MIN_FOR_SURVECode': 'count'
                    }).reset_index()
                    
                    interviewer_success.columns = ['INTERV_INIT', 'Successful Surveys', 'Total Approaches']
                    interviewer_success['Success Rate %'] = (interviewer_success['Successful Surveys'] / interviewer_success['Total Approaches'] * 100).round(2)
                    
                    # FIX: Use outer merge to include ALL interviewers, even those with 0 refusals
                    interviewer_stats = pd.merge(interviewer_success, interviewer_refusals, on='INTERV_INIT', how='left')
                    
                    # Fill NaN values for interviewers with 0 refusals
                    interviewer_stats['Total Refusals'] = interviewer_stats['Total Refusals'].fillna(0)
                    interviewer_stats['Refusal Rate %'] = (interviewer_stats['Total Refusals'] / interviewer_stats['Total Approaches'] * 100).round(2)
                    
                    # Rename for display
                    interviewer_stats_display = interviewer_stats.rename(columns={'INTERV_INIT': 'Interviewer'})
                    
                    # FIX: Show ALL interviewers, not just top 10
                    st.write(f"**All Interviewers Refusal Statistics ({len(interviewer_stats_display)} total)**")
                    
                    # Add search and filter functionality for large tables
                    search_interviewer = st.text_input("Search Interviewers:", "")
                    
                    if search_interviewer:
                        filtered_interviewers = interviewer_stats_display[
                            interviewer_stats_display['Interviewer'].str.contains(search_interviewer, case=False, na=False)
                        ]
                    else:
                        filtered_interviewers = interviewer_stats_display
                    
                    # Display all interviewers with pagination or scroll
                    st.dataframe(
                        filtered_interviewers[['Interviewer', 'Total Approaches', 'Successful Surveys', 'Success Rate %', 'Total Refusals', 'Refusal Rate %']],
                        use_container_width=True,
                        hide_index=True,
                        height=400
                    )
                    
                    # Chart: Interviewers with highest refusal rates (still show top for visualization)
                    if not interviewer_stats_display.empty:
                        st.subheader("Top Interviewers by Refusal Rate")
                        # Filter out interviewers with 0 approaches to avoid division errors
                        valid_interviewers = interviewer_stats_display[interviewer_stats_display['Total Approaches'] > 0]
                        if not valid_interviewers.empty:
                            top_refusal_interviewers = valid_interviewers.nlargest(min(10, len(valid_interviewers)), 'Refusal Rate %')
                            
                            if not top_refusal_interviewers.empty:
                                fig_interv = px.bar(top_refusal_interviewers, 
                                                x='Interviewer', y='Refusal Rate %',
                                                title=f"Top {len(top_refusal_interviewers)} Interviewers by Refusal Rate",
                                                hover_data=['Total Refusals', 'Total Approaches'])
                                fig_interv.update_layout(xaxis_tickangle=-45)
                                st.plotly_chart(fig_interv, use_container_width=True)

            with tab3:
                st.subheader("Route Refusal Analysis")
                
                # FIX: Check if required columns exist
                if 'ROUTE_MAIN' not in refusal_df.columns:
                    st.warning("Route data not available.")
                else:
                    # Get ALL routes from the full dataset (including those with 0 refusals)
                    all_routes = refusal_analysis_df['ROUTE_MAIN'].unique()
                    
                    # Route-level refusal statistics
                    route_refusals = refusal_df.groupby('ROUTE_MAIN').size().reset_index(name='Total Refusals')
                    
                    # Calculate success rates for comparison - for ALL routes
                    route_success = refusal_analysis_df.groupby('ROUTE_MAIN').agg({
                        'HAVE_5_MIN_LABEL': lambda x: (x == 'Yes I can participate in the survey (have 5 min+)').sum() if 'HAVE_5_MIN_LABEL' in refusal_analysis_df.columns else 0,
                        'HAVE_5_MIN_FOR_SURVECode': 'count'
                    }).reset_index()
                    
                    route_success.columns = ['ROUTE_MAIN', 'Successful Surveys', 'Total Approaches']
                    route_success['Success Rate %'] = (route_success['Successful Surveys'] / route_success['Total Approaches'] * 100).round(2)
                    
                    # FIX: Use outer merge to include ALL routes, even those with 0 refusals
                    route_stats = pd.merge(route_success, route_refusals, on='ROUTE_MAIN', how='left')
                    
                    # Fill NaN values for routes with 0 refusals
                    route_stats['Total Refusals'] = route_stats['Total Refusals'].fillna(0)
                    route_stats['Refusal Rate %'] = (route_stats['Total Refusals'] / route_stats['Total Approaches'] * 100).round(2)
                    
                    # Rename for display
                    route_stats_display = route_stats.rename(columns={'ROUTE_MAIN': 'Route'})
                    
                    # FIX: Show ALL routes, not just top 10
                    st.write(f"**All Routes Refusal Statistics ({len(route_stats_display)} total)**")
                    
                    # Add search and filter functionality for large tables
                    search_route = st.text_input("Search Routes:", "")
                    
                    if search_route:
                        filtered_routes = route_stats_display[
                            route_stats_display['Route'].astype(str).str.contains(search_route, case=False, na=False)
                        ]
                    else:
                        filtered_routes = route_stats_display
                    
                    # Display all routes with pagination or scroll
                    st.dataframe(
                        filtered_routes[['Route', 'Total Approaches', 'Successful Surveys', 'Success Rate %', 'Total Refusals', 'Refusal Rate %']],
                        use_container_width=True,
                        hide_index=True,
                        height=400
                    )
                    
                    # Chart: Routes with highest refusal rates (still show top for visualization)
                    if not route_stats_display.empty:
                        st.subheader("Top Routes by Refusal Rate")
                        # Filter out routes with 0 approaches to avoid division errors
                        valid_routes = route_stats_display[route_stats_display['Total Approaches'] > 0]
                        if not valid_routes.empty:
                            top_refusal_routes = valid_routes.nlargest(min(10, len(valid_routes)), 'Refusal Rate %')
                            
                            if not top_refusal_routes.empty:
                                fig_routes = px.bar(top_refusal_routes, 
                                                x='Route', y='Refusal Rate %',
                                                title=f"Top {len(top_refusal_routes)} Routes by Refusal Rate",
                                                hover_data=['Total Refusals', 'Total Approaches'])
                                fig_routes.update_layout(xaxis_tickangle=-45)
                                st.plotly_chart(fig_routes, use_container_width=True) 

            with tab4:
                st.subheader("Demographic Analysis of Refusals")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Age distribution of refusals - UPDATED to show total vs refusals
                    st.write("**Age Distribution Analysis**")
                    if 'AGE_GROUP_LABEL' in refusal_analysis_df.columns:
                        # Calculate total counts from full dataset
                        age_totals = refusal_analysis_df['AGE_GROUP_LABEL'].value_counts().reset_index()
                        age_totals.columns = ['Age Group', 'Total Count']
                        
                        # Calculate refusal counts
                        age_refusals = refusal_df['AGE_GROUP_LABEL'].value_counts().reset_index()
                        age_refusals.columns = ['Age Group', 'Refusal Count']
                        
                        # Merge total and refusal counts
                        age_analysis = pd.merge(age_totals, age_refusals, on='Age Group', how='left')
                        age_analysis['Refusal Count'] = age_analysis['Refusal Count'].fillna(0)
                        age_analysis['Refusal Rate %'] = (age_analysis['Refusal Count'] / age_analysis['Total Count'] * 100).round(2)
                        age_analysis['Participation Count'] = age_analysis['Total Count'] - age_analysis['Refusal Count']
                        age_analysis['Participation Rate %'] = (age_analysis['Participation Count'] / age_analysis['Total Count'] * 100).round(2)
                        
                        # Display the comprehensive analysis
                        st.dataframe(
                            age_analysis[['Age Group', 'Total Count', 'Participation Count', 'Participation Rate %', 'Refusal Count', 'Refusal Rate %']],
                            use_container_width=True,
                            hide_index=True,
                            height=300
                        )
                        
                        # Chart: Refusal rate by age group
                        if not age_analysis.empty:
                            fig_age = px.bar(age_analysis, x='Age Group', y='Refusal Rate %',
                                        title="Refusal Rate by Age Group",
                                        hover_data=['Total Count', 'Refusal Count'])
                            st.plotly_chart(fig_age, use_container_width=True)
                    else:
                        st.info("Age data not available.")
                
                with col2:
                    # Gender distribution of refusals - UPDATED to show total vs refusals
                    st.write("**Gender Distribution Analysis**")
                    
                    if 'YOUR_GENDERCode' in refusal_analysis_df.columns:
                        # Calculate total counts from full dataset
                        gender_totals = refusal_analysis_df['YOUR_GENDERCode'].value_counts().reset_index()
                        gender_totals.columns = ['Gender Code', 'Total Count']
                        
                        # Calculate refusal counts
                        gender_refusals_counts = refusal_df['YOUR_GENDERCode'].value_counts().reset_index()
                        gender_refusals_counts.columns = ['Gender Code', 'Refusal Count']
                        
                        # Map codes to labels
                        gender_mapping = {
                            '1': 'Male',
                            '2': 'Female', 
                            '3': 'Other'
                        }
                        
                        # Merge total and refusal counts
                        gender_analysis = pd.merge(gender_totals, gender_refusals_counts, on='Gender Code', how='left')
                        gender_analysis['Refusal Count'] = gender_analysis['Refusal Count'].fillna(0)
                        gender_analysis['Gender'] = gender_analysis['Gender Code'].map(gender_mapping)
                        gender_analysis['Gender'] = gender_analysis['Gender'].fillna('Not Specified')
                        gender_analysis['Refusal Rate %'] = (gender_analysis['Refusal Count'] / gender_analysis['Total Count'] * 100).round(2)
                        gender_analysis['Participation Count'] = gender_analysis['Total Count'] - gender_analysis['Refusal Count']
                        gender_analysis['Participation Rate %'] = (gender_analysis['Participation Count'] / gender_analysis['Total Count'] * 100).round(2)
                        
                        # Display the comprehensive analysis
                        st.dataframe(
                            gender_analysis[['Gender', 'Total Count', 'Participation Count', 'Participation Rate %', 'Refusal Count', 'Refusal Rate %']],
                            use_container_width=True,
                            hide_index=True,
                            height=300
                        )
                        
                        # Chart: Refusal rate by gender
                        if not gender_analysis.empty:
                            fig_gender = px.bar(gender_analysis, x='Gender', y='Refusal Rate %',
                                            title="Refusal Rate by Gender",
                                            hover_data=['Total Count', 'Refusal Count'])
                            st.plotly_chart(fig_gender, use_container_width=True)
                    
                    else:
                        st.info("Gender data not available.")
                
                # Language distribution of refusals - UPDATED to show total vs refusals
                st.write("**Language Distribution Analysis**")
                if 'LANGUAGE_LABEL' in refusal_analysis_df.columns:
                    # Calculate total counts from full dataset
                    language_totals = refusal_analysis_df['LANGUAGE_LABEL'].value_counts().reset_index()
                    language_totals.columns = ['Language', 'Total Count']
                    
                    # Calculate refusal counts
                    language_refusals_counts = refusal_df['LANGUAGE_LABEL'].value_counts().reset_index()
                    language_refusals_counts.columns = ['Language', 'Refusal Count']
                    
                    # Merge total and refusal counts
                    language_analysis = pd.merge(language_totals, language_refusals_counts, on='Language', how='left')
                    language_analysis['Refusal Count'] = language_analysis['Refusal Count'].fillna(0)
                    language_analysis['Refusal Rate %'] = (language_analysis['Refusal Count'] / language_analysis['Total Count'] * 100).round(2)
                    language_analysis['Participation Count'] = language_analysis['Total Count'] - language_analysis['Refusal Count']
                    language_analysis['Participation Rate %'] = (language_analysis['Participation Count'] / language_analysis['Total Count'] * 100).round(2)
                    
                    # Display top languages (show all if less than 10, otherwise top 10)
                    display_languages = language_analysis.nlargest(10, 'Total Count') if len(language_analysis) > 10 else language_analysis
                    
                    st.dataframe(
                        display_languages[['Language', 'Total Count', 'Participation Count', 'Participation Rate %', 'Refusal Count', 'Refusal Rate %']],
                        use_container_width=True,
                        hide_index=True,
                        height=300
                    )
                    
                    # Chart: Refusal rate by language (top 10)
                    if not display_languages.empty:
                        fig_language = px.bar(display_languages, x='Language', y='Refusal Rate %',
                                        title="Refusal Rate by Language (Top 10)",
                                        hover_data=['Total Count', 'Refusal Count'])
                        fig_language.update_layout(xaxis_tickangle=-45)
                        st.plotly_chart(fig_language, use_container_width=True)
                else:
                    st.info("Language data not available.")
                
                # Race distribution of refusals - UPDATED to show total vs refusals
                st.write("**Race/Ethnicity Distribution Analysis**")
                if not refusal_race_df.empty and 'RACE_CATEGORY' in refusal_race_df.columns:
                    # For race data, we need to handle it differently since it's in a separate table
                    # Count total race records (assuming refusal_race_df contains all race data)
                    race_totals = refusal_race_df['RACE_CATEGORY'].value_counts().reset_index()
                    race_totals.columns = ['Race/Ethnicity', 'Total Count']
                    
                    # For refusal counts, we need to merge with refusal data
                    if 'RESPONDENT_ID' in refusal_race_df.columns and 'RESPONDENT_ID' in refusal_df.columns:
                        refusal_race_merged = refusal_race_df[refusal_race_df['RESPONDENT_ID'].isin(refusal_df['RESPONDENT_ID'])]
                        race_refusals_counts = refusal_race_merged['RACE_CATEGORY'].value_counts().reset_index()
                        race_refusals_counts.columns = ['Race/Ethnicity', 'Refusal Count']
                        
                        # Merge total and refusal counts
                        race_analysis = pd.merge(race_totals, race_refusals_counts, on='Race/Ethnicity', how='left')
                        race_analysis['Refusal Count'] = race_analysis['Refusal Count'].fillna(0)
                        race_analysis['Refusal Rate %'] = (race_analysis['Refusal Count'] / race_analysis['Total Count'] * 100).round(2)
                        race_analysis['Participation Count'] = race_analysis['Total Count'] - race_analysis['Refusal Count']
                        race_analysis['Participation Rate %'] = (race_analysis['Participation Count'] / race_analysis['Total Count'] * 100).round(2)
                        
                        # Display top races (show all if less than 10, otherwise top 10)
                        display_races = race_analysis.nlargest(10, 'Total Count') if len(race_analysis) > 10 else race_analysis
                        
                        st.dataframe(
                            display_races[['Race/Ethnicity', 'Total Count', 'Participation Count', 'Participation Rate %', 'Refusal Count', 'Refusal Rate %']],
                            use_container_width=True,
                            hide_index=True,
                            height=300
                        )
                        
                        # Chart: Refusal rate by race (top 10)
                        if not display_races.empty:
                            fig_race = px.bar(display_races, x='Race/Ethnicity', y='Refusal Rate %',
                                        title="Refusal Rate by Race/Ethnicity (Top 10)",
                                        hover_data=['Total Count', 'Refusal Count'])
                            fig_race.update_layout(xaxis_tickangle=-45)
                            st.plotly_chart(fig_race, use_container_width=True)
                    else:
                        st.info("Cannot link race data with refusal data - RESPONDENT_ID missing.")
                else:
                    st.info("Race/ethnicity data not available.")
            
            # Summary insights
            st.subheader("📈 Key Insights")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Refusal Statistics:**")
                st.write(f"• Total refusals: {total_refusals:,}")
                st.write(f"• Overall refusal rate: {refusal_rate:.1f}%")
                
                if 'interviewer_stats_display' in locals() and not interviewer_stats_display.empty:
                    max_refusal_interviewer = interviewer_stats_display.nlargest(1, 'Refusal Rate %')
                    if not max_refusal_interviewer.empty:
                        st.write(f"• Highest refusal rate interviewer: {max_refusal_interviewer.iloc[0]['Interviewer']} ({max_refusal_interviewer.iloc[0]['Refusal Rate %']}%)")
                
                if 'route_stats_display' in locals() and not route_stats_display.empty:
                    max_refusal_route = route_stats_display.nlargest(1, 'Refusal Rate %')
                    if not max_refusal_route.empty:
                        st.write(f"• Highest refusal rate route: {max_refusal_route.iloc[0]['Route']} ({max_refusal_route.iloc[0]['Refusal Rate %']}%)")
            
            with col2:
                st.write("**Recommendations:**")
                st.write("• Focus training on interviewers with high refusal rates")
                st.write("• Investigate routes with consistently high refusal rates")
                st.write("• Consider language assistance for non-English speakers")
                st.write("• Review approach techniques in high-refusal demographic groups")
            
            # Navigation
            if st.button("🔙 Home Page"):
                st.query_params["page"] = "main"
                st.rerun()

        def location_maps_page():
            """
            Display the location maps interface integrated with existing filter structure
            """
            st.title("🗺️ Location Maps")

            try:
                # Load the Elvis data for mapping
                from automated_refresh_flow_new import PROJECTS, fetch_data
                from automated_sync_flow_constants_maps import KCATA_HEADER_MAPPING
                from automated_sync_flow_utils import prepare_location_data

                project_config = PROJECTS[st.session_state["selected_project"]]
                elvis_config = project_config["databases"]["elvis"]
                table_name = elvis_config["table"]
                database_name = elvis_config["database"]

                with st.spinner("🔄 Loading location data..."):
                    csv_buffer = fetch_data(database_name, table_name)

                    if not csv_buffer:
                        st.error("❌ Failed to fetch location data from Elvis table")
                        return

                    csv_buffer.seek(0)
                    elvis_df = pd.read_csv(csv_buffer, low_memory=False)

                    # Safely drop junk header row if present
                    if elvis_df.iloc[0].isnull().all():
                        elvis_df = elvis_df.drop(index=0)

                    elvis_df = elvis_df.reset_index(drop=True)

                    # Apply column renaming
                    try:
                        elvis_df.columns = elvis_df.columns.str.strip()
                        elvis_df = elvis_df.rename(columns=KCATA_HEADER_MAPPING)
                        st.success("✅ Data loaded and columns renamed successfully!")
                    except Exception as e:
                        st.warning(
                            f"Column renaming failed: {str(e)}. Using original column names."
                        )
                    # ------------------------
                    # Apply LACMTA Agency Filter
                    # ------------------------
                    selected_agency = st.session_state.get("selected_agency")
                    selected_project = st.session_state.get("selected_project")

                    if selected_project == "LACMTA_FEEDER" and selected_agency and selected_agency != "All":
                        elvis_df, _ = apply_lacmta_agency_filter(
                            df=elvis_df,
                            project=selected_project,
                            agency=selected_agency,
                            bucket_name=os.getenv("bucket_name"),
                            project_config=project_config
                        )

                    # Prepare location data
                    location_df, unique_routes = prepare_location_data(elvis_df)

                    if location_df.empty:
                        st.warning("No location data available after filtering.")
                        if st.button("🔙 Home Page", key="location_maps_empty_home"):
                            st.query_params["page"] = "main"
                            st.rerun()
                        return

                    # Search (consistent with other pages)
                    search_query = st.text_input("🔍 Search", value="")
                    filtered_locations = filter_dataframe(location_df, search_query)

                    # Statistics
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("📍 Total Points", len(filtered_locations))
                    with col2:
                        st.metric("🛣️ Unique Routes", filtered_locations["route_code"].nunique())
                    with col3:
                        st.metric(
                            "📊 Location Types",
                            filtered_locations["location_type"].nunique(),
                        )
                    with col4:
                        st.metric("👥 Survey Records", filtered_locations["id"].nunique())

                    # Initialize session state
                    if "location_routes" not in st.session_state:
                        st.session_state.location_routes = []

                    if "location_types" not in st.session_state:
                        st.session_state.location_types = sorted(
                            location_df["location_type"].dropna().unique().tolist()
                        )

                    st.subheader("📍 Map Filters")

                    # Filter options
                    route_options = sorted(
                        location_df["route_code"].dropna().unique().tolist()
                    )
                    location_type_options = sorted(
                        location_df["location_type"].dropna().unique().tolist()
                    )

                    col1, col2, col3 = st.columns(3)

                    with col1:
                        selected_routes = st.multiselect(
                            "Select Routes:",
                            options=route_options,
                            default=st.session_state.location_routes,
                            key="location_routes_multiselect",
                        )

                    with col2:
                        selected_location_types = st.multiselect(
                            "Select Location Types:",
                            options=location_type_options,
                            default=st.session_state.location_types,
                            key="location_types_multiselect",
                        )

                    with col3:
                        st.write("**Quick Actions**")
                        col3a, col3b = st.columns(2)
                        with col3a:
                            if st.button("Select All Routes", key="location_select_all_routes"):
                                st.session_state.location_routes = route_options
                                st.rerun()
                        with col3b:
                            if st.button("Clear Routes", key="location_clear_routes"):
                                st.session_state.location_routes = []
                                st.rerun()

                    col4, col5 = st.columns(2)
                    with col4:
                        if st.button("Select All Types", key="location_select_all_types"):
                            st.session_state.location_types = location_type_options
                            st.rerun()
                    with col5:
                        if st.button("Clear Types", key="location_clear_types"):
                            st.session_state.location_types = []
                            st.rerun()

                    st.write("**Quick Filters**")
                    q1, q2, q3 = st.columns(3)
                    with q1:
                        if st.button("Just Alighting", key="just_alighting_btn"):
                            st.session_state.location_types = ["Alighting"]
                            st.session_state.location_routes = []
                            st.rerun()
                    with q2:
                        if st.button("Just Origin", key="just_origin_btn"):
                            st.session_state.location_types = ["Origin"]
                            st.session_state.location_routes = []
                            st.rerun()
                    with q3:
                        if st.button("Show All", key="show_all_btn"):
                            st.session_state.location_routes = []
                            st.session_state.location_types = location_type_options
                            st.rerun()

                    # Persist state
                    st.session_state.location_routes = selected_routes
                    st.session_state.location_types = selected_location_types

                    # Apply filters
                    temp_filtered = filtered_locations.copy()

                    if selected_routes:
                        temp_filtered = temp_filtered[
                            temp_filtered["route_code"].isin(selected_routes)
                        ]

                    if selected_location_types:
                        temp_filtered = temp_filtered[
                            temp_filtered["location_type"].isin(selected_location_types)
                        ]

                    # Filter summary
                    filter_info = []
                    if selected_routes:
                        filter_info.append(
                            f"Routes: {len(selected_routes)} selected"
                            if len(selected_routes) > 3
                            else f"Routes: {', '.join(selected_routes)}"
                        )
                    if selected_location_types:
                        filter_info.append(
                            f"Types: {len(selected_location_types)} selected"
                            if len(selected_location_types) > 3
                            else f"Types: {', '.join(selected_location_types)}"
                        )

                    if filter_info:
                        st.info(f"**Active Filters:** {', '.join(filter_info)}")
                    else:
                        st.info("**Showing all routes and location types**")

                    # Data table
                    st.subheader("📍 Location Data")
                    display_columns = [
                        "route_code",
                        "route_name",
                        "location_type",
                        "latitude",
                        "longitude",
                        "address",
                        "city",
                    ]
                    st.dataframe(
                        temp_filtered[display_columns],
                        use_container_width=True,
                        hide_index=True,
                    )

                    # Map
                    st.subheader("🗺️ Interactive Map")

                    if temp_filtered.empty:
                        st.warning("No data available for the selected filters.")
                    else:
                        temp_filtered["latitude"] = pd.to_numeric(
                            temp_filtered["latitude"], errors="coerce"
                        )
                        temp_filtered["longitude"] = pd.to_numeric(
                            temp_filtered["longitude"], errors="coerce"
                        )

                        map_data = temp_filtered.dropna(
                            subset=["latitude", "longitude"]
                        ).copy()

                        if map_data.empty:
                            st.warning("No valid coordinates to display on map.")
                        else:
                            color_map = {
                                "Home": "#1f77b4",
                                "Origin": "#2ca02c",
                                "Boarding": "#ff7f0e",
                                "Alighting": "#d62728",
                                "Destination": "#9467bd",
                            }

                            map_data["color_hex"] = map_data["location_type"].map(color_map)

                            try:
                                st.map(
                                    map_data,
                                    latitude="latitude",
                                    longitude="longitude",
                                    color="color_hex",
                                    size=100,
                                    use_container_width=True,
                                )
                            except Exception:
                                st.map(
                                    map_data,
                                    latitude="latitude",
                                    longitude="longitude",
                                    size=100,
                                    use_container_width=True,
                                )

                            st.sidebar.subheader("🎨 Map Legend")
                            for loc, col in color_map.items():
                                if loc in map_data["location_type"].unique():
                                    st.sidebar.markdown(
                                        f"<span style='color:{col}'>■</span> {loc}",
                                        unsafe_allow_html=True,
                                    )

                            st.sidebar.subheader("📊 Map Stats")
                            st.sidebar.metric("Points on Map", len(map_data))
                            st.sidebar.metric(
                                "Routes on Map", map_data["route_code"].nunique()
                            )

                    # Download
                    st.subheader("📥 Data Export")
                    if st.button("Download Location Data as CSV", key="location_download_btn"):
                        csv_data, file_name = create_csv(
                            temp_filtered, "location_data.csv"
                        )
                        download_csv(csv_data, file_name, "Download Location Data")

            except Exception as e:
                st.error(f"❌ Error loading location data: {str(e)}")
                st.info("Please ensure the Elvis table is available and accessible.")

                with st.expander("🔧 Technical Details"):
                    import traceback
                    st.code(traceback.format_exc())

            # Navigation
            if st.button("Home Page", key="location_maps_home_btn"):
                st.query_params["page"] = "main"
                st.rerun()

        def demographic_review_page(df: pd.DataFrame):
            # --- PAGE CONFIG ---
            st.set_page_config(page_title="Demographic Review", layout="wide")
            st.title("🧭 Demographic Review")

            # --- MAIN LAYOUT ---
            left_col, right_col = st.columns([2, 1])

            # --- RIGHT: QUESTIONS PANEL ---
            with right_col:
                with st.container(height=600, border=True):
                    st.markdown("### Survey Questions")
                    search_query = st.text_input("🔍 Search Question", placeholder="Type to search a question...")

                    # Filter questions
                    all_questions = df["Question"].unique()
                    filtered_questions = [
                        q for q in all_questions if search_query.lower() in q.lower()
                    ]

                    selected_question = st.radio(
                        "Select a Question",
                        filtered_questions,
                        label_visibility="collapsed",
                        index=None,
                    )

            # --- LEFT: GRAPH AREA ---
            with left_col:
                with st.container(height=600, border=True):
                    st.markdown("### Results")

                    if selected_question:
                        question_data = df[df["Question"] == selected_question].copy()
                        question_data = question_data.sort_values(by="Percentage", ascending=False).reset_index(drop=True)

                        # Calculate dynamic width based on number of bars
                        # Each bar gets ~200 pixels of width to show only 5-6 bars at once
                        num_bars = len(question_data)
                        chart_width = num_bars * 200  # 200px per bar

                        # Create the bar chart manually (one trace only)
                        fig = go.Figure()

                        fig.add_trace(
                            go.Bar(
                                x=question_data["Answer Text"],
                                y=question_data["Percentage"],
                                text=[f"{p:.1f}%" for p in question_data["Percentage"]],
                                textposition="outside",
                                hovertemplate="<b>%{x}</b><br>Percentage: %{y:.1f}%<br>Count: %{customdata}<extra></extra>",
                                customdata=question_data["Count"],
                                marker=dict(color="#0068c9"),
                            )
                        )

                        fig.update_layout(
                            xaxis_title=None,
                            yaxis_title=None,
                            yaxis=dict(ticksuffix="%", range=[0, 100]),
                            showlegend=False,
                            height=500,
                            width=chart_width,  # Dynamic width
                            margin=dict(t=50, b=50, l=50, r=50),
                            plot_bgcolor="white",
                            font=dict(color="black"),
                            autosize=False  # Disable autosize
                        )

                        # Use width="content" instead of use_container_width=False
                        st.plotly_chart(fig, width="content")
                    else:
                        st.info("👆 Select a question from the right to view its results.")

        # Layout columns
        header_col1, header_col2, header_col3 = st.columns([2, 2, 1])

        # Header Section
        with header_col1:
            # Button to trigger the entire script
            # Initialize flag
            if "sync_running" not in st.session_state:
                st.session_state.sync_running = False

            if role.upper() != "CLIENT":
                # Initialize sync flags
                if "sync_running" not in st.session_state:
                    st.session_state.sync_running = False
                if "sync_completed" not in st.session_state:
                    st.session_state.sync_completed = False
                
                # Check if sync is already running
                is_sync_running = st.session_state.get("sync_running", False)
                sync_completed = st.session_state.get("sync_completed", False)
                
                # If sync was completed in previous run, reset flags and clear status
                if sync_completed and not is_sync_running:
                    st.session_state.sync_running = False
                    st.session_state.sync_completed = False
                    # Re-read after reset
                    is_sync_running = False
                    sync_completed = False
                
                # Use placeholders for status messages so they can be cleared
                status_placeholder = st.empty()
                warning_placeholder = st.empty()
                
                # Show status message if sync is running (but not if it just completed)
                if is_sync_running and not sync_completed:
                    status_placeholder.info("🔄 Sync is already running. Please wait for it to finish.")
                    warning_placeholder.warning("⚠️ Do not refresh the page or click other buttons while sync is in progress.")
                else:
                    # Clear status messages when sync is not running
                    status_placeholder.empty()
                    warning_placeholder.empty()
                
                # Render button with disabled state based on sync status
                sync_button_clicked = st.button(
                    "Sync" if not is_sync_running else "Sync (running...)",
                    disabled=is_sync_running,
                    key="sync_button"
                )
                
                # Determine if we should execute sync:
                # Only execute if button was clicked AND sync is not already running
                # We don't continue sync on rerun - it must complete in one execution
                should_execute_sync = sync_button_clicked and not is_sync_running
                
                if should_execute_sync:
                    # CRITICAL: Set state immediately to prevent concurrent syncs
                    st.session_state.sync_running = True
                    st.session_state.sync_completed = False
                    
                    # Add session keep-alive mechanism
                    keep_alive_placeholder = st.empty()
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    time_elapsed = st.empty()

                    def update_progress(step, total_steps, message, start_time):
                        progress = step / total_steps
                        progress_bar.progress(progress)
                        elapsed = time.time() - start_time
                        status_text.text(f"🔄 {message}")
                        time_elapsed.text(f"⏱️ Time elapsed: {elapsed:.1f} seconds")
                        keep_alive_placeholder.text(
                            f"⏳ Processing... Step {step} of {total_steps}: {message}"
                        )
                        time.sleep(0.5)

                    start_time = time.time()

                    try:
                        update_progress(1, 12, "Starting sync process...", start_time)
                        
                        # Check if we need to sync to agency schema
                        selected_agency = st.session_state.get("selected_agency", None)
                        selected_project = st.session_state.get("selected_project", "")
                        
                        if selected_project == "LACMTA_FEEDER" and selected_agency and selected_agency != "All":
                            # Create agency schema if it doesn't exist
                            update_progress(2, 12, f"Preparing agency schema: {selected_agency}...", start_time)
                            # agency_schema_name = create_agency_schema(selected_agency)
                            agency_schema_name = f"LACMTA_FEEDER_{selected_agency}"
                            if agency_schema_name:
                                update_progress(3, 12, f"Syncing to agency schema: {agency_schema_name}...", start_time)
                                # Store current schema
                                current_schema = st.session_state.get("schema")
                                
                                try:
                                    # Temporarily switch to agency schema
                                    st.session_state["schema"] = agency_schema_name
                                    
                                    result = fetch_and_process_data(
                                        st.session_state["selected_project"],
                                        agency_schema_name,
                                    )
                                    
                                    # Restore original schema
                                    st.session_state["schema"] = current_schema
                                    
                                except Exception as e:
                                    # Restore original schema on error
                                    st.session_state["schema"] = current_schema
                                    raise e
                            else:
                                update_progress(3, 12, "Agency schema creation failed, syncing to default...", start_time)
                                result = fetch_and_process_data(
                                    st.session_state["selected_project"],
                                    st.session_state["schema"],
                                )
                        else:
                            update_progress(2, 12, "Syncing to default schema...", start_time)
                            update_progress(3, 12, "Fetching and processing data from Snowflake...", start_time)
                            
                            result = fetch_and_process_data(
                                st.session_state["selected_project"],
                                st.session_state["schema"],
                            )

                        update_progress(4, 12, "Data processing completed...", start_time)
                        update_progress(5, 12, "Updating cache...", start_time)

                        if "cache_key" not in st.session_state:
                            st.session_state["cache_key"] = 0
                        st.session_state["cache_key"] += 1

                        update_progress(6, 12, "Loading processed data...", start_time)
                        dataframes = fetch_dataframes_from_snowflake(st.session_state["cache_key"])
                        update_progress(7, 12, "Data loaded successfully...", start_time)

                        update_progress(8, 12, "Updating weekday dataframes...", start_time)
                        wkday_df = dataframes.get('wkday_df', pd.DataFrame())
                        wkday_dir_df = dataframes.get('wkday_dir_df', pd.DataFrame())
                        wkday_time_df = dataframes.get('wkday_time_df', pd.DataFrame())
                        wkday_raw_df = dataframes.get('wkday_raw_df', pd.DataFrame())
                        wkday_stationwise_df = dataframes.get('wkday_stationwise_df', pd.DataFrame())

                        update_progress(9, 12, "Updating weekend dataframes...", start_time)
                        wkend_df = dataframes.get('wkend_df', pd.DataFrame())
                        wkend_dir_df = dataframes.get('wkend_dir_df', pd.DataFrame())
                        wkend_time_df = dataframes.get('wkend_time_df', pd.DataFrame())
                        wkend_raw_df = dataframes.get('wkend_raw_df', pd.DataFrame())
                        wkend_stationwise_df = dataframes.get('wkend_stationwise_df', pd.DataFrame())

                        update_progress(10, 12, "Updating detail dataframes...", start_time)
                        detail_df = dataframes.get('detail_df', pd.DataFrame())
                        surveyor_report_trends_df = dataframes.get('surveyor_report_trends_df', pd.DataFrame())
                        route_report_trends_df = dataframes.get('route_report_trends_df', pd.DataFrame())
                        surveyor_report_date_trends_df = dataframes.get('surveyor_report_date_trends_df', pd.DataFrame())
                        route_report_date_trends_df = dataframes.get('route_report_date_trends_df', pd.DataFrame())

                        update_progress(11, 12, "Updating analysis dataframes...", start_time)
                        low_response_questions_df = dataframes.get('low_response_questions_df', pd.DataFrame())
                        refusal_analysis_df = dataframes.get('refusal_analysis_df', pd.DataFrame())
                        refusal_race_df = dataframes.get('refusal_race_df', pd.DataFrame())
                        by_interv_totals_df = dataframes.get('by_interv_totals_df', pd.DataFrame())
                        by_route_totals_df = dataframes.get('by_route_totals_df', pd.DataFrame())
                        survey_detail_totals_df = dataframes.get('survey_detail_totals_df', pd.DataFrame())
                        route_comparison_df = dataframes.get('route_comparison_df', pd.DataFrame())
                        reverse_routes_df = dataframes.get('reverse_routes_df', pd.DataFrame())
                        reverse_routes_difference_df = dataframes.get('reverse_routes_difference_df', pd.DataFrame())

                        update_progress(12, 12, "Finalizing sync...", start_time)

                        progress_bar.empty()
                        status_text.empty()
                        time_elapsed.empty()
                        keep_alive_placeholder.empty()

                        total_time = time.time() - start_time
                        
                        # Clear status messages
                        status_placeholder.empty()
                        warning_placeholder.empty()
                        
                        # Show appropriate success message
                        if selected_project == "LACMTA_FEEDER" and selected_agency and selected_agency != "All":
                            st.success(
                                f"✅ Data synced successfully to {selected_agency} schema in {total_time:.1f} seconds! "
                                f"Schema: LACMTA_FEEDER_{selected_agency} 📂"
                            )
                        else:
                            st.success(
                                f"✅ Data synced successfully in {total_time:.1f} seconds! "
                                "Pipelines are tidy, tables are aligned, and we're good to go! 📂"
                            )

                    except Exception as e:
                        progress_bar.empty()
                        status_text.empty()
                        time_elapsed.empty()
                        keep_alive_placeholder.empty()
                        # Clear status messages
                        status_placeholder.empty()
                        warning_placeholder.empty()
                        
                        st.error(f"❌ Sync failed: {str(e)}")
                        st.info("Please try again or contact support if the issue persists.")
                        # Mark sync as completed (even on error) so UI can reset
                        st.session_state.sync_running = False
                        st.session_state.sync_completed = True
                        # Small delay to show error message, then rerun to refresh UI
                        time.sleep(2)
                        st.rerun()
                    else:
                        # Mark sync as completed successfully BEFORE rerun
                        st.session_state.sync_running = False
                        st.session_state.sync_completed = True
                        # Cache key was already incremented during sync (line 4430), so data will be fresh
                        # Small delay to show success message, then rerun to refresh UI and fetch fresh data
                        time.sleep(1.5)
                        # Rerun will clear status messages and fetch fresh data from Snowflake
                        st.rerun()

            
            
            
            # Button to trigger the entire script
            # if role.upper() != "CLIENT":
            #     if st.button("Sync"):
            #         import gc
            #         import time
                    
            #         # ===== PHASE 1: MEMORY CLEANUP BEFORE STARTING =====
            #         gc.collect()
                    
            #         # Delete large dataframes to free memory
            #         large_vars = ['wkday_raw_df', 'wkend_raw_df', 'wkday_df', 'wkend_df', 
            #                     'wkday_dir_df', 'wkend_dir_df', 'detail_df', 'wkday_stationwise_df',
            #                     'wkend_stationwise_df', 'dataframes']
            #         for var_name in large_vars:
            #             if var_name in globals():
            #                 del globals()[var_name]
            #         gc.collect()
                    
            #         # ===== PHASE 2: MINIMAL PROGRESS INDICATORS =====
            #         keep_alive_placeholder = st.empty()
            #         progress_bar = st.progress(0)
            #         status_text = st.empty()
                    
            #         def update_progress(step, total_steps, message):
            #             progress = step / total_steps
            #             progress_bar.progress(progress)
            #             status_text.text(f"🔄 {message}")
                        
            #             # CRITICAL: Frequent session keep-alive
            #             keep_alive_placeholder.text(f"Step {step}/{total_steps}: {message}")
                        
            #             # Memory cleanup every step
            #             gc.collect()
                        
            #             # Keep session alive with small delay
            #             time.sleep(0.5)
                    
            #         try:
            #             # ===== PHASE 3: EXECUTE WITH MEMORY MANAGEMENT =====
            #             update_progress(1, 5, "Starting sync process...")
                        
            #             update_progress(2, 5, "Processing data (this may take 3-4 minutes)...")
            #             result = fetch_and_process_data(
            #                 st.session_state["selected_project"], 
            #                 st.session_state["schema"]
            #             )
                        
            #             update_progress(3, 5, "Data processed, updating cache...")
                        
            #             # Update cache key
            #             if "cache_key" not in st.session_state:
            #                 st.session_state["cache_key"] = 0
            #             st.session_state["cache_key"] += 1
                        
            #             # Clear memory before loading new data
            #             gc.collect()
                        
            #             update_progress(4, 5, "Loading essential data from Snowflake...")
                        
            #             # Load only essential dataframes
            #             dataframes = fetch_dataframes_from_snowflake(st.session_state["cache_key"])
                        
            #             # Update only critical dataframes needed for current view
            #             essential_df_mapping = {
            #                 'wkday_df': 'wkday_df',
            #                 'wkday_dir_df': 'wkday_dir_df', 
            #                 'wkday_time_df': 'wkday_time_df',
            #                 'wkend_df': 'wkend_df',
            #                 'wkend_dir_df': 'wkend_dir_df',
            #                 'wkend_time_df': 'wkend_time_df',
            #                 'detail_df': 'detail_df'
            #             }
                        
            #             for df_key, global_var in essential_df_mapping.items():
            #                 if df_key in dataframes:
            #                     globals()[global_var] = dataframes[df_key]
                        
            #             update_progress(5, 5, "Finalizing...")
                        
            #             # ===== PHASE 4: CLEANUP AND SUCCESS =====
            #             progress_bar.empty()
            #             status_text.empty()
            #             keep_alive_placeholder.empty()
                        
            #             st.success("✅ Data synced successfully!")
                        
            #             # Small delay then rerun
            #             time.sleep(2)
            #             st.rerun()
                        
            #         except Exception as e:
            #             # Cleanup on error
            #             progress_bar.empty()
            #             status_text.empty()
            #             keep_alive_placeholder.empty()
            #             gc.collect()
                        
            #             st.error(f"❌ Sync failed: {str(e)}")




            # if "sync_in_progress" not in st.session_state:
            #     st.session_state["sync_in_progress"] = False

            # if role.upper() != "CLIENT":
            #     # col1, col2, col3 = st.columns([1, 1, 6])
            #     # with col1:
            #     clicked = st.button(
            #         "Sync",
            #         disabled=st.session_state["sync_in_progress"]
            #     )
                






            #     # if show_metrics:
            #     #     st.markdown("---")
            #     #     st.subheader("📊 Database Records Overview")
            #     #     show_database_metrics_popup()

            #     if st.session_state["sync_in_progress"]:
            #         st.caption("🔄 Sync is running. Please wait…")

            #     if clicked and not st.session_state["sync_in_progress"]:
            #         st.session_state["sync_in_progress"] = True
            #         st.session_state["run_sync"] = True
            #         st.rerun()

            # if st.session_state.get("run_sync", False):

            #     import gc
            #     import time

            #     try:
            #         gc.collect()

            #         fetch_and_process_data(
            #             st.session_state["selected_project"],
            #             st.session_state["schema"]
            #         )

            #         st.session_state["cache_key"] = st.session_state.get("cache_key", 0) + 1

            #         dataframes = fetch_dataframes_from_snowflake(
            #             st.session_state["cache_key"]
            #         )

            #         essential_df_mapping = {
            #             'wkday_df': 'wkday_df',
            #             'wkday_dir_df': 'wkday_dir_df',
            #             'wkday_time_df': 'wkday_time_df',
            #             'wkend_df': 'wkend_df',
            #             'wkend_dir_df': 'wkend_dir_df',
            #             'wkend_time_df': 'wkend_time_df',
            #             'detail_df': 'detail_df'
            #         }

            #         for df_key, global_var in essential_df_mapping.items():
            #             if df_key in dataframes:
            #                 globals()[global_var] = dataframes[df_key]

            #         st.success("✅ Data synced successfully!")

            #     except Exception as e:
            #         st.error(f"❌ Sync failed: {str(e)}")

            #     finally:
            #         # Unlock
            #         st.session_state["sync_in_progress"] = False
            #         st.session_state["run_sync"] = False

            #         # Trigger ONE clean rerun
            #         st.session_state["sync_completed"] = True


            # if st.session_state.get("sync_completed", False):
            #     # Clear flag BEFORE rerun to avoid loop
            #     st.session_state["sync_completed"] = False
            #     st.rerun()




        # Button Section
        with header_col2:
            if role.upper() != "CLIENT":
                if st.button("Export Elvis Data"):
                    export_elvis_data()

        with header_col3:
            if role.upper() != "CLIENT":
                if current_page == "weekend":
                    csv_weekend_raw, week_end_raw_file_name = create_csv(wkend_raw_df, "wkend_raw_data.csv")
                    st.download_button(
                        "Download Weekend Raw Data",
                        data=csv_weekend_raw,
                        file_name=week_end_raw_file_name,
                        mime="text/csv",
                        key="download_weekend"
                    )
                else:
                    csv_weekday_raw, week_day_raw_file_name = create_csv(wkday_raw_df, "wkday_raw_data.csv")
                    st.download_button(
                        "Download Weekday Raw Data",
                        data=csv_weekday_raw,
                        file_name=week_day_raw_file_name,
                        mime="text/csv",
                        key="download_weekday"
                    )

        st.markdown('</div>', unsafe_allow_html=True)


        # === End of Unified Button Row ===
        
        # ---- Validation Section Box ----
        with st.container(key="validation_section"):
            st.html("""
            <style>
            div.st-key-toggle_right {
                display: flex;
                justify-content: flex-end;
            }
            </style>
            """)

            with st.container(border=True):
                bar_left, bar_right = st.columns([10, 2], vertical_alignment="center")

                with bar_left:
                    st.markdown(
                        "### Dashboard Validation "
                        "<span style='font-size:0.85rem; color:#6b7280;'>"
                        "— enable to view DB metrics"
                        "</span>",
                        unsafe_allow_html=True,
                    )

                with bar_right:
                    with st.container(key="toggle_right"):
                        show_metrics = st.toggle("Enable", value=False, label_visibility="collapsed")

                if show_metrics:
                    show_database_metrics_popup()





            
        if 'rail' in selected_schema.lower():
        
            if current_page == "weekday":
                weekday_page()

            elif current_page == "weekend":
                weekend_page()
            elif current_page=='timedetails':
                time_details(detail_df)
            elif current_page=='weekday_station':
                weekday_station_page()
            elif current_page=='weekend_station':
                weekend_station_page()
            elif current_page == "location_maps":  # Add this line
                location_maps_page()
            else:
                # wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                #                         '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                if 'uta' in selected_project:
                    wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED','STATION_ID',  '(0) Collect', '(0) Remain','(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                                            '(0) Goal','(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal','(5) Goal']
                    wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4','5']
                elif 'tucson' in selected_project:
                    wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                            '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                    wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']
                
                elif 'stl' in selected_project:
                    wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                            '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                    wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']

                elif 'kcata' in selected_project:
                    wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                            '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                    wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']

                else:
                    wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(0) Collect', '(0) Remain','(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                                            '(0) Goal','(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal','(5) Goal']
                    wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4','5']
            


                main_page(wkday_dir_df[wkday_dir_columns],
                                wkday_time_df[wkday_time_columns],
                                wkday_df[['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', 'Route Level Goal', '# of Surveys', 'Remaining']])
        else:
            if current_page == "weekday":
                weekday_page()
            elif current_page == "weekend":
                weekend_page()
            elif current_page=='timedetails':
                time_details(detail_df)
            elif current_page == "dailytotals":
                if 'stl' in selected_project or 'kcata' in selected_project or 'actransit' in selected_project or 'salem' in selected_project or 'lacmta_feeder' in selected_project:
                    daily_totals_page()
            elif current_page == "low_response_questions_tab":
                if 'actransit' in selected_project or 'salem' in selected_project or 'lacmta_feeder' in selected_project:  # Add this new route
                    low_response_questions_page()
            elif current_page == "refusal":  # ADD THIS NEW PAGE FOR REFUSAL ANALYSIS
                if 'actransit' in selected_project or 'salem' in selected_project or 'lacmta_feeder' in selected_project:
                    show_refusal_analysis(refusal_analysis_df, refusal_race_df)
            elif current_page == "surveyreport":
                if any(x in selected_project for x in ['stl', 'kcata', 'actransit', 'salem', 'lacmta_feeder']):

                    surveyor_last_row = surveyor_report_trends_df.iloc[-1].to_dict()
                    route_last_row = route_report_trends_df.iloc[-1].to_dict()

                    filter_col = "Date_Surveyor" if 'stl' in selected_project else "Date"

                    # ==========================
                    # CREATE TABS
                    # ==========================
                    tab_surveyor, tab_route = st.tabs(["Surveyor Report", "Route Report"])

                    # ==========================
                    # SURVEYOR TAB
                    # ==========================
                    with tab_surveyor:
                        render_metrics(
                            surveyor_last_row,
                            "TRIP LOGIC & QAQC REPORT – SURVEYOR REPORT"
                        )

                        display_filtered_or_unfiltered_report(
                            unfiltered_df=dataframes['surveyor_report_trends_df'],
                            filtered_df=dataframes['surveyor_report_date_trends_df'],
                            filter_column_name=filter_col,
                            display_column_name="INTERV_INIT",
                            section_title="Surveyor Report",
                            date_label="Surveyor"
                        )

                    # ==========================
                    # ROUTE TAB
                    # ==========================
                    with tab_route:
                        render_metrics(
                            route_last_row,
                            "TRIP LOGIC & QAQC REPORT – ROUTE REPORT"
                        )

                        display_filtered_or_unfiltered_report(
                            unfiltered_df=dataframes['route_report_trends_df'],
                            filtered_df=dataframes['route_report_date_trends_df'],
                            filter_column_name="Date_Route",
                            display_column_name="ROUTE",
                            section_title="Route Report",
                            date_label="Route"
                        )

            elif current_page == "route_comparison":
                if 'kcata' in selected_project or 'actransit' in selected_project or 'salem' in selected_project or 'lacmta_feeder' in selected_project and 'rail' not in selected_schema.lower():
                    route_comparison_page()
            elif current_page == "reverse_routes":
                if 'kcata' in selected_project or 'actransit' in selected_project or 'salem' in selected_project or 'lacmta_feeder' in selected_project and 'rail' not in selected_schema.lower():
                    reverse_routes_page()
            elif current_page == "location_maps":  # Add this line
                location_maps_page()
            elif current_page == "demographic":
                if 'actransit' in selected_project or 'salem' in selected_project or 'lacmta_feeder' in selected_project:
                   demographic_review_page(demographic_review_df)
            elif current_page == "accounts_management":
                accounts_management_page()
            elif current_page == "create_accounts":
                create_accounts_page()
            elif current_page == "password_update":
                password_update_page()

            else:
                if 'tucson' in selected_project:
                    wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                            '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                    wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']
                elif 'lacmta_feeder' in selected_project:
                    wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                            '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                    wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']

                elif 'kcata' in selected_project or 'actransit' in selected_project or 'salem' in selected_project:
                    wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(5) Collect', '(5) Remain',
                                            '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']
                    wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4', '5']

                else:
                    wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(0) Collect', '(0) Remain','(1) Collect', '(1) Remain',
                                            '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                                            '(0) Goal','(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal','(5) Goal']
                    wkday_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4','5']
                
                try:
                    main_page(wkday_dir_df[wkday_dir_columns],
                            wkday_time_df[wkday_time_columns],
                            wkday_df[['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', 'Route Level Goal', '# of Surveys', 'Remaining']])
                    
                except KeyError as e:
                    st.error(f"⚠️ Missing columns in data: {e}")
                    st.error("Available columns in weekday direction data:")
                    st.write(wkday_dir_df.columns.tolist())
                    st.stop()  # Prevent further execution