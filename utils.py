import boto3
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, ColumnsAutoSizeMode,JsCode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from streamlit.runtime.scriptrunner import get_script_run_ctx


# Columns to apply the coloring
column_name_patterns = ['(0) Remain', '(1) Remain', '(2) Remain', 
                        '(3) Remain', '(4) Remain', '(5) Remain', 'Remaining']

# JS code for conditional formatting
cellStyle = JsCode("""
    function(params) {
        const val = params.value;
        if (val === null || val === undefined || val === "") return null;
        if (val >= -10000 && val < 1) return {'background-color': '#BCE29E', 'color': 'black'};
        if (val >= 1 && val < 6) return {'background-color': '#E5EBB2', 'color': 'black'};
        if (val >= 6 && val < 35) return {'background-color': '#F8C4B4', 'color': 'black'};
        if (val >= 35 && val < 10000) return {'background-color': '#FF8787', 'color': 'black'};
        return null;
    }
""")

def render_aggrid(dataframe, height=400, pinned_column=None, key="grid"):
    gb = GridOptionsBuilder.from_dataframe(dataframe)
    gb.configure_default_column(editable=False, groupable=False, autoSizeColumns=True)
    
    if pinned_column and pinned_column in dataframe.columns:
        gb.configure_column(pinned_column, pinned='left')
    
    # Apply conditional formatting to matching columns
    for col in dataframe.columns:
        if any(pattern in col for pattern in column_name_patterns):
            gb.configure_column(col, cellStyle=cellStyle)
    
    # Grid options
    gb.configure_grid_options(
        alwaysShowHorizontalScroll=True,
        enableRangeSelection=True,
        pagination=True,
        paginationPageSize=10000,
        domLayout='normal',
        suppressColumnVirtualisation=True
    )
    
    grid_options = gb.build()
    grid_options["autoSizeAllColumns"] = True
    
    AgGrid(
        dataframe,
        gridOptions=grid_options,
        height=height,
        theme="streamlit",
        allow_unsafe_jscode=True,
        key=f"{key}",
        suppressHorizontalScroll=False,
        fit_columns_on_grid_load=False,
        columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
        reload_data=True,
        width='100%'
    )

def apply_optimized_styling(df):
    """Apply optimized conditional formatting without changing data types"""
    column_name_patterns = ['(0) Remain', '(1) Remain', '(2) Remain', 
                           '(3) Remain', '(4) Remain', '(5) Remain', 'Remaining']
    
    target_columns = [col for col in df.columns if any(pattern in col for pattern in column_name_patterns)]
    
    if not target_columns:
        return df.style

    def color_conditions(val):
        if pd.isna(val):
            return ''
        try:
            num_val = float(val)
            if -10000 <= num_val < 1:
                return 'background-color: #BCE29E; color: black'
            elif 1 <= num_val < 6:
                return 'background-color: #E5EBB2; color: black'
            elif 6 <= num_val < 35:
                return 'background-color: #F8C4B4; color: black'
            elif 35 <= num_val < 10000:
                return 'background-color: #FF8787; color: black'
        except (ValueError, TypeError):
            pass
        return ''

    styled_df = df.style

    # Force integer formatting for all numeric columns
    int_cols = df.select_dtypes(include=["int", "float"]).columns
    styled_df = styled_df.format({col: "{:.0f}" for col in int_cols})

    # Apply coloring only to target columns
    for col in target_columns:
        if col in df.columns:
            styled_df = styled_df.map(color_conditions, subset=[col])

    return styled_df

def render_styled_dataframe(dataframe, height=400, pinned_column=None, key="grid"):
    """Render dataframe with conditional formatting"""
    styled_df = apply_optimized_styling(dataframe)
    
    st.dataframe(
        styled_df,
        use_container_width=True,
        hide_index=True,
        height=height
    )

def create_csv(df, file_name):
    """
    Convert the DataFrame to CSV and return it for downloading.
    Append (weekend) or (weekday) to the file name based on page_type.
    """
    # Add page type to the file name
    # suffix = "(weekend)" if page_type == "weekend" else "(weekday)"
    file_name = f"{file_name}.csv"
    
    # Convert DataFrame to CSV
    csv = df.to_csv(index=False)
    return csv, file_name

def download_csv(csv, file_name, label):
    """
    Create a Streamlit download button for a given CSV.
    """
    with st.empty():  # Using st.container() to wrap the button
        st.download_button(
            label=label,
            data=csv,
            file_name=file_name,
            mime="text/csv",
            type="primary",  # Makes the button more prominent
            icon=":material/download:",  # Adds a download icon
        )



def update_query_params(new_page):
    ctx = get_script_run_ctx()
    if ctx:
        st.query_params["page"] = new_page
        st.experimental_rerun()

from database import DatabaseConnector
import io
import os
import boto3
from dotenv import load_dotenv
load_dotenv()

# Function to fetch data from the database
def fetch_data(database_name, table_name):
    HOST = os.getenv("SQL_HOST")
    USER = os.getenv("SQL_USER")
    PASSWORD = os.getenv("SQL_PASSWORD")
    
    db_connector = DatabaseConnector(HOST, database_name, USER, PASSWORD)

    try:
        db_connector.connect()
        print("Connection successful.....")

        if db_connector.connection is None:
            st.error("Database connection failed. Check credentials and server availability.")
            st.query_params["logged_in"] = "true"
            st.query_params["page"] = "main"
            st.rerun()  # Refresh the page after login
            return None
        
        connection = db_connector.connection  # Get MySQL connection object
        print(f"{table_name=} and {database_name=}")
        
        select_query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(select_query, connection)  # Load data into DataFrame
        db_connector.disconnect()  # Close database connection

        # Store DataFrame in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        return csv_buffer  

    # except mysql.connector.Error as sql_err:
    #     st.error(f"Database error: {sql_err}")
        
    except Exception as e:
        st.error(f"Error fetching data: {e}")
    
    finally:
        try:
            db_connector.disconnect()
        except Exception:
            pass  # Ensure it doesn't crash on failed disconnect

    # Redirect back to the main page without disturbing the whole process
    st.query_params["logged_in"] = "true"
    st.query_params["page"] = "main"
    st.rerun()  # Refresh the page after login
    return None


# -----------------------
# Initialize S3 client for file reading
# -----------------------
bucket_name = os.getenv('bucket_name')
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('aws_access_key_id'),
    aws_secret_access_key=os.getenv('aws_secret_access_key')
)

# Function to read an Excel file from S3 into a DataFrame
def read_excel_from_s3(bucket_name, file_key, sheet_name):
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    excel_data = response['Body'].read()
    return pd.read_excel(io.BytesIO(excel_data), sheet_name=sheet_name)

# -----------------------
# AGENCY FILTERING FOR LACMTA_FEEDER (Better Approach)
# -----------------------
def apply_lacmta_agency_filter(
    df,
    project,
    agency,
    bucket_name,
    project_config,
    baby_elvis_df=None
):
    """
    Applies LACMTA_FEEDER agency-based route filtering on df (and baby_elvis_df if provided)

    Returns:
        df, baby_elvis_df
    """
    if project != "LACMTA_FEEDER" or not agency or agency == "All":
        return df, baby_elvis_df

    print(f"Applying agency filter for: {agency}")

    # Read details file to get agency-route mapping
    detail_df_stops = read_excel_from_s3(
        bucket_name,
        project_config["files"]["details"],
        "STOPS"
    )

    if detail_df_stops is None or detail_df_stops.empty:
        print("Warning: Could not read details file for agency filtering")
        return df, baby_elvis_df

    # Find agency column (case-insensitive)
    agency_col_name = None
    for col in detail_df_stops.columns:
        if col.lower() == "agency":
            agency_col_name = col
            break

    if not agency_col_name:
        print("Warning: 'agency' column not found in stops sheet")
        return df, baby_elvis_df

    # Filter stops for selected agency
    agency_stops = detail_df_stops[detail_df_stops[agency_col_name] == agency]

    if agency_stops.empty:
        print(f"No stops found for agency: {agency}")
        return df, baby_elvis_df

    # Get unique routes
    agency_routes = agency_stops["ETC_ROUTE_ID"].dropna().unique()
    print("Agency Routes before processing:", agency_routes)

    # Extract base route codes
    agency_route_codes = []
    for route in agency_routes:
        route_str = str(route)
        route_parts = route_str.split("_")
        if len(route_parts) > 1:
            agency_route_codes.append("_".join(route_parts[:-1]))
        else:
            agency_route_codes.append(route_str)

    # -------------------------
    # Apply filter on df
    # -------------------------
    if df is not None and "ROUTE_SURVEYEDCode" in df.columns and agency_route_codes:
        before_count = len(df)

        df = df.copy()
        df["ROUTE_BASE"] = df["ROUTE_SURVEYEDCode"].apply(
            lambda x: "_".join(str(x).split("_")[:-1]) if pd.notna(x) else None
        )

        df = df[df["ROUTE_BASE"].isin(agency_route_codes)]
        df = df.drop(columns=["ROUTE_BASE"])

        after_count = len(df)
        print(f"Agency Filter ({agency}): {before_count} -> {after_count} records")

    # -------------------------
    # Apply filter on baby_elvis_df (optional)
    # -------------------------
    if baby_elvis_df is not None and "ROUTE_SURVEYEDCode" in baby_elvis_df.columns:
        baby_elvis_df = baby_elvis_df.copy()
        baby_elvis_df["ROUTE_BASE"] = baby_elvis_df["ROUTE_SURVEYEDCode"].apply(
            lambda x: "_".join(str(x).split("_")[:-1]) if pd.notna(x) else None
        )
        baby_elvis_df = baby_elvis_df[
            baby_elvis_df["ROUTE_BASE"].isin(agency_route_codes)
        ]
        baby_elvis_df = baby_elvis_df.drop(columns=["ROUTE_BASE"])

    return df, baby_elvis_df