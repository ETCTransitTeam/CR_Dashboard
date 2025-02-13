import streamlit as st
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import pd_writer, write_pandas
from decouple import config
import datetime
import numpy as np
import subprocess
from st_aggrid import AgGrid, JsCode
from st_aggrid import AgGrid, ColumnsAutoSizeMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from automated_refresh_flow_new import fetch_and_process_data

# Set page config
st.set_page_config(page_title="Completion REPORT DashBoard", layout='wide')

# Function to create Snowflake connection
def create_snowflake_connection():
    conn = snowflake.connector.connect(
        user=config('user'),
        password=config('password'),
        account=config('account'),
        warehouse=config('warehouse'),
        database=config('database'),
        schema='tucson_bus',
        role=config('role')
    )
    return conn

# Function to style dataframe
def style_dataframe(df, column_name_patterns):
    target_columns = [col for col in df.columns if any(pattern in col for pattern in column_name_patterns)]
    
    def highlight_cell(val):
        if -10000 <= val < 1:
            return "background-color: #BCE29E; color: black;"
        elif 1 <= val < 6:
            return "background-color: #E5EBB2; color: black;"
        elif 6 <= val < 35:
            return "background-color: #F8C4B4; color: black;"
        elif 35 <= val < 10000:
            return "background-color: #FF8787; color: black;"
        return ""

    styled_df = df.style.map(highlight_cell, subset=target_columns)
    return styled_df

pinned_column='ROUTE_SURVEYEDCode'
column_name_patterns=['(0) Remain', '(1) Remain', '(2) Remain', 
       '(3) Remain', '(4) Remain', '(5) Remain' ,'Remaining']

# Function to fetch dataframes from Snowflake
@st.cache
def fetch_dataframes_from_snowflake():
    conn = create_snowflake_connection()
    cur = conn.cursor()

    table_to_df_mapping = {
        'wkday_comparison': 'wkday_df',
        'wkday_dir_comparison': 'wkday_dir_df',
        'wkend_comparison': 'wkend_df',
        'wkend_dir_comparison': 'wkend_dir_df',
        'wkend_time_data': 'wkend_time_df',
        'wkday_time_data': 'wkday_time_df',
        'wkend_raw': 'wkend_raw_df',
        'wkday_raw': 'wkday_raw_df',
        'TOD': 'detail_df'
    }

    dataframes = {}

    try:
        for table_name, df_name in table_to_df_mapping.items():
            query = f"SELECT * FROM {table_name}"
            cur.execute(query)
            data = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            df = pd.DataFrame(data, columns=columns)
            dataframes[df_name] = df
            print(f"Data fetched and stored in DataFrame: {df_name}")
    except Exception as e:
        print(f"Error fetching data: {e}")
    finally:
        cur.close()
        conn.close()

    return dataframes

# Function to create CSV
def create_csv(df, file_name):
    file_name = f"{file_name}.csv"
    csv = df.to_csv(index=False)
    return csv, file_name

# Function to download CSV
def download_csv(csv, file_name, label):
    with st.empty():
        st.download_button(
            label=label,
            data=csv,
            file_name=file_name,
            mime="text/csv"
        )

# Function to filter dataframe
def filter_dataframe(df, query):
    if query:
        df = df[df.apply(lambda row: row.astype(str).str.contains(query, case=False).any(), axis=1)]
    return df

# Function to render AgGrid
def render_aggrid(dataframe, height, pinned_column, key):
    cellStyle = JsCode("""
        function(params) {
            const val = params.value;
            if (val >= -10000 && val < 1) {
                return {'background-color': '#BCE29E', 'color': 'black'};
            } else if (val >= 1 && val < 6) {
                return {'background-color': '#E5EBB2', 'color': 'black'};
            } else if (val >= 6 && val < 35) {
                return {'background-color': '#F8C4B4', 'color': 'black'};
            } else if (val >= 35 && val < 10000) {
                return {'background-color': '#FF8787', 'color': 'black'};
            }
            return null;
        }
    """)

    gb = GridOptionsBuilder.from_dataframe(dataframe)
    gb.configure_default_column(editable=False, groupable=False, autoSizeColumns=True)

    if pinned_column in dataframe.columns:
        gb.configure_column(pinned_column, pinned='left')
    else:
        print(f"Column '{pinned_column}' not found in the dataframe")

    valid_columns = [col for col in dataframe.columns if any(pattern in col for pattern in column_name_patterns)]
    for column in valid_columns:
        gb.configure_column(column, cellStyle=cellStyle)

    other_options = {'suppressColumnVirtualisation': True}
    gb.configure_grid_options(
        alwaysShowHorizontalScroll=True,
        enableRangeSelection=True,
        pagination=True,
        paginationPageSize=10000,
        domLayout='normal',
        **other_options
    )

    grid_options = gb.build()
    grid_options["autoSizeAllColumns"] = True

    AgGrid(
        dataframe,
        gridOptions=grid_options,
        height=height,
        theme="streamlit",
        allow_unsafe_jscode=True,
        key=f'grid_{key}',
        suppressHorizontalScroll=False,
        fit_columns_on_grid_load=False,
        columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
        reload_data=True,
        width='100%',
    )

# Function to handle login
def login():
    st.title("Login Page")
    email = st.text_input("Email")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        if email == config('USER_EMAIL') and password ==config('USER_PASSWORD'):
            st.session_state["logged_in"] = True
            st.experimental_rerun()
        else:
            st.error("Incorrect email or password")

# Main function to run the app
def main():
    if "logged_in" not in st.session_state:
        st.session_state["logged_in"] = False

    if not st.session_state["logged_in"]:
        login()
    else:
        # Fetch dataframes from Snowflake
        dataframes = fetch_dataframes_from_snowflake()

        # Access DataFrames
        wkday_df = dataframes['wkday_df']
        wkday_dir_df = dataframes['wkday_dir_df']
        wkend_df = dataframes['wkend_df']
        wkend_dir_df = dataframes['wkend_dir_df']
        wkend_time_df = dataframes['wkend_time_df']
        wkday_time_df = dataframes['wkday_time_df']
        wkend_raw_df = dataframes['wkend_raw_df']
        wkday_raw_df = dataframes['wkday_raw_df']
        detail_df = dataframes['detail_df']

        # Main dashboard logic
        st.sidebar.header("Filters")
        search_query = st.sidebar.text_input(label='Search', placeholder='Search')

        query_params = st.experimental_get_query_params()
        page = query_params.get("page", ["main"])[0]

        def update_query_params_and_rerun(new_page):
            if page != new_page:
                st.experimental_set_query_params(page=new_page)
                st.experimental_rerun()

        header_col1, header_col2, header_col3 = st.columns([2, 2, 1])

        with header_col1:
            st.header('Completion Report')
            if st.button("Sync"):
                with st.spinner("Syncing... Please wait"):
                    result = fetch_and_process_data()
                st.success(f"Data Synced Successfully!")
            current_date = datetime.datetime.now()
            formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
            st.markdown(f"##### **Last Refresh DATE**: {formatted_date}")

            completed_dates = pd.concat([wkday_raw_df['Completed'], wkend_raw_df['Completed']])
            most_recent_completed_date = pd.to_datetime(completed_dates).max()
            st.markdown(f"##### **Completed**: {most_recent_completed_date.strftime('%Y-%m-%d %H:%M:%S')}")

        with header_col2:
            if page != 'timedetails':
                if page == "weekend":
                    st.header(f'Total Records: {wkend_df["# of Surveys"].sum()}')
                    csv_weekend_raw, week_end_raw_file_name = create_csv(wkend_raw_df, "wkend_raw_data.csv")
                    download_csv(csv_weekend_raw, week_end_raw_file_name, "Download WeekEnd Raw Data")
                else:
                    st.header(f'Total Records: {wkday_df["# of Surveys"].sum()}')
                    csv_weekday_raw, week_day_raw_file_name = create_csv(wkday_raw_df, "wkday_raw_data.csv")
                    download_csv(csv_weekday_raw, week_day_raw_file_name, "Download WeekDay Raw Data")

                if st.button('Time OF Day Details'):
                    st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=timedetails">', unsafe_allow_html=True)
            else:
                st.header(f'Time OF Day Details')

        with header_col3:
            if st.button("WEEKDAY-OVERALL"):
                st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=weekday">', unsafe_allow_html=True)

            if st.button("WEEKEND-OVERALL"):
                st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=weekend">', unsafe_allow_html=True)

        def time_details(details_df):
            st.dataframe(details_df, height=670, use_container_width=True)
            if st.button("GO TO HOME"):
                st.experimental_set_query_params(page="main")
                st.experimental_rerun()

        def main_page(data1, data2, data3):
            col1, col2 = st.columns([2, 1])

            with col1:
                if page == 'main':
                    st.subheader('Route Direction Level Comparison (WeekDAY)')
                else:
                    st.subheader("Route Direction Level Comparison")
                filtered_df1 = filter_dataframe(data1, search_query)
                render_aggrid(filtered_df1, 500, 'ROUTE_SURVEYEDCode', 1)
                csv1, file_name1 = create_csv(filtered_df1, "route_direction_comparison.csv")
                download_csv(csv1, file_name1, "Download Route Direction Comparison Data")

                filtered_df3 = filter_dataframe(data3, search_query)
                st.subheader("Route Level Comparison")
                render_aggrid(filtered_df3, 400, 'ROUTE_SURVEYEDCode', 2)
                csv3, file_name3 = create_csv(filtered_df3, "route_level_comparison.csv")
                download_csv(csv3, file_name3, "Download Route Level Comparison Data")

            with col2:
                st.subheader("Time Range Data")
                data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']] = data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']].apply(pd.to_numeric, errors='coerce')
                data2[['1', '2', '3', '4']] = data2[['1', '2', '3', '4']].apply(pd.to_numeric, errors='coerce')
                data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']] = data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']].fillna(0)
                data2[['1', '2', '3', '4']] = data2[['1', '2', '3', '4']].fillna(0)
                expected_totals = data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']].sum()
                collected_totals = data2[['1', '2', '3', '4']].sum()
                difference = np.maximum(expected_totals.values - collected_totals.values, 0)
                result_df = pd.DataFrame({
                    'Time Period': ['1', '2', '3', '4'],
                    'Collected Totals': collected_totals.values.astype(int),
                    'Expected Totals': expected_totals.values.astype(int),
                    'Remaining': difference.astype(int),
                })

                filtered_df2 = filter_dataframe(data2, search_query)
                render_aggrid(filtered_df2, 500, 'Display_Text', 3)
                csv2, file_name2 = create_csv(filtered_df2, "time_range_data.csv")
                download_csv(csv2, file_name2, "Download Time Range Data")

                filtered_df4 = filter_dataframe(result_df, search_query)
                st.subheader("Time Period OverAll Data")
                render_aggrid(filtered_df4, 400, 'Time Period', 4)
                csv4, file_name4 = create_csv(filtered_df4, "time_period_overall_data.csv")
                download_csv(csv4, file_name4, "Download Time Period Overall Data")

        def weekday_page():
            st.title("Weekday OverAll Data")
            wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                 '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
            main_page(wkday_dir_df[wkday_dir_columns],
                      wkday_time_df[['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']],
                      wkday_df[['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', 'Route Level Goal', '# of Surveys', 'Remaining']])
            if st.button("GO TO HOME"):
                st.experimental_set_query_params(page="main")
                st.experimental_rerun()

        def weekend_page():
            st.title("Weekend OverAll Data")
            wkend_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                 '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                 '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
            main_page(wkend_dir_df[wkend_dir_columns],
                      wkend_time_df[['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']],
                      wkend_df[['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', 'Route Level Goal', '# of Surveys', 'Remaining']])
            if st.button("GO TO HOME"):
                st.experimental_set_query_params(page="main")
                st.experimental_rerun()

        if page == "weekday":
            weekday_page()
        elif page == "weekend":
            weekend_page()
        elif page == 'timedetails':
            time_details(detail_df)
        else:
            wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                 '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
            main_page(wkday_dir_df[wkday_dir_columns],
                      wkday_time_df[['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']],
                      wkday_df[['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', 'Route Level Goal', '# of Surveys', 'Remaining']])

# Run the main function
if __name__ == "__main__":
    main()