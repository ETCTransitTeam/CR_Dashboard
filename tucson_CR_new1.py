import os
import bcrypt
import base64
import datetime
import subprocess
import numpy as np
import pandas as pd
import streamlit as st
import snowflake.connector
from decouple import config
from st_aggrid import AgGrid, ColumnsAutoSizeMode,JsCode
from streamlit.runtime.scriptrunner import get_script_run_ctx
from st_aggrid.grid_options_builder import GridOptionsBuilder
from automated_refresh_flow_new import fetch_and_process_data
from snowflake.connector.pandas_tools import pd_writer,write_pandas
from dotenv import load_dotenv


load_dotenv()


st.set_page_config(page_title="Completion REPORT DashBoard", layout='wide')

# Ensure session state exists
if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False

# Get existing query parameters
query_params = st.experimental_get_query_params()
# current_page = query_params.get("page", [""])[0]  # Get 'page' value if it exists
current_page = st.experimental_get_query_params().get("page", ["login"])[0]  # Default to "login" if not set


schema_value = {'TUCSON': 'tucson_bus','VTA': 'public', 'UTA': 'uta_rail'}


def user_connect_to_snowflake():
    print("hehe user = ",os.getenv('user'))
    return snowflake.connector.connect(
        user=os.getenv('user'),
        password=os.getenv('password'),
        account=os.getenv('account'),
        warehouse=os.getenv('warehouse'),
        database=os.getenv('database'),
        schema='user',
        role=os.getenv('role')
    )

# Function to register a new user in Snowflake
def register_new_user(email, username, password):
    # Connect to Snowflake
    conn = user_connect_to_snowflake()
    cursor = conn.cursor()

    # Check if user already exists
    check_user_query = """
    SELECT email FROM user.user_table WHERE email = %s
    """
    cursor.execute(check_user_query, (email,))
    existing_user = cursor.fetchone()

    if existing_user:
        print("User already exists!")
        st.error('User Already Exists')
        cursor.close()
        conn.close()
        return False  # User already exists

    # Hash the password using bcrypt
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
    
    # Encode the hashed password to Base64 before saving to Snowflake
    encoded_hashed_password = base64.b64encode(hashed_password).decode('utf-8')

    # Insert the new user into the table
    insert_query = """
    INSERT INTO user.user_table (email, username, password)
    VALUES (%s, %s, %s)
    """
    cursor.execute(insert_query, (email, username, encoded_hashed_password))

    # Commit the transaction and close connection
    conn.commit()
    cursor.close()
    conn.close()

    print("User registered successfully!")
    return True  # Registration successful

# Function to display the registration page and handle user registration
def register_page():
    """Displays a registration form and handles user registration."""
    st.title("Register Page")

    # Input fields for registration
    username = st.text_input("Username")
    email = st.text_input("Email")
    password1 = st.text_input("Password", type="password")
    password2 = st.text_input("Confirm Password", type="password")

    if st.button("Register"):
        # Validate inputs
        if not username or not email or not password1 or not password2:
            st.error("Please fill in all fields.")
        elif password1 != password2:
            st.error("Passwords do not match.")
        else:
            # Call the register_new_user function to register the user in Snowflake
            if register_new_user(email, username, password1):
                st.success("Registration successful! You can now log in.")
                
                # Redirect to login page
                st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=login">', unsafe_allow_html=True)

                # st.experimental_set_query_params(page="login")
                # query_params = st.experimental_get_query_params()
                # st.write("Updated Query Params:", query_params)

                # st.experimental_rerun()

    # Navigation buttons
    if st.button("Go to Login Page"):
        # st.experimental_set_query_params(page="login")
        # st.experimental_rerun()
        st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=login">', unsafe_allow_html=True)


# Check if user exists and password matches
def check_user_login(email, password):
    # Connect to Snowflake
    conn = user_connect_to_snowflake()
    cursor = conn.cursor()

    # Query to get the user based on the email
    query = """
    SELECT email, username, password
    FROM user.user_table
    WHERE email = %s
    """
    cursor.execute(query, (email,))
    user = cursor.fetchone()  # Fetch the user record
    
    # Check if user exists and password matches
    if user:
        # Decode the Base64-encoded hashed password from the database
        stored_hashed_password = base64.b64decode(user[2])  # user[2] is the password column
        
        if bcrypt.checkpw(password.encode('utf-8'), stored_hashed_password):
            return True  # Password matches
        else:
            return False  # Incorrect password
    else:
        return False  # User not found
    # Close connection
    cursor.close()
    conn.close()

# Streamlit login page
def login():
    """Displays a login form and handles authentication."""
    st.title("Login Page")
    
    # User inputs
    email = st.text_input("Email")
    password = st.text_input("Password", type="password")
    
    # Project selection
    project = st.selectbox("Select a Project", list(schema_value.keys()))

    # Handle login button click
    if st.button("Login"):
        if check_user_login(email, password):
            st.success("LogIn successful!")
            print(f'{project.lower()=}')
            # If login is successful
            st.session_state["logged_in"] = True
            st.session_state["selected_project"] = project  # Store selected project in session state
            st.session_state["schema"] = schema_value[project]
            print(f'{st.session_state["selected_project"]=}')
            print(f'{st.session_state["schema"]=}')

            # Preserve 'page' parameter in URL after login
            st.experimental_set_query_params(logged_in="true", page='main')  
            st.experimental_rerun()  # Refresh the page after login
            # st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=main">', unsafe_allow_html=True)

        else:
            # Display error if login fails
            st.error("Incorrect email or password")
    
    # Button to go to register page
    if st.button("Go to Register Page"):
        # st.experimental_set_query_params(page="register")
        # st.experimental_rerun()
        st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=register">', unsafe_allow_html=True)


if not st.session_state["logged_in"]:
    if current_page == "register":
        register_page()  # Show the register page if the query parameter is set to "register"
    elif current_page == "login":
        login()  # Show the login page by default
    else:
        st.write("Please log in first.")
    st.stop()
else:
    selected_schema = st.session_state.get("schema", None)
        
    def create_snowflake_connection():
        conn = snowflake.connector.connect(
            user=os.getenv('user'),
            password=os.getenv('password'),
            account=os.getenv('account'),
            warehouse=os.getenv('warehouse'),
            database=os.getenv('database'),
            schema=selected_schema,
            role=os.getenv('role')
        )
        return conn


    pinned_column='ROUTE_SURVEYEDCode'
    column_name_patterns=['(0) Remain', '(1) Remain', '(2) Remain', 
        '(3) Remain', '(4) Remain', '(5) Remain' ,'Remaining']

    @st.cache(allow_output_mutation=True)  # Use st.cache in Streamlit 1.6.0
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
            'TOD': 'detail_df'
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

    # Example: Access DataFrames
    wkday_df = dataframes['wkday_df']
    wkday_dir_df = dataframes['wkday_dir_df']
    wkend_df = dataframes['wkend_df']
    wkend_dir_df = dataframes['wkend_dir_df']
    wkend_time_df = dataframes['wkend_time_df']
    wkday_time_df = dataframes['wkday_time_df']
    wkend_raw_df = dataframes['wkend_raw_df']
    wkday_raw_df = dataframes['wkday_raw_df']
    detail_df = dataframes['detail_df']


    wkday_stationwise_df = dataframes.get('wkday_stationwise_df')
    wkend_stationwise_df = dataframes.get('wkend_stationwise_df')

    st.sidebar.header("Filters")
    search_query=st.sidebar.text_input(label='Search', placeholder='Search')
    if st.sidebar.button("Logout"):
        st.session_state["logged_in"] = False
        st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=login">', unsafe_allow_html=True)
        # st.experimental_set_query_params(logged_in="false")  # Update URL
        # st.experimental_rerun()  # Reload the page to show login again



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
                mime="text/csv"
            )


    # Retrieve current query parameters
    query_params = st.experimental_get_query_params()
    page = query_params.get("page", ["main"])[0]

    # Debugging: Display the current query parameters
    # st.write(f"Current query parameters: {query_params}")

    def update_query_params(new_page):
        ctx = get_script_run_ctx()
        if ctx:
            st.query_params["page"] = new_page
            st.experimental_rerun()

    def clear_cache():
        st.experimental_memo.clear()

    # Layout columns
    header_col1, header_col2, header_col3 = st.columns([2, 2, 1])

    # Header Section
    with header_col1:
        st.header('Completion Report')
        # Button to trigger the entire script
        if st.button("Sync"):
            with st.spinner("Syncing... Please wait...It will take 2 to 3 mints"):
                result = fetch_and_process_data(st.session_state["selected_project"],st.session_state["schema"])
                if "cache_key" not in st.session_state:
                    st.session_state["cache_key"] = 0
                st.session_state["cache_key"] += 1                
                # Fetch and process data again
                dataframes = fetch_dataframes_from_snowflake(st.session_state["cache_key"])
                print("Data fetched successfully")  # Debug statement
                
                # Example: Access DataFrames
                wkday_df = dataframes['wkday_df']
                wkday_dir_df = dataframes['wkday_dir_df']
                wkend_df = dataframes['wkend_df']
                wkend_dir_df = dataframes['wkend_dir_df']
                wkend_time_df = dataframes['wkend_time_df']
                wkday_time_df = dataframes['wkday_time_df']
                wkend_raw_df = dataframes['wkend_raw_df']
                wkday_raw_df = dataframes['wkday_raw_df']
                detail_df = dataframes['detail_df']
                wkday_stationwise_df = dataframes.get('wkday_stationwise_df')
                wkend_stationwise_df = dataframes.get('wkend_stationwise_df')
            st.success(f"Data Synced Successfully!")
        current_date = datetime.datetime.now()
        formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
        st.markdown(f"##### **Last Refresh DATE**: {formatted_date}")

        # Get the most recent "Completed" date from both wkday_raw_df and wkend_raw_df

        completed_dates = pd.concat([wkday_raw_df['Completed'], wkend_raw_df['Completed']])
        most_recent_completed_date = pd.to_datetime(completed_dates).max()

        # # # Display the most recent "Completed" date
        st.markdown(f"##### **Completed**: {most_recent_completed_date.strftime('%Y-%m-%d %H:%M:%S')}")


    # Page Content Section
    with header_col2:
        if page != 'timedetails':
            if page == "weekend":
                st.header(f'Total Records: {wkend_df["# of Surveys"].sum()}')
                csv_weekend_raw, week_end_raw_file_name = create_csv(wkend_raw_df, "wkend_raw_data.csv")
                download_csv(csv_weekend_raw, week_end_raw_file_name, "Download WeekEnd Raw Data")
            else:  # Default to weekday data for main and weekday pages
                st.header(f'Total Records: {wkday_df["# of Surveys"].sum()}')
                csv_weekday_raw, week_day_raw_file_name = create_csv(wkday_raw_df, "wkday_raw_data.csv")
                download_csv(csv_weekday_raw, week_day_raw_file_name, "Download WeekDay Raw Data")

            # Button for Time OF Day Details
            if st.button('Time OF Day Details'):
                st.experimental_set_query_params(page="timedetails")
                st.experimental_rerun()
                # st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=timedetails">', unsafe_allow_html=True)
        else:
            st.header(f'Time OF Day Details')

    # Button Section
    with header_col3:
        # WEEKDAY-OVERALL button
        if st.button("WEEKDAY-OVERALL"):
            st.experimental_set_query_params(page="weekday")
            st.experimental_rerun()
            # st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=weekday">', unsafe_allow_html=True)

        # WEEKEND-OVERALL button
        if st.button("WEEKEND-OVERALL"):
            st.experimental_set_query_params(page="weekend")
            st.experimental_rerun()
            # st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=weekend">', unsafe_allow_html=True)

        if 'rail' in selected_schema.lower():
            if st.button("WEEKDAY StationWise Comparison"):
                st.experimental_set_query_params(page="weekday_station")
                st.experimental_rerun()
                # st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=weekday_station">', unsafe_allow_html=True)

            if st.button("WEEKEND StationWise Comparison"):
                st.experimental_set_query_params(page="weekend_station")
                st.experimental_rerun()
                # st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=weekend_station">', unsafe_allow_html=True)


    # if "page" in query_params and query_params["page"][0] != page:
    #     st.experimental_rerun()


    def filter_dataframe(df, query):
        if query:
            df = df[df.apply(lambda row: row.astype(str).str.contains(query, case=False).any(), axis=1)]
        # if usage.lower() == 'use':
        #     df = df[df['Final_Usage'].str.lower() == 'use']
        # elif usage.lower() == 'remove':
        #     df = df[df['Final_Usage'].str.lower() == 'remove']
        
        # if date:
        #     df = df[df['Date'].dt.date == date]
        return df

    def time_details(details_df):
        # st.dataframe(details_df[['OPPO_TIME[CODE]', 'TIME_ON[Code]', 'TIME_ON', 'TIME_PERIOD[Code]',
        #                           'TIME_PERIOD', 'START_TIME']], height=670, use_container_width=True)

        st.dataframe(details_df, height=670, use_container_width=True)
        if st.button("GO TO HOME"):
            st.experimental_set_query_params(page="main")
            st.experimental_rerun()



    def render_aggrid(dataframe, height, pinned_column,key):
        """
        Render an AgGrid with the specified dataframe, height, and pinned column.
        
        Args:
            dataframe (pd.DataFrame): The dataframe to display in AgGrid.
            height (int): The height of the grid in pixels.
            pinned_column (str): The column to pin to the left.

        Returns:
            None: Displays the AgGrid.
        """
        # JavaScript code for cell styling
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
                return null;  // Default style
            }
        """)

        # Create GridOptionsBuilder
        gb = GridOptionsBuilder.from_dataframe(dataframe)
        gb.configure_default_column(editable=False, groupable=False, autoSizeColumns=True)

        # Pin the specified column
        if pinned_column in dataframe.columns:
            gb.configure_column(pinned_column, pinned='left')
        else:
            print(f"Column '{pinned_column}' not found in the dataframe")

        # Apply cell style to target columns
        valid_columns = [col for col in dataframe.columns if any(pattern in col for pattern in column_name_patterns)]
        for column in valid_columns:
            gb.configure_column(column, cellStyle=cellStyle)

        other_options = {'suppressColumnVirtualisation': True}

        # Build grid options
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
        
        # Render AgGrid
        AgGrid(
            dataframe,
            gridOptions=grid_options,
            height=height,
            theme="streamlit",  # Choose theme: 'streamlit', 'light', 'dark', etc.
            allow_unsafe_jscode=True,  # Required to enable custom JsCode
            key=f'grid_{key}',  # Unique key for the grid instance
            suppressHorizontalScroll=False,
            fit_columns_on_grid_load=False,
            columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
            reload_data=True,  # Allow horizontal scrolling
            width='100%',  # Ensure the grid takes full width
        )


    def main_page(data1, data2, data3):
        """Main page display with dynamic data"""
        # Create two columns layout
        col1, col2 = st.columns([2, 1])  # Left column is wider

        # Display the first dataframe on the left full screen (col1)
        with col1:
            if page=='main':
                st.subheader('Route Direction Level Comparison (WeekDAY)')
            else:
                st.subheader("Route Direction Level Comparison")
            filtered_df1 = filter_dataframe(data1, search_query)


            render_aggrid(filtered_df1,500,'ROUTE_SURVEYEDCode',1)
            csv1, file_name1 = create_csv(filtered_df1, "route_direction_comparison.csv")
            download_csv(csv1, file_name1, "Download Route Direction Comparison Data")

            filtered_df3 = filter_dataframe(data3, search_query)
            st.subheader("Route Level Comparison")
            render_aggrid(filtered_df3,400,'ROUTE_SURVEYEDCode',2)
            csv3, file_name3 = create_csv(filtered_df3, "route_level_comparison.csv")
            download_csv(csv3, file_name3, "Download Route Level Comparison Data")

        # Display buttons and dataframes in the second column (col2)
        with col2:

            st.subheader("Time Range Data")
            # Convert relevant columns in both dataframes to numeric values, handling errors
            data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']] = data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']].apply(pd.to_numeric, errors='coerce')
            data2[['1', '2', '3', '4']] = data2[['1', '2', '3', '4']].apply(pd.to_numeric, errors='coerce')

            # Fill any NaN values with 0 (or handle them differently if needed)
            data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']] = data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']].fillna(0)
            data2[['1', '2', '3', '4']] = data2[['1', '2', '3', '4']].fillna(0)

            # Calculate the sums for expected and collected totals
            expected_totals = data1[['(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']].sum()
            collected_totals = data2[['1', '2', '3', '4']].sum()

            # Calculate the difference, ensuring no negative values
            difference = np.maximum(expected_totals.values - collected_totals.values, 0)
            result_df = pd.DataFrame({
                'Time Period':  [ '1', '2', '3', '4',],
                'Collected Totals': collected_totals.values.astype(int),
                'Expected Totals': expected_totals.values.astype(int),
                'Remaining': difference.astype(int),
            })



            filtered_df2 = filter_dataframe(data2, search_query)

            render_aggrid(filtered_df2,500,'Display_Text',3)
            csv2, file_name2 = create_csv(filtered_df2, "time_range_data.csv")
            download_csv(csv2, file_name2, "Download Time Range Data")



            filtered_df4 = filter_dataframe(result_df, search_query)
        
            # Render AgGrid
            st.subheader("Time Period OverAll Data")
            render_aggrid(filtered_df4,400,'Time Period',4)

            csv4, file_name4 = create_csv(filtered_df4, "time_period_overall_data.csv")
            download_csv(csv4, file_name4, "Download Time Period Overall Data")




    def weekday_page():
        st.title("Weekday OverAll Data")

        wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED','(1) Collect', '(1) Remain',
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

    # if "page_type" not in st.session_state:
    #     st.session_state["page_type"] = "weekday"  # Default page
    def weekday_station_page():
        st.subheader('Route StationWise Comparison(WeekDAY)')
        filtered_df = filter_dataframe(wkday_stationwise_df[[ 'ROUTE_SURVEYEDCode','ROUTE_SURVEYED','STATION_ID','STATION_NAME', '(0) Collect', '(0) Remain', '(1) Collect', '(1) Remain',
                                '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(5) Collect', '(5) Remain',
                                '(0) Goal', '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']], search_query)

        render_aggrid(filtered_df,500,'ROUTE_SURVEYEDCode',1)

        if st.button("GO TO HOME"):
            st.experimental_set_query_params()
            st.experimental_rerun()

    def weekend_station_page():
        st.subheader('Route StationWise Comparison(WeekEND)')
        filtered_df = filter_dataframe(wkend_stationwise_df[[ 'ROUTE_SURVEYEDCode','ROUTE_SURVEYED','STATION_ID','STATION_NAME', '(0) Collect', '(0) Remain', '(1) Collect', '(1) Remain',
                                '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(5) Collect', '(5) Remain',
                                '(0) Goal', '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']], search_query)

        render_aggrid(filtered_df,500,'ROUTE_SURVEYEDCode',1)
        if st.button("GO TO HOME"):
            st.experimental_set_query_params()
            st.experimental_rerun()

    # if page=='register':
    #     register()
    # if not st.session_state.logged_in:
    #     login()
    # else:
    if 'rail' in selected_schema.lower():
        if page == "weekday":
            weekday_page()
        elif page == "weekend":
            weekend_page()
        elif page=='timedetails':
            time_details(detail_df)
        elif page=='weekday_station':
            weekday_station_page()
        elif page=='weekend_station':
            weekend_station_page()
        else:
            wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                    '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']

            main_page(wkday_dir_df[wkday_dir_columns],
                    wkday_time_df[['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']],
                    wkday_df[['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', 'Route Level Goal', '# of Surveys', 'Remaining']])
    else:
        if page == "weekday":
            weekday_page()
        elif page == "weekend":
            weekend_page()
        elif page=='timedetails':
            time_details(detail_df)
        else:
            wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                    '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']

            main_page(wkday_dir_df[wkday_dir_columns],
                    wkday_time_df[['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']],
                    wkday_df[['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', 'Route Level Goal', '# of Surveys', 'Remaining']])