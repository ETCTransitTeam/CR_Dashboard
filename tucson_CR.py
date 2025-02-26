
import os
import datetime
import numpy as np
import pandas as pd
import streamlit as st
import snowflake.connector
from automated_refresh_flow_new import fetch_and_process_data
from utils import render_aggrid,create_csv,download_csv,update_query_params
from authentication.auth import schema_value,register_page,login,logout,is_authenticated,forgot_password,reset_password,activate_account,change_password,send_change_password_email,change_password_form,create_new_user_page
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
    else:
        st.write('Token Expired. LogIn Again')
        if st.button("Go to Login Page"):
            # st.experimental_set_query_params(page="login")
            # st.experimental_rerun()
            st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=login">', unsafe_allow_html=True)
    st.stop()
else:
    if not is_authenticated():
        st.error("Your Token Expired.You need to log in first.")
        # Optionally, redirect the user to the login page
        st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=login">', unsafe_allow_html=True)
    else:
        
        # if "page" not in st.session_state:
        #     st.session_state["page"] = "main"

        # # Retrieve current query parameters
        # query_params = st.experimental_get_query_params()
        # page = query_params.get("page", ["main"])[0]
        # if page != st.session_state["page"]:
        #     st.session_state["page"] = page
        # print(st.session_state["page"])
        # st.write('Welcome to the protected page!')
        selected_schema = st.session_state.get("schema", None)
        selected_project = str(st.session_state.get("selected_project", "")).lower()
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

        st.sidebar.text(st.session_state['user']["role"])
        st.sidebar.text(st.session_state['user']["username"])
        st.sidebar.text(st.session_state['user']["email"])
        st.sidebar.header("Filters")
        search_query=st.sidebar.text_input(label='Search', placeholder='Search')
        # st.sidebar.text(st.session_state['token'])
        # if "logged_in" in st.session_state and st.session_state["logged_in"]:
        #     st.sidebar.header("Select Project")

        #     # Get available projects for the logged-in user
        #     available_projects = st.session_state.get("available_projects", [])

        #     if available_projects:
        #         if "selected_project" not in st.session_state:
        #             st.session_state["selected_project"] = available_projects[0]

        #         # Allow user to change project
        #         selected_project = st.sidebar.selectbox("Projects", available_projects, 
        #                                                 index=available_projects.index(st.session_state["selected_project"]),
        #                                                 key="selected_project")

        #         # Update schema in session when project changes
        #         st.session_state["schema"] = schema_value[selected_project]
        # else:
        #     st.sidebar.warning("No projects assigned.")
        # if st.session_state['user']["role"].lower()=='admin':

        #     if st.sidebar.button('ADD User', key='Create USER Button'):
        #         st.sidebar.text('Create a User')
        #         st.session_state['page'] = 'create_user'
        #         # st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=create_user">', unsafe_allow_html=True)

        #         st.experimental_set_query_params(page="create_user")
        #         st.experimental_rerun()

        # if st.sidebar.button('Change Password', key='Change Password Button'):
        #     # send_change_password_email(st.session_state['user']['email'])

        #     st.experimental_set_query_params(page="change_password")
        #     st.experimental_rerun()

        if st.sidebar.button("Logout",key='Logout Button'):
            logout()
            # st.session_state["logged_in"] = False
            # st.markdown(f'<meta http-equiv="refresh" content="0;url=/?page=login">', unsafe_allow_html=True)

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



        def main_page(data1, data2, data3):
            """Main page display with dynamic data"""
            # Create two columns layout
            col1, col2 = st.columns([2, 1])  # Left column is wider

            # Display the first dataframe on the left full screen (col1)
            with col1:
                if current_page=='main':
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
            if 'tucson' in selected_project:
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
            if st.button("GO TO HOME"):
                st.experimental_set_query_params(page="main")
                st.experimental_rerun()

        def weekend_page():
            st.title("Weekend OverAll Data")
            if 'tucson' in selected_project:
                wkend_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain',
                                        '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                wkend_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4']
            else:
                wkend_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(0) Collect', '(0) Remain','(1) Collect', '(1) Remain',
                                        '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain','(5) Collect', '(5) Remain',
                                        '(0) Goal','(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal','(5) Goal']
                wkend_time_columns=['Display_Text', 'Original Text', 'Time Range', '1', '2', '3', '4','5']
                
                
            main_page(wkend_dir_df[wkend_dir_columns],
                    wkend_time_df[wkend_time_columns],
                    wkend_df[['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', 'Route Level Goal', '# of Surveys', 'Remaining']])

            if st.button("GO TO HOME"):
                st.experimental_set_query_params(page="main")
                st.experimental_rerun()

        # if "page_type" not in st.session_state:
        #     st.session_state["page_type"] = "weekday"  # Default page
        def weekday_station_page():
            st.subheader('Route StationWise Comparison(WeekDAY)')
            if 'tucson' in selected_project:
                wkday_stationwise_columns=[ 'ROUTE_SURVEYEDCode','ROUTE_SURVEYED','STATION_ID','STATION_NAME','(1) Collect', '(1) Remain',
                                    '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', 
                                     '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
            else:
                wkday_stationwise_columns=[ 'ROUTE_SURVEYEDCode','ROUTE_SURVEYED','STATION_ID','STATION_NAME', '(0) Collect', '(0) Remain', '(1) Collect', '(1) Remain',
                    '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(5) Collect', '(5) Remain',
                    '(0) Goal', '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']
            filtered_df = filter_dataframe(wkday_stationwise_df[wkday_stationwise_columns], search_query)

            render_aggrid(filtered_df,500,'ROUTE_SURVEYEDCode',1)

            if st.button("GO TO HOME"):
                st.experimental_set_query_params()
                st.experimental_rerun()

        def weekend_station_page():
            st.subheader('Route StationWise Comparison(WeekEND)')
            if 'tucson' in selected_project:
                wkend_stationwise_columns=[ 'ROUTE_SURVEYEDCode','ROUTE_SURVEYED','STATION_ID','STATION_NAME','(1) Collect', '(1) Remain',
                                    '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', 
                                     '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
            else:
                wkend_stationwise_columns=[ 'ROUTE_SURVEYEDCode','ROUTE_SURVEYED','STATION_ID','STATION_NAME', '(0) Collect', '(0) Remain', '(1) Collect', '(1) Remain',
                    '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(5) Collect', '(5) Remain',
                    '(0) Goal', '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal', '(5) Goal']

            filtered_df = filter_dataframe(wkend_stationwise_df[wkend_stationwise_columns], search_query)

            render_aggrid(filtered_df,500,'ROUTE_SURVEYEDCode',1)
            if st.button("GO TO HOME"):
                st.experimental_set_query_params()
                st.experimental_rerun()

        if current_page=='create_user':
            create_new_user_page()
        elif current_page=='change_password':
            change_password(st.session_state['user']['email'])
        else:
            # def header_section(wkday_df,wkday_dir_df,wkend_df,wkend_dir_df,wkend_time_df,wkday_time_df,wkend_raw_df,wkday_raw_df,detail_df,wkday_stationwise_df,wkend_stationwise_df):
            # Layout columns
            header_col1, header_col2, header_col3 = st.columns([2, 2, 1])

            # Header Section
            with header_col1:
                st.header('Completion Report')
                # Button to trigger the entire script
            
                if st.session_state['user']["role"].lower()=='admin':
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
                if current_page != 'timedetails':
                    if current_page == "weekend":
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

            # header_section(wkday_df,wkday_dir_df,wkend_df,wkend_dir_df,wkend_time_df,wkday_time_df,wkend_raw_df,wkday_raw_df,detail_df,wkday_stationwise_df,wkend_stationwise_df)

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
                else:
                    # wkday_dir_columns = ['ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', '(1) Collect', '(1) Remain',
                    #                         '(2) Collect', '(2) Remain', '(3) Collect', '(3) Remain', '(4) Collect', '(4) Remain', '(1) Goal', '(2) Goal', '(3) Goal', '(4) Goal']
                    if 'tucson' in selected_project:
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
                else:
                    if 'tucson' in selected_project:
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