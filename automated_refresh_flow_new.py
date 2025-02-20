import pandas as pd
import numpy as np
from datetime import date,datetime
from geopy.distance import geodesic
import warnings
import copy
from database import DatabaseConnector
import os
from dotenv import load_dotenv
import math
import random
import streamlit as st
import io
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import boto3
from io import BytesIO
from decouple import config
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

warnings.filterwarnings('ignore')

load_dotenv()



PROJECTS = {
    "TUCSON": {
        "database": os.getenv("TUCSON_DATABASE_NAME"),
        "table": os.getenv("TUCSON_TABLE_NAME"),
        "files": {
            "details": "details_TUCSON_AZ_od_excel.xlsx",
            "cr": "TUCSON_AZ_CR.xlsx"
        }
    },
    "UTA": {
        "database": os.getenv("UTA_DATABASE_NAME"),
        "table": os.getenv("UTA_TABLE_NAME"),
        "files": {
            "details": "details_project_od_excel_UTA.xlsx",
            "cr": "UTA_SL_CR.xlsx"
        }
    },
    "VTA": {
        "database": os.getenv("VTA_DATABASE_NAME"),
        "table": os.getenv("VTA_TABLE_NAME"),
        "files": {
            "details": "details_vta_CA_od_excel.xlsx",
            "cr": "VTA_CA_CR.xlsx"
        }
    }
}

def fetch_and_process_data(project,schema):

    # in some Compeletion Report LSNAMECODE is splited in some it is not so have to check that
    def edit_ls_code_column(x):
        value=x.split('_')
        if len(value)>3:
            route_value="_".join(value[:-1])
        else:
            route_value="_".join(value)
        return route_value

    # for generated file version
    version=2
    project_name=project
    today_date = date.today()
    today_date=''.join(str(today_date).split('-'))

    project_config = PROJECTS[project]
    table_name=project_config["table"]
    database_name=project_config["database"]
    # Function to fetch data from the database
    # def fetch_data(database_name, table_name):
    #     HOST = os.getenv("SQL_HOST")
    #     USER = os.getenv("SQL_USER")
    #     PASSWORD = os.getenv("SQL_PASSWORD")
        
    #     db_connector = DatabaseConnector(HOST, database_name, USER, PASSWORD)

    #     try:
    #         db_connector.connect()
    #         print("Connection successful.....")

    #         if db_connector.connection is None:
    #             st.error("Database connection failed. Check credentials and server availability.")
    #             st.experimental_set_query_params(logged_in="true", page='main')
    #             st.experimental_rerun()
    #             return None
            
    #         connection = db_connector.connection  # Get MySQL connection object
    #         print(f"{table_name=} and {database_name=}")
            
    #         select_query = f"SELECT * FROM {table_name}"
    #         df = pd.read_sql(select_query, connection)  # Load data into DataFrame
    #         print(df.tail(2))
            
    #         db_connector.disconnect()  # Close database connection

    #         # Store DataFrame in memory
    #         csv_buffer = io.StringIO()
    #         df.to_csv(csv_buffer, index=False)
    #         csv_buffer.seek(0)

    #         return csv_buffer  

    #     # except mysql.connector.Error as sql_err:
    #     #     st.error(f"Database error: {sql_err}")
            
    #     except Exception as e:
    #         st.error(f"Error fetching data: {e}")
        
    #     # finally:
    #     #     try:
    #     #         db_connector.disconnect()
    #     #     except Exception:
    #     #         pass  # Ensure it doesn't crash on failed disconnect

    #     # # Redirect back to the main page without disturbing the whole process
    #     # st.experimental_set_query_params(logged_in="true", page='main')
    #     # st.experimental_rerun()
    #     # return None

    def fetch_data(database_name,table_name):
        HOST = os.getenv("SQL_HOST")
        USER = os.getenv("SQL_USER")
        PASSWORD = os.getenv("SQL_PASSWORD")
        # DATABASE = os.getenv("SQL_DATABASE")
        # db_connector = DatabaseConnector(HOST, 'elvistucsonod2025', USER, PASSWORD)
        db_connector = DatabaseConnector(HOST, database_name, USER, PASSWORD)
        try:
            db_connector.connect()
            print("Connection successful.....")
            if db_connector.connection is None:
                st.error("Database connection failed. Check credentials and server availability.")
                return None
            
            connection = db_connector.connection  # Get MySQL connection object
            print(f"{table_name=} and {database_name=}")
            select_query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(select_query, connection)  # Load data into DataFrame
            print(df.tail(2))
            db_connector.disconnect()  # Close database connection

            # Store DataFrame in memory
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            return csv_buffer  

        except Exception as e:
            st.error(f"Error fetching data: {e}")
            return None
        

    # Initialize session state for df
    if "df" not in st.session_state:
        st.session_state.df = None

    # Streamlit button to fetch data
    csv_buffer = fetch_data(database_name,table_name)
    if csv_buffer:  # Ensure data was fetched successfully
        st.session_state.df = pd.read_csv(csv_buffer)  # Load into DataFrame from memory
    else:
        st.error("Failed to load data.")

    # Display DataFrame if available
    if st.session_state.df is not None:
        df = st.session_state.df
        # Apply filters only after confirming df is loaded
        df = df[df['INTERV_INIT'] != '999']
        df = df[df['HAVE_5_MIN_FOR_SURVECode'] == 1]
        df=df[df['INTERV_INIT']!=999]
        df.drop_duplicates(subset='id',inplace=True)

        
        # st.write(df.head())  # Display the filtered data
    else:
        st.warning("No data available. Click 'Fetch Data' to load the dataset.")

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

    bucket_name = os.getenv('bucket_name')
    file_keys = {
        'details_TUCSON_AZ_od_excel.xlsx': ['STOPS', 'XFERS'],
        'TUCSON_AZ_CR.xlsx': ['WkEND-Overall', 'WkEND-RouteTotal', 'WkDAY-Overall', 'WkDAY-RouteTotal']
    }
    s3_client = boto3.client(
    's3',
    aws_access_key_id = os.getenv('aws_access_key_id'),
    aws_secret_access_key = os.getenv('aws_secret_access_key')
    )
    # Function to read an Excel file from S3 into a DataFrame
    def read_excel_from_s3(bucket_name, file_key, sheet_name):
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        excel_data = response['Body'].read()
        return pd.read_excel(BytesIO(excel_data), sheet_name=sheet_name)

    if project=='TUCSON':
        detail_df_stops = read_excel_from_s3(bucket_name,project_config["files"]["details"], 'STOPS')
        detail_df_xfers = read_excel_from_s3(bucket_name, project_config["files"]["details"], 'XFERS')

        wkend_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkEND-Overall')
        wkend_route_df = read_excel_from_s3(bucket_name,project_config["files"]["cr"], 'WkEND-RouteTotal')

        wkday_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-Overall')
        wkday_route_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-RouteTotal')
        print("Files read for TUCSON")

    elif project=='VTA':
        detail_df_stops = read_excel_from_s3(bucket_name,project_config["files"]["details"], 'STOPS')
        detail_df_xfers = read_excel_from_s3(bucket_name, project_config["files"]["details"], 'XFERS')

        wkend_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkEND-Overall')
        wkend_route_df = read_excel_from_s3(bucket_name,project_config["files"]["cr"], 'WkEND-RouteTotal')

        wkday_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-Overall')
        wkday_route_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-RouteTotal')
        df['ROUTE_SURVEYEDCode_Splited']=df['ROUTE_SURVEYEDCode'].apply(lambda x:('_').join(str(x).split('_')[:-1]) )

        print("Files read for VTA")

    elif project=='UTA':
        detail_df_stops = read_excel_from_s3(bucket_name,project_config["files"]["details"], 'STOPS')
        detail_df_xfers = read_excel_from_s3(bucket_name, project_config["files"]["details"], 'XFERS')

        
        wkend_overall_df=read_excel_from_s3(bucket_name,project_config["files"]["cr"],'WkEND-RAIL')
        # wkend_overall_df['LS_NAME_CODE']=wkend_overall_df['LS_NAME_CODE'].apply(edit_ls_code_column)
        wkend_route_df=read_excel_from_s3(bucket_name,project_config["files"]["cr"],'WkEND-RailTotal')

        wkday_overall_df=read_excel_from_s3(bucket_name,project_config["files"]["cr"],'WkDAY-RAIL')
        # wkday_overall_df['LS_NAME_CODE']=wkday_overall_df['LS_NAME_CODE'].apply(edit_ls_code_column)
        wkday_route_df=read_excel_from_s3(bucket_name,project_config["files"]["cr"],'WkDAY-RailTotal')
        print("Files read for UTA")

        df['ROUTE_SURVEYEDCode_Splited']=df['ROUTE_SURVEYEDCode'].apply(lambda x:('_').join(str(x).split('_')[:-1]) )
        stop_on_clntid=['stoponclntid']
        stop_on_clntid=check_all_characters_present(df,stop_on_clntid)
        df['STATION_ID_SPLITTED']=df[stop_on_clntid[0]].apply(lambda x:str(x).split('_')[-1])


        wkend_overall_df['STATION_ID_SPLITTED']=wkend_overall_df['STATION_ID'].apply(lambda x: str(x).split('_')[-1])
        wkday_overall_df['STATION_ID_SPLITTED']=wkday_overall_df['STATION_ID'].apply(lambda x: str(x).split('_')[-1])

        wkday_route_df['ROUTE_TOTAL'] = pd.to_numeric(wkday_route_df['ROUTE_TOTAL'], errors='coerce')
        wkday_route_df['ROUTE_TOTAL'].fillna(0, inplace=True)
        wkend_route_df['ROUTE_TOTAL'] = pd.to_numeric(wkend_route_df['ROUTE_TOTAL'], errors='coerce')
        wkend_route_df['ROUTE_TOTAL'].fillna(0, inplace=True)

        wkday_route_df['ROUTE_TOTAL'] = np.ceil(wkday_route_df['ROUTE_TOTAL']).astype(int)
        wkend_route_df['ROUTE_TOTAL'] = np.ceil(wkend_route_df['ROUTE_TOTAL']).astype(int)

        wkday_overall_df[[0,1,2,3,4,5]]=wkday_overall_df[[0,1,2,3,4,5]].fillna(0)
        wkend_overall_df[[0,1,2,3,4,5]]=wkend_overall_df[[0,1,2,3,4,5]].fillna(0)

    print('Files From S3 loaded succssfully')
    # detail_df_stops = read_excel_from_s3(bucket_name, 'details_TUCSON_AZ_od_excel.xlsx', 'STOPS')
    # detail_df_xfers = read_excel_from_s3(bucket_name, 'details_TUCSON_AZ_od_excel.xlsx', 'XFERS')

    # wkend_overall_df = read_excel_from_s3(bucket_name, 'TUCSON_AZ_CR.xlsx', 'WkEND-Overall')
    # wkend_route_df = read_excel_from_s3(bucket_name, 'TUCSON_AZ_CR.xlsx', 'WkEND-RouteTotal')

    # wkday_overall_df = read_excel_from_s3(bucket_name, 'TUCSON_AZ_CR.xlsx', 'WkDAY-Overall')
    # wkday_route_df = read_excel_from_s3(bucket_name, 'TUCSON_AZ_CR.xlsx', 'WkDAY-RouteTotal')


    df=df[df['HAVE_5_MIN_FOR_SURVECode']==1]
    df=df[df['INTERV_INIT']!='999']
    df=df[df['INTERV_INIT']!=999]

    stop_on_column_check=['stoponaddr']
    stop_off_column_check=['stopoffaddr']
    stop_on_id_column_check=['stoponclntid']
    stop_off_id_column_check=['stopoffclntid']
    stop_on_id_column=check_all_characters_present(df,stop_on_id_column_check)
    stop_off_id_column=check_all_characters_present(df,stop_off_id_column_check)
    stop_on_column=check_all_characters_present(df,stop_on_column_check)
    stop_off_column=check_all_characters_present(df,stop_off_column_check)

    stop_on_lat_lon_columns_check=['stoponlat','stoponlong']
    stop_off_lat_lon_columns_check=['stopofflat','stopofflong']
    stop_on_lat_lon_columns=check_all_characters_present(df,stop_on_lat_lon_columns_check)
    stop_off_lat_lon_columns=check_all_characters_present(df,stop_off_lat_lon_columns_check)
    stop_off_lat_lon_columns.sort()
    stop_on_lat_lon_columns.sort()

    origin_address_lat_column=['originaddresslat']
    origin_address_long_column=['originaddresslong']
    origin_address_lat=check_all_characters_present(df,origin_address_lat_column)
    origin_address_long=check_all_characters_present(df,origin_address_long_column)

    route_surveyed_column_check=['routesurveyedcode']
    route_surveyed_column=check_all_characters_present(df,route_surveyed_column_check)


    date_columns_check=['completed','datestarted']
    date_columns=check_all_characters_present(df,date_columns_check)

    def determine_date(row):
        if not pd.isnull(row[date_columns[0]]):
            return row[date_columns[0]]
        elif not pd.isnull(row[date_columns[1]]):
            return row[date_columns[1]]
        else:
            return pd.NaT

    df['Date'] = df.apply(determine_date, axis=1)


    def get_day_name(x):
        if pd.isna(x):  # Check if x is NaT (missing value)
            return None  # Or return 'Unknown' or 'Missing' based on your needs
        formats_to_check = ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M']
        
        for format_str in formats_to_check:
            try:
                date_object = datetime.strptime(x, format_str)
                day_name = date_object.strftime('%A')
                return day_name
            except ValueError:
                continue

    df['Day']=df['Date'].apply(get_day_name)


    try:
        df['LAST_SURVEY_DATE'] = pd.to_datetime(df['Date'], format='%d/%m/%Y %H:%M', errors='coerce')
    except Exception as e:
        print(f"Error encountered: {e}")
        df['LAST_SURVEY_DATE'] = pd.to_datetime(df['Date'], errors='coerce', dayfirst=True)
    latest_date = df['LAST_SURVEY_DATE'].max()
    latest_date_df = pd.DataFrame({'Latest_Survey_Date': [latest_date]})


    weekend_df=df[df['Day'].isin(['Saturday','Sunday'])]

    weekday_df=df[~(df['Day'].isin(['Saturday','Sunday']))]


    # df.to_csv('Day Time SantaClarita.csv',index=False)

    # exit()
    #to get the TIMEON column
    time_column_check=['timeoncode']
    time_period_column_check=['timeon']
    time_column=check_all_characters_present(df,time_column_check)
    time_period_column=check_all_characters_present(df,time_period_column_check)
    route_survey_column_check=['routesurveyedcode']
    route_survey_column=check_all_characters_present(df,route_survey_column_check)
    stopon_clntid_column_check=['stoponclntid']
    stopon_clntid_column=check_all_characters_present(df,stopon_clntid_column_check)
    stopoff_clntid_column_check=['stopoffclntid']
    stopoff_clntid_column=check_all_characters_present(df,stopoff_clntid_column_check)


    # df[['id','Day',route_survey_column[0]]].to_csv('Checking Day Names.csv',index=False)


    wkend_overall_df.dropna(subset=['LS_NAME_CODE'],inplace=True)
    wkday_overall_df.dropna(subset=['LS_NAME_CODE'],inplace=True)

    
    def create_time_value_df_with_display(overall_df,df,project):
        """
        Create a time-value DataFrame summarizing counts and time ranges.

        Parameters:
            df (pd.DataFrame): Input DataFrame containing the time values.
            time_column (str): Name of the column in the input DataFrame containing the time values.

        Returns:
            pd.DataFrame: Processed DataFrame with counts, time ranges, and display text.
        """

        if project=='TUCSON':
            am_values = ['AM1','AM2','AM3', 'AM4']
            midday_values = [ 'MID1', 'MID2','MID3', 'MID4', 'MID5', 'MID6' ]
            pm_values = ['PM1','PM2', 'PM3']
            evening_values = ['OFF1', 'OFF2', 'OFF3', 'OFF4','OFF5']
            # for TUCSON have to change time groups too
            time_group_mapping = {
                1: am_values,
                2: midday_values,
                3: pm_values,
                4: evening_values,
            }

            time_mapping = {
                'AM1': 'Before 5:30 am',
                'AM2': '5:30 am - 6:30 am',
                'AM3': '6:30 am - 7:30 am',
                'AM4': '7:30 am - 8:30 am',
                'MID1': '8:30 am - 9:30 am',
                'MID2': '9:30 am - 10:30 am',
                'MID3': '10:30 am - 11:30 am',
                'MID4': '11:30 am - 12:30 pm',
                'MID5': '12:30 pm - 1:30 pm',
                'MID6': '1:30 pm - 2:30 pm',
                'PM1': '2:30 pm - 3:30 pm',
                'PM2': '3:30 pm - 4:30 pm',
                'PM3': '4:30 pm - 5:30 pm',
                'OFF1': '5:30 pm - 6:30 pm',
                'OFF2': '6:30 pm - 7:30 pm',
                'OFF3': '7:30 pm - 8:30 pm',
                'OFF4': '8:30 pm - 9:30 pm',
                'OFF5': 'After 9:30 pm'
            }

        # Initialize the new DataFrame
            new_df = pd.DataFrame(columns=["Original Text",  1, 2, 3, 4])

            # Populate the DataFrame with counts
            for col, values in time_group_mapping.items():
                for value in values:
                    count = df[df[time_column[0]] == value].shape[0]
                    row = {"Original Text": value}

                    # Initialize all columns to 0
                    for c in range(6):
                        row[c] = 0

                    # Update the corresponding column with the count
                    row[col] = count
                    new_df = pd.concat([new_df, pd.DataFrame([row])], ignore_index=True)

            # Map time values to time ranges
            new_df['Time Range'] = new_df['Original Text'].map(time_mapping)

            # Drop rows with missing time ranges
            new_df.dropna(subset=['Time Range'], inplace=True)

            # Add a display text column with sequential numbering
            new_df['Display_Text'] = range(1, len(new_df) + 1)
        elif project=='VTA':
            pre_early_am_values = ['AM1']
            early_am_values = ['AM2']
            am_values = ['AM3', 'AM4', 'MID1', 'MID2', 'MID7']
            midday_values = ['MID3', 'MID4', 'MID5', 'MID6', 'PM1']
            pm_values = ['PM2', 'PM3', 'PM4', 'PM5']
            evening_values = ['PM6', 'PM7', 'PM8', 'PM9']

            # Mapping time groups to corresponding columns
            time_group_mapping = {
                0: pre_early_am_values,
                1: early_am_values,
                2: am_values,
                3: midday_values,
                4: pm_values,
                5: evening_values,
            }

            # Mapping time values to time ranges
            time_mapping = {
                'AM1': 'Before 5:00 am',
                'AM2': '5:00 am - 6:00 am',
                'AM3': '6:00 am - 7:00 am',
                'MID1': '7:00 am - 8:00 am',
                'MID2': '8:00 am - 9:00 am',
                'MID7': '9:00 am - 10:00 am',
                'MID3': '10:00 am - 11:00 am',
                'MID4': '11:00 am - 12:00 pm',
                'MID5': '12:00 pm - 1:00 pm',
                'MID6': '1:00 pm - 2:00 pm',
                'PM1': '2:00 pm - 3:00 pm',
                'PM2': '3:00 pm - 4:00 pm',
                'PM3': '4:00 pm - 5:00 pm',
                'PM4': '5:00 pm - 6:00 pm',
                'PM5': '6:00 pm - 7:00 pm',
                'PM6': '7:00 pm - 8:00 pm',
                'PM7': '8:00 pm - 9:00 pm',
                'PM8': '9:00 pm - 10:00 pm',
                'PM9': 'After 10:00 pm'
            }

            # Initialize the new DataFrame
            new_df = pd.DataFrame(columns=["Original Text", 0, 1, 2, 3, 4, 5])

            # Populate the DataFrame with counts
            for col, values in time_group_mapping.items():
                for value in values:
                    count = df[df[time_column[0]] == value].shape[0]
                    row = {"Original Text": value}

                    # Initialize all columns to 0
                    for c in range(6):
                        row[c] = 0

                    # Update the corresponding column with the count
                    row[col] = count
                    new_df = pd.concat([new_df, pd.DataFrame([row])], ignore_index=True)

            # Map time values to time ranges
            new_df['Time Range'] = new_df['Original Text'].map(time_mapping)

            # Drop rows with missing time ranges
            new_df.dropna(subset=['Time Range'], inplace=True)

            # Add a display text column with sequential numbering
            new_df['Display_Text'] = range(1, len(new_df) + 1)
        elif project=='UTA':
             # Filter df where overall_df['ROUTE_SURVEYEDCode'] == df['ROUTE_SURVEYEDCode_Splited']
            matched_df = df[df['ROUTE_SURVEYEDCode_Splited'].isin(overall_df['ROUTE_SURVEYEDCode'])]

            # Define time value groups
            pre_early_am_values = [1]
            early_am_values = [2]
            am_values = [3, 4, 5, 6]
            midday_values = [7, 8, 9, 10, 11]
            pm_values = [12, 13, 14]
            evening_values = [15, 16, 17, 18]

            # Mapping time groups to corresponding columns
            time_group_mapping = {
                0: pre_early_am_values,
                1: early_am_values,
                2: am_values,
                3: midday_values,
                4: pm_values,
                5: evening_values,
            }

            # Mapping time values to time ranges
            time_mapping = {
                1: 'Before 5:00 am',
                2: '5:00 am - 6:00 am',
                3: '6:00 am - 7:00 am',
                4: '7:00 am - 8:00 am',
                5: '8:00 am - 9:00 am',
                6: '9:00 am - 10:00 am',
                7: '10:00 am - 11:00 am',
                8: '11:00 am - 12:00 pm',
                9: '12:00 pm - 1:00 pm',
                10: '1:00 pm - 2:00 pm',
                11: '2:00 pm - 3:00 pm',
                12: '3:00 pm - 4:00 pm',
                13: '4:00 pm - 5:00 pm',
                14: '5:00 pm - 6:00 pm',
                15: '6:00 pm - 7:00 pm',
                16: '7:00 pm - 8:00 pm',
                17: '8:00 pm - 9:00 pm',
                18: 'After 9:00 pm',
            }

            # Ensure the time_column is of integer type
            matched_df[time_column] = matched_df[time_column].fillna(0).astype(int)

            # Initialize the new DataFrame
            new_df = pd.DataFrame(columns=["Original Text", 0, 1, 2, 3, 4, 5])

            # Populate the DataFrame with counts
            for col, values in time_group_mapping.items():
                for value in values:
                    count = matched_df[matched_df[time_column[0]] == value].shape[0]
                    row = {"Original Text": value}

                    # Initialize all columns to 0
                    for c in range(6):
                        row[c] = 0

                    # Update the corresponding column with the count
                    row[col] = count
                    new_df = pd.concat([new_df, pd.DataFrame([row])], ignore_index=True)

            # Map time values to time ranges
            new_df['Time Range'] = new_df['Original Text'].map(time_mapping)

            # Drop rows with missing time ranges
            new_df.dropna(subset=['Time Range'], inplace=True)

            # Add a display text column with sequential numbering
            new_df['Display_Text'] = range(1, len(new_df) + 1)

        return new_df
    if project!='UTA':
        wkend_time_value_df=create_time_value_df_with_display(wkend_overall_df,weekend_df,project)
        wkday_time_value_df=create_time_value_df_with_display(wkday_overall_df,weekday_df,project)

    # To create Route_SurveyedCode Direction wise comparison in terms of time values
    def create_route_direction_level_df(overalldf,df,project):
        if project=='TUCSON':
            # For Tucson PROJECT Have to change values TIME PERIOD VALUES
            am_values = ['AM1','AM2','AM3', 'AM4']
            midday_values = [ 'MID1', 'MID2','MID3', 'MID4', 'MID5', 'MID6' ]
            pm_values = ['PM1','PM2', 'PM3']
            evening_values = ['OFF1', 'OFF2', 'OFF3', 'OFF4','OFF5']
            am_column=[1] #This is for AM header
            midday_colum=[2] #this is for MIDDAY header
            pm_column=[3] #this is for PM header
            evening_column=[4] #this is for EVENING header

            def convert_string_to_integer(x):
                try:
                    return float(x)
                except (ValueError, TypeError):
                    return 0
                
            # Creating new dataframe for specifically AM, PM, MIDDAY, Evenving data and added values from Compeletion Report

            # For TUCSON PROJECT we are not using pre_early_am and early_am columns so have to comment the following code accordingly 
            new_df=pd.DataFrame()
            new_df['ROUTE_SURVEYEDCode']=overalldf['LS_NAME_CODE']
            # new_df['CR_PRE_Early_AM']=overalldf[pre_early_am_column[0]].apply(math.ceil)
            # new_df['CR_Early_AM']=overalldf[early_am_column[0]].apply(math.ceil)
            new_df['CR_AM_Peak']=overalldf[am_column[0]].apply(math.ceil)
            new_df['CR_Midday']=overalldf[midday_colum[0]].apply(math.ceil)
            new_df['CR_PM_Peak']=overalldf[pm_column[0]].apply(math.ceil)
            new_df['CR_Evening']=overalldf[evening_column[0]].apply(math.ceil)
            # print("new_df_columns",new_df.columns)
            # new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
            new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
            new_df.fillna(0,inplace=True)

            for index, row in new_df.iterrows():
                route_code = row['ROUTE_SURVEYEDCode']

                def get_counts_and_ids(time_values):
                    # Just for SALEM
                    # subset_df = df[(df['ROUTE_SURVEYEDCode_Splited'] == route_code) & (df[time_column[0]].isin(time_values))]
                    subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code) & (df[time_column[0]].isin(time_values))]
                    subset_df=subset_df.drop_duplicates(subset='id')
                    count = subset_df.shape[0]
                    ids = subset_df['id'].values
                    return count, ids

                # pre_early_am_value, pre_early_am_value_ids = get_counts_and_ids(pre_early_am_values)
                # early_am_value, early_am_value_ids = get_counts_and_ids(early_am_values)
                am_value, am_value_ids = get_counts_and_ids(am_values)
                midday_value, midday_value_ids = get_counts_and_ids(midday_values)
                pm_value, pm_value_ids = get_counts_and_ids(pm_values)
                evening_value, evening_value_ids = get_counts_and_ids(evening_values)
                
                # new_df.loc[index, 'CR_Total'] = row['CR_PRE_Early_AM']+row['CR_Early_AM']+row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
                new_df.loc[index, 'CR_Total'] = row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
                new_df.loc[index, 'CR_AM_Peak'] =row['CR_AM_Peak']
                # new_df.loc[index, 'CR_AM_Peak'] =row['CR_PRE_EARLY_AM']+row['CR_EARLY_AM']+ row['CR_AM_Peak']

                # new_df.loc[index, 'DB_PRE_Early_AM_Peak'] = pre_early_am_value
                # new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_value
                new_df.loc[index, 'DB_AM_Peak'] = am_value
                new_df.loc[index, 'DB_Midday'] = midday_value
                new_df.loc[index, 'DB_PM_Peak'] = pm_value
                new_df.loc[index, 'DB_Evening'] = evening_value
                # new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value+pre_early_am_value+early_am_value
                new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value
                
            #     new_df['ROUTE_SURVEYEDCode_Splited']=new_df['ROUTE_SURVEYEDCode'].apply(lambda x:('_').join(x.split('_')[:-1]) )
                route_code_level_df=pd.DataFrame()

                unique_routes=new_df['ROUTE_SURVEYEDCode'].unique()

                route_code_level_df['ROUTE_SURVEYEDCode']=unique_routes

                # weekend_df.rename(columns={'ROUTE_TOTAL':'CR_Overall_Goal','SURVEY_ROUTE_CODE':'ROUTE_SURVEYEDCode','LS_NAME_CODE':'ROUTE_SURVEYEDCode'},inplace=True)

                for index, row in new_df.iterrows():
                    # pre_early_am_peak_diff=row['CR_PRE_Early_AM']-row['DB_PRE_Early_AM_Peak']
                    # early_am_peak_diff=row['CR_Early_AM']-row['DB_Early_AM_Peak']
                    am_peak_diff=row['CR_AM_Peak']-row['DB_AM_Peak']
                    midday_diff=row['CR_Midday']-row['DB_Midday']    
                    pm_peak_diff=row['CR_PM_Peak']-row['DB_PM_Peak']
                    evening_diff=row['CR_Evening']-row['DB_Evening']
                    total_diff=row['CR_Total']-row['DB_Total']
            #         overall_difference=row['CR_Overall_Goal']-row['DB_Total']
                    # new_df.loc[index, 'PRE_Early_AM_DIFFERENCE'] = math.ceil(max(0, pre_early_am_peak_diff))
                    # new_df.loc[index, 'Early_AM_DIFFERENCE'] = math.ceil(max(0, early_am_peak_diff))
                    new_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
                    new_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
                    new_df.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
                    new_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
                    # route_level_df.loc[index, 'Total_DIFFERENCE'] = math.ceil(max(0, total_diff))
                    # new_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, pre_early_am_peak_diff))+math.ceil(max(0, early_am_peak_diff))+math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))
                    new_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))

        elif project=='VTA':
            pre_early_am_values=['AM1'] 
            early_am_values=['AM2'] 
            am_values=['AM3','AM4','MID1','MID2','MID7'] 
            midday_values=['MID3','MID4','MID5','MID6','PM1']
            pm_values=['PM2','PM3','PM4','PM5']
            evening_values=['PM6','PM7','PM8','PM9']
            pre_early_am_column=[0]  #0 is for Pre-Early AM header
            early_am_column=[1]  #1 is for Early AM header
            am_column=[2] #This is for AM header
            midday_colum=[3] #this is for MIDDAY header
            pm_column=[4] #this is for PM header
            evening_column=[5] #this is for EVENING header

            def convert_string_to_integer(x):
                try:
                    return float(x)
                except (ValueError, TypeError):
                    return 0
                
            # Creating new dataframe for specifically AM, PM, MIDDAY, Evenving data and added values from Compeletion Report
            new_df=pd.DataFrame()
            new_df['ROUTE_SURVEYEDCode']=overalldf['LS_NAME_CODE']
            new_df['CR_PRE_Early_AM']=overalldf[pre_early_am_column[0]].apply(math.ceil)
            new_df['CR_Early_AM']=overalldf[early_am_column[0]].apply(math.ceil)
            new_df['CR_AM_Peak']=overalldf[am_column[0]].apply(math.ceil)
            new_df['CR_Midday']=overalldf[midday_colum[0]].apply(math.ceil)
            new_df['CR_PM_Peak']=overalldf[pm_column[0]].apply(math.ceil)
            new_df['CR_Evening']=overalldf[evening_column[0]].apply(math.ceil)
            # print("new_df_columns",new_df.columns)
            new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
            new_df.fillna(0,inplace=True)

            for index, row in new_df.iterrows():
                route_code = row['ROUTE_SURVEYEDCode']

                def get_counts_and_ids(time_values):
                    # Just for SALEM
                    # subset_df = df[(df['ROUTE_SURVEYEDCode_Splited'] == route_code) & (df[time_column[0]].isin(time_values))]
                    subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code) & (df[time_column[0]].isin(time_values))]
                    subset_df=subset_df.drop_duplicates(subset='id')
                    count = subset_df.shape[0]
                    ids = subset_df['id'].values
                    return count, ids

                pre_early_am_value, pre_early_am_value_ids = get_counts_and_ids(pre_early_am_values)
                early_am_value, early_am_value_ids = get_counts_and_ids(early_am_values)
                am_value, am_value_ids = get_counts_and_ids(am_values)
                midday_value, midday_value_ids = get_counts_and_ids(midday_values)
                pm_value, pm_value_ids = get_counts_and_ids(pm_values)
                evening_value, evening_value_ids = get_counts_and_ids(evening_values)
                
                new_df.loc[index, 'CR_Total'] = row['CR_PRE_Early_AM']+row['CR_Early_AM']+row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
                new_df.loc[index, 'CR_AM_Peak'] =row['CR_AM_Peak']
                # new_df.loc[index, 'CR_AM_Peak'] =row['CR_PRE_EARLY_AM']+row['CR_EARLY_AM']+ row['CR_AM_Peak']
                new_df.loc[index, 'DB_PRE_Early_AM_Peak'] = pre_early_am_value
                new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_value
                new_df.loc[index, 'DB_AM_Peak'] = am_value
                new_df.loc[index, 'DB_Midday'] = midday_value
                new_df.loc[index, 'DB_PM_Peak'] = pm_value
                new_df.loc[index, 'DB_Evening'] = evening_value
                new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value+pre_early_am_value+early_am_value
                
            #     new_df['ROUTE_SURVEYEDCode_Splited']=new_df['ROUTE_SURVEYEDCode'].apply(lambda x:('_').join(x.split('_')[:-1]) )
                route_code_level_df=pd.DataFrame()

                unique_routes=new_df['ROUTE_SURVEYEDCode'].unique()

                route_code_level_df['ROUTE_SURVEYEDCode']=unique_routes

                # weekend_df.rename(columns={'ROUTE_TOTAL':'CR_Overall_Goal','SURVEY_ROUTE_CODE':'ROUTE_SURVEYEDCode','LS_NAME_CODE':'ROUTE_SURVEYEDCode'},inplace=True)

                for index, row in new_df.iterrows():
                    pre_early_am_peak_diff=row['CR_PRE_Early_AM']-row['DB_PRE_Early_AM_Peak']
                    early_am_peak_diff=row['CR_Early_AM']-row['DB_Early_AM_Peak']
                    am_peak_diff=row['CR_AM_Peak']-row['DB_AM_Peak']
                    midday_diff=row['CR_Midday']-row['DB_Midday']    
                    pm_peak_diff=row['CR_PM_Peak']-row['DB_PM_Peak']
                    evening_diff=row['CR_Evening']-row['DB_Evening']
                    total_diff=row['CR_Total']-row['DB_Total']
            #         overall_difference=row['CR_Overall_Goal']-row['DB_Total']
                    new_df.loc[index, 'PRE_Early_AM_DIFFERENCE'] = math.ceil(max(0, pre_early_am_peak_diff))
                    new_df.loc[index, 'Early_AM_DIFFERENCE'] = math.ceil(max(0, early_am_peak_diff))
                    new_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
                    new_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
                    new_df.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
                    new_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
                    # route_level_df.loc[index, 'Total_DIFFERENCE'] = math.ceil(max(0, total_diff))
                    new_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, pre_early_am_peak_diff))+math.ceil(max(0, early_am_peak_diff))+math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))


        elif project=='UTA':
            pre_early_am_values = [1]
            early_am_values = [2]
            am_values = [3, 4, 5, 6]
            midday_values = [7, 8, 9, 10, 11]
            pm_values = [12, 13, 14]
            evening_values = [15, 16, 17, 18]

            pre_early_am_column = [0]  # 0 is for Pre-Early AM header
            early_am_column = [1]  # 1 is for Early AM header
            am_column = [2]  # This is for AM header
            midday_column = [3]  # this is for MIDDAY header
            pm_column = [4]  # this is for PM header
            evening_column = [5]  # this is for EVENING header

            def convert_string_to_integer(x):
                try:
                    return float(x)
                except (ValueError, TypeError):
                    return 0

            # Creating new dataframe for specifically AM, PM, MIDDAY, Evening data and added values from Completion Report
            new_df = pd.DataFrame()
            new_df['ROUTE_SURVEYEDCode']=overalldf['LS_NAME_CODE']
            new_df['STATION_ID']=overalldf['STATION_ID']
            new_df['STATION_ID_SPLITTED']=overalldf['STATION_ID_SPLITTED']
            new_df['CR_PRE_Early_AM'] = pd.to_numeric(overalldf[pre_early_am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
            new_df['CR_Early_AM'] = pd.to_numeric(overalldf[early_am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
            new_df['CR_AM_Peak'] = pd.to_numeric(overalldf[am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
            new_df['CR_Midday'] = pd.to_numeric(overalldf[midday_column[0]], errors='coerce').fillna(0).apply(math.ceil)
            new_df['CR_PM_Peak'] = pd.to_numeric(overalldf[pm_column[0]], errors='coerce').fillna(0).apply(math.ceil)
            new_df['CR_Evening'] = pd.to_numeric(overalldf[evening_column[0]], errors='coerce').fillna(0).apply(math.ceil)
            # print("new_df_columns",new_df.columns)
            new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
            new_df.fillna(0,inplace=True)
        #     new code added for merging the same ROUTE_SURVEYEDCode
            # new_df=new_df.groupby('ROUTE_SURVEYEDCode', as_index=False).sum()
            # new_df.reset_index(drop=True, inplace=True)

            for index, row in new_df.iterrows():
                route_code = row['ROUTE_SURVEYEDCode']
                station_id=row['STATION_ID_SPLITTED']

                def get_counts_and_ids(time_values):
                    # Just for SALEM
                    # subset_df = df[(df['ROUTE_SURVEYEDCode_Splited'] == route_code) & (df[time_column[0]].isin(time_values))]
                    subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code)&(df['STATION_ID_SPLITTED']==station_id)  & (df[time_column[0]].isin(time_values))]
                    subset_df=subset_df.drop_duplicates(subset='id')
                    count = subset_df.shape[0]
                    ids = subset_df['id'].values
                    return count, ids

                pre_early_am_value, pre_early_am_value_ids = get_counts_and_ids(pre_early_am_values)
                early_am_value, early_am_value_ids = get_counts_and_ids(early_am_values)
                am_value, am_value_ids = get_counts_and_ids(am_values)
                midday_value, midday_value_ids = get_counts_and_ids(midday_values)
                pm_value, pm_value_ids = get_counts_and_ids(pm_values)
                evening_value, evening_value_ids = get_counts_and_ids(evening_values)
                
                new_df.loc[index, 'CR_Total'] = row['CR_PRE_Early_AM']+row['CR_Early_AM']+row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
                new_df.loc[index, 'CR_AM_Peak'] =row['CR_AM_Peak']
                # new_df.loc[index, 'CR_AM_Peak'] =row['CR_PRE_EARLY_AM']+row['CR_EARLY_AM']+ row['CR_AM_Peak']
                new_df.loc[index, 'DB_PRE_Early_AM_Peak'] = pre_early_am_value
                new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_value
                new_df.loc[index, 'DB_AM_Peak'] = am_value
                new_df.loc[index, 'DB_Midday'] = midday_value
                new_df.loc[index, 'DB_PM_Peak'] = pm_value
                new_df.loc[index, 'DB_Evening'] = evening_value
                new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value+pre_early_am_value+early_am_value
                
            #     new_df['ROUTE_SURVEYEDCode_Splited']=new_df['ROUTE_SURVEYEDCode'].apply(lambda x:('_').join(x.split('_')[:-1]) )
                route_code_level_df=pd.DataFrame()

                unique_routes=new_df['ROUTE_SURVEYEDCode'].unique()

                route_code_level_df['ROUTE_SURVEYEDCode']=unique_routes

                # weekend_df.rename(columns={'ROUTE_TOTAL':'CR_Overall_Goal','SURVEY_ROUTE_CODE':'ROUTE_SURVEYEDCode','LS_NAME_CODE':'ROUTE_SURVEYEDCode'},inplace=True)

                for index, row in new_df.iterrows():
                    pre_early_am_peak_diff=row['CR_PRE_Early_AM']-row['DB_PRE_Early_AM_Peak']
                    early_am_peak_diff=row['CR_Early_AM']-row['DB_Early_AM_Peak']
                    am_peak_diff=row['CR_AM_Peak']-row['DB_AM_Peak']
                    midday_diff=row['CR_Midday']-row['DB_Midday']    
                    pm_peak_diff=row['CR_PM_Peak']-row['DB_PM_Peak']
                    evening_diff=row['CR_Evening']-row['DB_Evening']
                    total_diff=row['CR_Total']-row['DB_Total']
            #         overall_difference=row['CR_Overall_Goal']-row['DB_Total']
                    new_df.loc[index, 'PRE_Early_AM_DIFFERENCE'] = math.ceil(max(0, pre_early_am_peak_diff))
                    new_df.loc[index, 'Early_AM_DIFFERENCE'] = math.ceil(max(0, early_am_peak_diff))
                    new_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
                    new_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
                    new_df.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
                    new_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
                    # route_level_df.loc[index, 'Total_DIFFERENCE'] = math.ceil(max(0, total_diff))
                    new_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, pre_early_am_peak_diff))+math.ceil(max(0, early_am_peak_diff))+math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))            
        return new_df

    wkend_route_direction_df=create_route_direction_level_df(wkend_overall_df,weekend_df,project)
    wkday_route_direction_df=create_route_direction_level_df(wkday_overall_df,weekday_df,project)

    if project=='UTA':
                
        def create_station_wise_route_level_df(overall_df,df):
            pre_early_am_values = [1]
            early_am_values = [2]
            am_values = [3, 4, 5, 6]
            midday_values = [7, 8, 9, 10, 11]
            pm_values = [12, 13, 14]
            evening_values = [15, 16, 17, 18]

            pre_early_am_column=[0]  #0 is for Pre-Early AM header
            early_am_column=[1]  #1 is for Early AM header
            am_column=[2] #This is for AM header
            midday_column=[3] #this is for MIDDAY header
            pm_column=[4] #this is for PM header
            evening_column=[5] #this is for EVENING header

            def convert_string_to_integer(x):
                try:
                    return float(x)
                except (ValueError, TypeError):
                    return 0

            # Creating new dataframe for specifically AM, PM, MIDDAY, Evenving data and added values from Compeletion Report
            new_df=pd.DataFrame()
            new_df['ROUTE_SURVEYEDCode']=overall_df['LS_NAME_CODE']
            new_df['STATION_ID']=overall_df['STATION_ID']
            new_df['STATION_ID_SPLITTED']=overall_df['STATION_ID_SPLITTED']
            new_df['CR_PRE_Early_AM'] = pd.to_numeric(overall_df[pre_early_am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
            new_df['CR_Early_AM'] = pd.to_numeric(overall_df[early_am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
            new_df['CR_AM_Peak'] = pd.to_numeric(overall_df[am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
            new_df['CR_Midday'] = pd.to_numeric(overall_df[midday_column[0]], errors='coerce').fillna(0).apply(math.ceil)
            new_df['CR_PM_Peak'] = pd.to_numeric(overall_df[pm_column[0]], errors='coerce').fillna(0).apply(math.ceil)
            new_df['CR_Evening'] = pd.to_numeric(overall_df[evening_column[0]], errors='coerce').fillna(0).apply(math.ceil)
            new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
            new_df.fillna(0,inplace=True)
            new_df['ROUTE_SURVEYEDCode_Splitted']=new_df['ROUTE_SURVEYEDCode'].apply(edit_ls_code_column)
            
            for index, row in new_df.iterrows():
                route_code = row['ROUTE_SURVEYEDCode_Splitted']
                station_id=row['STATION_ID_SPLITTED']
                def get_counts_and_ids(time_values):
                    # Just for SALEM
                    # subset_df = df[(df['ROUTE_SURVEYEDCode_Splited'] == route_code) & (df[time_column[0]].isin(time_values))]
                    subset_df = df[(df['ROUTE_SURVEYEDCode_Splited'] == route_code)& (df['STATION_ID_SPLITTED']==station_id)& (df[time_column[0]].isin(time_values))]
                    subset_df=subset_df.drop_duplicates(subset='id')
                    count = subset_df.shape[0]
                    ids = subset_df['id'].values
                    return count, ids

                pre_early_am_value, pre_early_am_value_ids = get_counts_and_ids(pre_early_am_values)
                early_am_value, early_am_value_ids = get_counts_and_ids(early_am_values)
                am_value, am_value_ids = get_counts_and_ids(am_values)
                midday_value, midday_value_ids = get_counts_and_ids(midday_values)
                pm_value, pm_value_ids = get_counts_and_ids(pm_values)
                evening_value, evening_value_ids = get_counts_and_ids(evening_values)
            #     print(pre_early_am_value,early_am_value,am_value,midday_value,pm_value,evening_value)
                new_df.loc[index, 'CR_Total'] = row['CR_PRE_Early_AM']+row['CR_Early_AM']+row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
                new_df.loc[index, 'CR_AM_Peak'] =row['CR_AM_Peak']
                # new_df.loc[index, 'CR_AM_Peak'] =row['CR_PRE_EARLY_AM']+row['CR_EARLY_AM']+ row['CR_AM_Peak']
                new_df.loc[index, 'DB_PRE_Early_AM_Peak'] = pre_early_am_value
                new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_value
                new_df.loc[index, 'DB_AM_Peak'] = am_value
                new_df.loc[index, 'DB_Midday'] = midday_value
                new_df.loc[index, 'DB_PM_Peak'] = pm_value
                new_df.loc[index, 'DB_Evening'] = evening_value
                new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value+pre_early_am_value+early_am_value
                unique_station_ids=new_df['STATION_ID_SPLITTED'].unique()

            results = []

            # Iterate over unique station IDs
            for station_id in unique_station_ids:
                # Filter DataFrame for the current station ID
                station_df = new_df[new_df['STATION_ID_SPLITTED'] == station_id]
                
                # Iterate over unique ROUTE_SURVEYEDCode_Splitted for the current station ID
                for route_code in station_df['ROUTE_SURVEYEDCode_Splitted'].unique():
                    # Filter rows for the specific route and station
                    filtered_df = station_df[station_df['ROUTE_SURVEYEDCode_Splitted'] == route_code]
                    
                    # Sum numeric columns and convert to a single row
                    summed_row = filtered_df.sum(numeric_only=True).to_frame().T
                    
                    # Add key identifying columns
                    summed_row['ROUTE_SURVEYEDCode'] = station_df.iloc[0]['ROUTE_SURVEYEDCode']
                    summed_row['STATION_ID'] = station_df.iloc[0]['STATION_ID']
                    summed_row['STATION_ID_SPLITTED'] = station_id
                    summed_row['ROUTE_SURVEYEDCode_Splitted'] = route_code
                    
                    # Append the row to results
                    results.append(summed_row)

            # Concatenate all results into a new DataFrame
            route_station_wise = pd.concat(results, ignore_index=True)
            route_station_wise.drop(columns=['ROUTE_SURVEYEDCode_Splitted','STATION_ID_SPLITTED'],inplace=True)

            for index, row in route_station_wise.iterrows():
                pre_early_am_peak_diff=row['CR_PRE_Early_AM']-row['DB_PRE_Early_AM_Peak']
                early_am_peak_diff=row['CR_Early_AM']-row['DB_Early_AM_Peak']
                am_peak_diff=row['CR_AM_Peak']-row['DB_AM_Peak']
                midday_diff=row['CR_Midday']-row['DB_Midday']    
                pm_peak_diff=row['CR_PM_Peak']-row['DB_PM_Peak']
                evening_diff=row['CR_Evening']-row['DB_Evening']
                total_diff=row['CR_Total']-row['DB_Total']
        #         overall_difference=row['CR_Overall_Goal']-row['DB_Total']
                route_station_wise.loc[index, 'PRE_Early_AM_DIFFERENCE'] = math.ceil(max(0, pre_early_am_peak_diff))
                route_station_wise.loc[index, 'Early_AM_DIFFERENCE'] = math.ceil(max(0, early_am_peak_diff))
                route_station_wise.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
                route_station_wise.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
                route_station_wise.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
                route_station_wise.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
                # route_level_df.loc[index, 'Total_DIFFERENCE'] = math.ceil(max(0, total_diff))
                route_station_wise.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, pre_early_am_peak_diff))+math.ceil(max(0, early_am_peak_diff))+math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))

            return route_station_wise

        wkend_stationwise_route_df=create_station_wise_route_level_df(wkend_overall_df,weekend_df)
        wkday_stationwise_route_df=create_station_wise_route_level_df(wkday_overall_df,weekday_df)

    else:
        pass

    def create_route_level_df(overall_df,route_df,df,project):
        if project=='TUCSON':
            am_values = ['AM1','AM2','AM3', 'AM4']
            midday_values = [ 'MID1', 'MID2','MID3', 'MID4', 'MID5', 'MID6' ]
            pm_values = ['PM1','PM2', 'PM3']
            evening_values = ['OFF1', 'OFF2', 'OFF3', 'OFF4','OFF5']
            am_column=[1] #This is for AM header
            midday_colum=[2] #this is for MIDDAY header
            pm_column=[3] #this is for PM header
            evening_column=[4] #this is for EVENING header

            def convert_string_to_integer(x):
                try:
                    return float(x)
                except (ValueError, TypeError):
                    return 0

            # Creating new dataframe for specifically AM, PM, MIDDAY, Evenving data and added values from Compeletion Report
            new_df=pd.DataFrame()
            new_df['ROUTE_SURVEYEDCode']=overall_df['LS_NAME_CODE']
            # new_df['CR_PRE_Early_AM']=overall_df[pre_early_am_column[0]].apply(math.ceil)
            # new_df['CR_Early_AM']=overall_df[early_am_column[0]].apply(math.ceil)
            new_df['CR_AM_Peak']=overall_df[am_column[0]].apply(math.ceil)
            new_df['CR_Midday']=overall_df[midday_colum[0]].apply(math.ceil)
            new_df['CR_PM_Peak']=overall_df[pm_column[0]].apply(math.ceil)
            new_df['CR_Evening']=overall_df[evening_column[0]].apply(math.ceil)
            print("new_df_columns",new_df.columns)
            # new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
            new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
            #  new_df[['CR_EARLY_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_EARLY_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
            # new_df['Overall Goal']=cr_df[overall_goal_column[0]]
            new_df.fillna(0,inplace=True)
            # adding values for AM, PM, MIDDAY and Evening from Database file to new Dataframe
            for index, row in new_df.iterrows():
                print("In loop 899")
                route_code = row['ROUTE_SURVEYEDCode']

                # Define a function to get the counts and IDs
                def get_counts_and_ids(time_values):
                    print("In get_counts_and_ids")
                    # Just for SALEM
                    # subset_df = df[(df['ROUTE_SURVEYEDCode_Splited'] == route_code) & (df[time_column[0]].isin(time_values))]
                    subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code) & (df[time_column[0]].isin(time_values))]
                    subset_df=subset_df.drop_duplicates(subset='id')
                    count = subset_df.shape[0]
                    ids = subset_df['id'].values
                    return count, ids
                
                # Calculate counts and IDs for each time slot
                # pre_early_am_value, pre_early_am_value_ids = get_counts_and_ids(pre_early_am_values)
                # early_am_value, early_am_value_ids = get_counts_and_ids(early_am_values)
                am_value, am_value_ids = get_counts_and_ids(am_values)
                midday_value, midday_value_ids = get_counts_and_ids(midday_values)
                pm_value, pm_value_ids = get_counts_and_ids(pm_values)
                evening_value, evening_value_ids = get_counts_and_ids(evening_values)
                
                # Assign values to new_df
                # new_df.loc[index, 'CR_Total'] = row['CR_EARLY_AM'] + row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
                # new_df.loc[index, 'CR_Total'] = row['CR_PRE_Early_AM']+row['CR_Early_AM']+row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
                new_df.loc[index, 'CR_Total'] = row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
                new_df.loc[index, 'CR_AM_Peak'] =row['CR_AM_Peak']
                # new_df.loc[index, 'CR_AM_Peak'] =row['CR_PRE_EARLY_AM']+row['CR_EARLY_AM']+ row['CR_AM_Peak']
                # new_df.loc[index, 'DB_PRE_Early_AM_Peak'] = pre_early_am_value
                # new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_value
                new_df.loc[index, 'DB_AM_Peak'] = am_value
                new_df.loc[index, 'DB_Midday'] = midday_value
                new_df.loc[index, 'DB_PM_Peak'] = pm_value
                new_df.loc[index, 'DB_Evening'] = evening_value
                # new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value+pre_early_am_value+early_am_value
                new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value
                
                # Join the IDs as a comma-separated string
                # new_df.loc[index, 'DB_PRE_Early_AM_IDS'] = ', '.join(map(str, pre_early_am_value_ids))
                # new_df.loc[index, 'DB_Early_AM_IDS'] = ', '.join(map(str, early_am_value_ids))
                new_df.loc[index, 'DB_AM_IDS'] = ', '.join(map(str, am_value_ids))
                new_df.loc[index, 'DB_Midday_IDS'] = ', '.join(map(str, midday_value_ids))
                new_df.loc[index, 'DB_PM_IDS'] = ', '.join(map(str, pm_value_ids))
                new_df.loc[index, 'DB_Evening_IDS'] = ', '.join(map(str, evening_value_ids))

            # new_df.to_csv('Time Base Comparison(Over All).csv',index=False)

            # Route Level Comparison
            # Just for SALEM because in SALEM Code values are already splitted
            # new_df['ROUTE_SURVEYEDCode_Splited']=new_df['ROUTE_SURVEYEDCode']
            new_df['ROUTE_SURVEYEDCode_Splited']=new_df['ROUTE_SURVEYEDCode'].apply(lambda x:('_').join(x.split('_')[:-1]) )

            # creating new dataframe for ROUTE_LEVEL_Comparison
            route_level_df=pd.DataFrame()

            unique_routes=new_df['ROUTE_SURVEYEDCode_Splited'].unique()

            route_level_df['ROUTE_SURVEYEDCode']=unique_routes

            route_df.rename(columns={'ROUTE_TOTAL':'CR_Overall_Goal','SURVEY_ROUTE_CODE':'ROUTE_SURVEYEDCode','LS_NAME_CODE':'ROUTE_SURVEYEDCode'},inplace=True)

            route_df.dropna(subset=['ROUTE_SURVEYEDCode'],inplace=True)
            route_level_df=pd.merge(route_level_df,route_df[['ROUTE_SURVEYEDCode','CR_Overall_Goal']],on='ROUTE_SURVEYEDCode')

            # adding values from database file and compeletion report for Route_Level
            for index , row in route_level_df.iterrows():
                print("In loop 965")
                subset_df=new_df[new_df['ROUTE_SURVEYEDCode_Splited']==row['ROUTE_SURVEYEDCode']]
                # sum_per_route_cr = subset_df[['CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total','Overall Goal']].sum()
                


                # sum_per_route_cr = subset_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total']].sum()
                sum_per_route_cr = subset_df[['CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total']].sum()
                # sum_per_route_cr = subset_df[['CR_EARLY_AM','CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total']].sum()
                # sum_per_route_db = subset_df[['DB_PRE_Early_AM_Peak','DB_Early_AM_Peak','DB_AM_Peak', 'DB_Midday', 'DB_PM_Peak', 'DB_Evening','DB_Total']].sum()
                sum_per_route_db = subset_df[['DB_AM_Peak', 'DB_Midday', 'DB_PM_Peak', 'DB_Evening','DB_Total']].sum()
                
                # route_level_df.loc[index,'CR_PRE_Early_AM']=sum_per_route_cr['CR_PRE_Early_AM']
                # route_level_df.loc[index,'CR_Early_AM']=sum_per_route_cr['CR_Early_AM']
                route_level_df.loc[index,'CR_AM_Peak']=sum_per_route_cr['CR_AM_Peak']
                route_level_df.loc[index,'CR_Midday']=sum_per_route_cr['CR_Midday']
                route_level_df.loc[index,'CR_PM_Peak']=sum_per_route_cr['CR_PM_Peak']
                route_level_df.loc[index,'CR_Evening']=sum_per_route_cr['CR_Evening']
                route_level_df.loc[index,'CR_Total']=sum_per_route_cr['CR_Total']
                # route_level_df.loc[index,'CR_Overall_Goal']=sum_per_route_cr['Overall Goal']
                
                # route_level_df.loc[index,'DB_PRE_Early_AM_Peak']=sum_per_route_db['DB_PRE_Early_AM_Peak']
                # route_level_df.loc[index,'DB_Early_AM_Peak']=sum_per_route_db['DB_Early_AM_Peak']
                route_level_df.loc[index,'DB_AM_Peak']=sum_per_route_db['DB_AM_Peak']
                route_level_df.loc[index,'DB_Midday']=sum_per_route_db['DB_Midday']
                route_level_df.loc[index,'DB_PM_Peak']=sum_per_route_db['DB_PM_Peak']
                route_level_df.loc[index,'DB_Evening']=sum_per_route_db['DB_Evening']
                route_level_df.loc[index,'DB_Total']=sum_per_route_db['DB_Total']   
                # route_level_df.loc[index,'DB_PRE_Early_AM_IDS']=', '.join(str(value) for value in subset_df['DB_PRE_Early_AM_IDS'].values)    
                # route_level_df.loc[index,'DB_Early_AM_IDS']=', '.join(str(value) for value in subset_df['DB_Early_AM_IDS'].values)    
                route_level_df.loc[index,'DB_AM_IDS']=', '.join(str(value) for value in subset_df['DB_AM_IDS'].values)    
                route_level_df.loc[index,'DB_Midday_IDS']=', '.join(str(value) for value in subset_df['DB_Midday_IDS'].values)    
                route_level_df.loc[index,'DB_PM_IDS']=', '.join(str(value) for value in subset_df['DB_PM_IDS'].values)    
                route_level_df.loc[index,'DB_Evening_IDS']=', '.join(str(value) for value in subset_df['DB_Evening_IDS'].values)

            # route_level_df.to_csv('Route Level Comparison(Value_Check).csv',index=False)
                
            # calculating the difference between values of database and compeletion report for Route_Level
            for index, row in route_level_df.iterrows():
                print("In loop 1004")
                # pre_early_am_peak_diff=row['CR_PRE_Early_AM']-row['DB_PRE_Early_AM_Peak']
                # early_am_peak_diff=row['CR_Early_AM']-row['DB_Early_AM_Peak']
                am_peak_diff=row['CR_AM_Peak']-row['DB_AM_Peak']
                midday_diff=row['CR_Midday']-row['DB_Midday']    
                pm_peak_diff=row['CR_PM_Peak']-row['DB_PM_Peak']
                evening_diff=row['CR_Evening']-row['DB_Evening']
                total_diff=row['CR_Total']-row['DB_Total']
                overall_difference=row['CR_Overall_Goal']-row['DB_Total']
                # route_level_df.loc[index, 'PRE_Early_AM_DIFFERENCE'] = math.ceil(max(0, pre_early_am_peak_diff))
                # route_level_df.loc[index, 'Early_AM_DIFFERENCE'] = math.ceil(max(0, early_am_peak_diff))
                route_level_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
                route_level_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
                route_level_df.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
                route_level_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
                # route_level_df.loc[index, 'Total_DIFFERENCE'] = math.ceil(max(0, total_diff))
                # route_level_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, pre_early_am_peak_diff))+math.ceil(max(0, early_am_peak_diff))+math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))
                route_level_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))
                route_level_df.loc[index, 'Overall_Goal_DIFFERENCE'] = math.ceil(max(0,overall_difference))
        
        elif project=='VTA':
            pre_early_am_values=['AM1'] 
            early_am_values=['AM2'] 
            am_values=['AM3','AM4','MID1','MID2','MID7'] 
            midday_values=['MID3','MID4','MID5','MID6','PM1']
            pm_values=['PM2','PM3','PM4','PM5']
            evening_values=['PM6','PM7','PM8','PM9']

            pre_early_am_column=[0]  #0 is for Pre-Early AM header
            early_am_column=[1]  #1 is for Early AM header
            am_column=[2] #This is for AM header
            midday_colum=[3] #this is for MIDDAY header
            pm_column=[4] #this is for PM header
            evening_column=[5] #this is for EVENING header

            def convert_string_to_integer(x):
                try:
                    return float(x)
                except (ValueError, TypeError):
                    return 0

            # Creating new dataframe for specifically AM, PM, MIDDAY, Evenving data and added values from Compeletion Report
            new_df=pd.DataFrame()
            new_df['ROUTE_SURVEYEDCode']=overall_df['LS_NAME_CODE']
            new_df['CR_PRE_Early_AM']=overall_df[pre_early_am_column[0]].apply(math.ceil)
            new_df['CR_Early_AM']=overall_df[early_am_column[0]].apply(math.ceil)
            new_df['CR_AM_Peak']=overall_df[am_column[0]].apply(math.ceil)
            new_df['CR_Midday']=overall_df[midday_colum[0]].apply(math.ceil)
            new_df['CR_PM_Peak']=overall_df[pm_column[0]].apply(math.ceil)
            new_df['CR_Evening']=overall_df[evening_column[0]].apply(math.ceil)
            print("new_df_columns",new_df.columns)
            new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
            #  new_df[['CR_EARLY_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_EARLY_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
            # new_df['Overall Goal']=cr_df[overall_goal_column[0]]
            new_df.fillna(0,inplace=True)
            # adding values for AM, PM, MIDDAY and Evening from Database file to new Dataframe
            for index, row in new_df.iterrows():
                route_code = row['ROUTE_SURVEYEDCode']

                # Define a function to get the counts and IDs
                def get_counts_and_ids(time_values):
                    # Just for SALEM
                    # subset_df = df[(df['ROUTE_SURVEYEDCode_Splited'] == route_code) & (df[time_column[0]].isin(time_values))]
                    subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code) & (df[time_column[0]].isin(time_values))]
                    subset_df=subset_df.drop_duplicates(subset='id')
                    count = subset_df.shape[0]
                    ids = subset_df['id'].values
                    return count, ids
                
                # Calculate counts and IDs for each time slot
                pre_early_am_value, pre_early_am_value_ids = get_counts_and_ids(pre_early_am_values)
                early_am_value, early_am_value_ids = get_counts_and_ids(early_am_values)
                am_value, am_value_ids = get_counts_and_ids(am_values)
                midday_value, midday_value_ids = get_counts_and_ids(midday_values)
                pm_value, pm_value_ids = get_counts_and_ids(pm_values)
                evening_value, evening_value_ids = get_counts_and_ids(evening_values)
                
                # Assign values to new_df
                # new_df.loc[index, 'CR_Total'] = row['CR_EARLY_AM'] + row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
                new_df.loc[index, 'CR_Total'] = row['CR_PRE_Early_AM']+row['CR_Early_AM']+row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
                new_df.loc[index, 'CR_AM_Peak'] =row['CR_AM_Peak']
                # new_df.loc[index, 'CR_AM_Peak'] =row['CR_PRE_EARLY_AM']+row['CR_EARLY_AM']+ row['CR_AM_Peak']
                new_df.loc[index, 'DB_PRE_Early_AM_Peak'] = pre_early_am_value
                new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_value
                new_df.loc[index, 'DB_AM_Peak'] = am_value
                new_df.loc[index, 'DB_Midday'] = midday_value
                new_df.loc[index, 'DB_PM_Peak'] = pm_value
                new_df.loc[index, 'DB_Evening'] = evening_value
                new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value+pre_early_am_value+early_am_value
                
                # Join the IDs as a comma-separated string
                new_df.loc[index, 'DB_PRE_Early_AM_IDS'] = ', '.join(map(str, pre_early_am_value_ids))
                new_df.loc[index, 'DB_Early_AM_IDS'] = ', '.join(map(str, early_am_value_ids))
                new_df.loc[index, 'DB_AM_IDS'] = ', '.join(map(str, am_value_ids))
                new_df.loc[index, 'DB_Midday_IDS'] = ', '.join(map(str, midday_value_ids))
                new_df.loc[index, 'DB_PM_IDS'] = ', '.join(map(str, pm_value_ids))
                new_df.loc[index, 'DB_Evening_IDS'] = ', '.join(map(str, evening_value_ids))

            # new_df.to_csv('Time Base Comparison(Over All).csv',index=False)

            # Route Level Comparison
            # Just for SALEM because in SALEM Code values are already splitted
            # new_df['ROUTE_SURVEYEDCode_Splited']=new_df['ROUTE_SURVEYEDCode']
            new_df['ROUTE_SURVEYEDCode_Splited']=new_df['ROUTE_SURVEYEDCode'].apply(lambda x:('_').join(x.split('_')[:-1]) )

            # creating new dataframe for ROUTE_LEVEL_Comparison
            route_level_df=pd.DataFrame()

            unique_routes=new_df['ROUTE_SURVEYEDCode_Splited'].unique()

            route_level_df['ROUTE_SURVEYEDCode']=unique_routes

            route_df.rename(columns={'ROUTE_TOTAL':'CR_Overall_Goal','SURVEY_ROUTE_CODE':'ROUTE_SURVEYEDCode','LS_NAME_CODE':'ROUTE_SURVEYEDCode'},inplace=True)

            route_df.dropna(subset=['ROUTE_SURVEYEDCode'],inplace=True)
            route_level_df=pd.merge(route_level_df,route_df[['ROUTE_SURVEYEDCode','CR_Overall_Goal']],on='ROUTE_SURVEYEDCode')

            # adding values from database file and compeletion report for Route_Level
            for index , row in route_level_df.iterrows():
                subset_df=new_df[new_df['ROUTE_SURVEYEDCode_Splited']==row['ROUTE_SURVEYEDCode']]
                # sum_per_route_cr = subset_df[['CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total','Overall Goal']].sum()
                


                sum_per_route_cr = subset_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total']].sum()
                # sum_per_route_cr = subset_df[['CR_EARLY_AM','CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total']].sum()
                sum_per_route_db = subset_df[['DB_PRE_Early_AM_Peak','DB_Early_AM_Peak','DB_AM_Peak', 'DB_Midday', 'DB_PM_Peak', 'DB_Evening','DB_Total']].sum()
                
                route_level_df.loc[index,'CR_PRE_Early_AM']=sum_per_route_cr['CR_PRE_Early_AM']
                route_level_df.loc[index,'CR_Early_AM']=sum_per_route_cr['CR_Early_AM']
                route_level_df.loc[index,'CR_AM_Peak']=sum_per_route_cr['CR_AM_Peak']
                route_level_df.loc[index,'CR_Midday']=sum_per_route_cr['CR_Midday']
                route_level_df.loc[index,'CR_PM_Peak']=sum_per_route_cr['CR_PM_Peak']
                route_level_df.loc[index,'CR_Evening']=sum_per_route_cr['CR_Evening']
                route_level_df.loc[index,'CR_Total']=sum_per_route_cr['CR_Total']
                # route_level_df.loc[index,'CR_Overall_Goal']=sum_per_route_cr['Overall Goal']
                
                route_level_df.loc[index,'DB_PRE_Early_AM_Peak']=sum_per_route_db['DB_PRE_Early_AM_Peak']
                route_level_df.loc[index,'DB_Early_AM_Peak']=sum_per_route_db['DB_Early_AM_Peak']
                route_level_df.loc[index,'DB_AM_Peak']=sum_per_route_db['DB_AM_Peak']
                route_level_df.loc[index,'DB_Midday']=sum_per_route_db['DB_Midday']
                route_level_df.loc[index,'DB_PM_Peak']=sum_per_route_db['DB_PM_Peak']
                route_level_df.loc[index,'DB_Evening']=sum_per_route_db['DB_Evening']
                route_level_df.loc[index,'DB_Total']=sum_per_route_db['DB_Total']   
                route_level_df.loc[index,'DB_PRE_Early_AM_IDS']=', '.join(str(value) for value in subset_df['DB_PRE_Early_AM_IDS'].values)    
                route_level_df.loc[index,'DB_Early_AM_IDS']=', '.join(str(value) for value in subset_df['DB_Early_AM_IDS'].values)    
                route_level_df.loc[index,'DB_AM_IDS']=', '.join(str(value) for value in subset_df['DB_AM_IDS'].values)    
                route_level_df.loc[index,'DB_Midday_IDS']=', '.join(str(value) for value in subset_df['DB_Midday_IDS'].values)    
                route_level_df.loc[index,'DB_PM_IDS']=', '.join(str(value) for value in subset_df['DB_PM_IDS'].values)    
                route_level_df.loc[index,'DB_Evening_IDS']=', '.join(str(value) for value in subset_df['DB_Evening_IDS'].values)

            # route_level_df.to_csv('Route Level Comparison(Value_Check).csv',index=False)
                
            # calculating the difference between values of database and compeletion report for Route_Level
            for index, row in route_level_df.iterrows():
                pre_early_am_peak_diff=row['CR_PRE_Early_AM']-row['DB_PRE_Early_AM_Peak']
                early_am_peak_diff=row['CR_Early_AM']-row['DB_Early_AM_Peak']
                am_peak_diff=row['CR_AM_Peak']-row['DB_AM_Peak']
                midday_diff=row['CR_Midday']-row['DB_Midday']    
                pm_peak_diff=row['CR_PM_Peak']-row['DB_PM_Peak']
                evening_diff=row['CR_Evening']-row['DB_Evening']
                total_diff=row['CR_Total']-row['DB_Total']
                overall_difference=row['CR_Overall_Goal']-row['DB_Total']
                route_level_df.loc[index, 'PRE_Early_AM_DIFFERENCE'] = math.ceil(max(0, pre_early_am_peak_diff))
                route_level_df.loc[index, 'Early_AM_DIFFERENCE'] = math.ceil(max(0, early_am_peak_diff))
                route_level_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
                route_level_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
                route_level_df.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
                route_level_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
                # route_level_df.loc[index, 'Total_DIFFERENCE'] = math.ceil(max(0, total_diff))
                route_level_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, pre_early_am_peak_diff))+math.ceil(max(0, early_am_peak_diff))+math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))
                route_level_df.loc[index, 'Overall_Goal_DIFFERENCE'] = math.ceil(max(0,overall_difference))
                
        elif project=='UTA':
            pre_early_am_values = [1]
            early_am_values = [2]
            am_values = [3, 4, 5, 6]
            midday_values = [7, 8, 9, 10, 11]
            pm_values = [12, 13, 14]
            evening_values = [15, 16, 17, 18]

            pre_early_am_column=[0]  #0 is for Pre-Early AM header
            early_am_column=[1]  #1 is for Early AM header
            am_column=[2] #This is for AM header
            midday_column=[3] #this is for MIDDAY header
            pm_column=[4] #this is for PM header
            evening_column=[5] #this is for EVENING header

            def convert_string_to_integer(x):
                try:
                    return float(x)
                except (ValueError, TypeError):
                    return 0

            # Creating new dataframe for specifically AM, PM, MIDDAY, Evenving data and added values from Compeletion Report
            new_df=pd.DataFrame()
            new_df['ROUTE_SURVEYEDCode']=overall_df['LS_NAME_CODE']
            new_df['CR_PRE_Early_AM'] = pd.to_numeric(overall_df[pre_early_am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
            new_df['CR_Early_AM'] = pd.to_numeric(overall_df[early_am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
            new_df['CR_AM_Peak'] = pd.to_numeric(overall_df[am_column[0]], errors='coerce').fillna(0).apply(math.ceil)
            new_df['CR_Midday'] = pd.to_numeric(overall_df[midday_column[0]], errors='coerce').fillna(0).apply(math.ceil)
            new_df['CR_PM_Peak'] = pd.to_numeric(overall_df[pm_column[0]], errors='coerce').fillna(0).apply(math.ceil)
            new_df['CR_Evening'] = pd.to_numeric(overall_df[evening_column[0]], errors='coerce').fillna(0).apply(math.ceil)
            new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']]=new_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak','CR_Midday','CR_PM_Peak','CR_Evening']].applymap(convert_string_to_integer)
            new_df.fillna(0,inplace=True)
            #  new code added for merging the same ROUTE_SURVEYEDCode 
            new_df = new_df.groupby('ROUTE_SURVEYEDCode', as_index=False).sum()
            new_df.reset_index(drop=True, inplace=True)

            # adding values for AM, PM, MIDDAY and Evening from Database file to new Dataframe
            for index, row in new_df.iterrows():
                route_code = row['ROUTE_SURVEYEDCode']

                def get_counts_and_ids(time_values):
                    # Just for SALEM
                    # subset_df = df[(df['ROUTE_SURVEYEDCode_Splited'] == route_code) & (df[time_column[0]].isin(time_values))]
                    subset_df = df[(df['ROUTE_SURVEYEDCode'] == route_code) & (df[time_column[0]].isin(time_values))]
                    subset_df=subset_df.drop_duplicates(subset='id')
                    count = subset_df.shape[0]
                    ids = subset_df['id'].values
                    return count, ids

                pre_early_am_value, pre_early_am_value_ids = get_counts_and_ids(pre_early_am_values)
                early_am_value, early_am_value_ids = get_counts_and_ids(early_am_values)
                am_value, am_value_ids = get_counts_and_ids(am_values)
                midday_value, midday_value_ids = get_counts_and_ids(midday_values)
                pm_value, pm_value_ids = get_counts_and_ids(pm_values)
                evening_value, evening_value_ids = get_counts_and_ids(evening_values)
            #     print(pre_early_am_value,early_am_value,am_value,midday_value,pm_value,evening_value)
                new_df.loc[index, 'CR_Total'] = row['CR_PRE_Early_AM']+row['CR_Early_AM']+row['CR_AM_Peak'] + row['CR_Midday'] + row['CR_PM_Peak'] + row['CR_Evening']
                new_df.loc[index, 'CR_AM_Peak'] =row['CR_AM_Peak']
                # new_df.loc[index, 'CR_AM_Peak'] =row['CR_PRE_EARLY_AM']+row['CR_EARLY_AM']+ row['CR_AM_Peak']
                new_df.loc[index, 'DB_PRE_Early_AM_Peak'] = pre_early_am_value
                new_df.loc[index, 'DB_Early_AM_Peak'] = early_am_value
                new_df.loc[index, 'DB_AM_Peak'] = am_value
                new_df.loc[index, 'DB_Midday'] = midday_value
                new_df.loc[index, 'DB_PM_Peak'] = pm_value
                new_df.loc[index, 'DB_Evening'] = evening_value
                new_df.loc[index, 'DB_Total'] = evening_value + am_value + midday_value + pm_value+pre_early_am_value+early_am_value

                
                # # Join the IDs as a comma-separated string
                new_df.loc[index, 'DB_PRE_Early_AM_IDS'] = ', '.join(map(str, pre_early_am_value_ids))
                new_df.loc[index, 'DB_Early_AM_IDS'] = ', '.join(map(str, early_am_value_ids))
                new_df.loc[index, 'DB_AM_IDS'] = ', '.join(map(str, am_value_ids))
                new_df.loc[index, 'DB_Midday_IDS'] = ', '.join(map(str, midday_value_ids))
                new_df.loc[index, 'DB_PM_IDS'] = ', '.join(map(str, pm_value_ids))
                new_df.loc[index, 'DB_Evening_IDS'] = ', '.join(map(str, evening_value_ids))

            # new_df.to_csv('Time Base Comparison(Over All).csv',index=False)

            # Route Level Comparison
            # Just for SALEM because in SALEM Code values are already splitted
            # new_df['ROUTE_SURVEYEDCode_Splited']=new_df['ROUTE_SURVEYEDCode']
            new_df['ROUTE_SURVEYEDCode_Splited']=new_df['ROUTE_SURVEYEDCode'].apply(lambda x:('_').join(x.split('_')[:-1]) )

            # creating new dataframe for ROUTE_LEVEL_Comparison
            route_level_df=pd.DataFrame()

            unique_routes=new_df['ROUTE_SURVEYEDCode_Splited'].unique()

            route_level_df['ROUTE_SURVEYEDCode']=unique_routes

            # Have to change the name accordingly
            route_df.rename(columns={'ROUTE_TOTAL':'CR_Overall_Goal','ETC_ROUTE_ID':'ROUTE_SURVEYEDCode'},inplace=True)

            route_df.dropna(subset=['ROUTE_SURVEYEDCode'],inplace=True)
            route_level_df=pd.merge(route_level_df,route_df[['ROUTE_SURVEYEDCode','CR_Overall_Goal']],on='ROUTE_SURVEYEDCode')

            route_level_df=route_level_df.groupby('ROUTE_SURVEYEDCode', as_index=False).sum()
            route_level_df.reset_index(drop=True, inplace=True)

            # adding values from database file and compeletion report for Route_Level
            for index , row in route_level_df.iterrows():
                subset_df=new_df[new_df['ROUTE_SURVEYEDCode_Splited']==row['ROUTE_SURVEYEDCode']]
                # sum_per_route_cr = subset_df[['CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total','Overall Goal']].sum()



                sum_per_route_cr = subset_df[['CR_PRE_Early_AM','CR_Early_AM','CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total']].sum()
                # sum_per_route_cr = subset_df[['CR_EARLY_AM','CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening','CR_Total']].sum()
                sum_per_route_db = subset_df[['DB_PRE_Early_AM_Peak','DB_Early_AM_Peak','DB_AM_Peak', 'DB_Midday', 'DB_PM_Peak', 'DB_Evening','DB_Total']].sum()

                route_level_df.loc[index,'CR_PRE_Early_AM']=sum_per_route_cr['CR_PRE_Early_AM']
                route_level_df.loc[index,'CR_Early_AM']=sum_per_route_cr['CR_Early_AM']
                route_level_df.loc[index,'CR_AM_Peak']=sum_per_route_cr['CR_AM_Peak']
                route_level_df.loc[index,'CR_Midday']=sum_per_route_cr['CR_Midday']
                route_level_df.loc[index,'CR_PM_Peak']=sum_per_route_cr['CR_PM_Peak']
                route_level_df.loc[index,'CR_Evening']=sum_per_route_cr['CR_Evening']
                route_level_df.loc[index,'CR_Total']=sum_per_route_cr['CR_Total']
                
                route_level_df.loc[index,'DB_PRE_Early_AM_Peak']=sum_per_route_db['DB_PRE_Early_AM_Peak']
                route_level_df.loc[index,'DB_Early_AM_Peak']=sum_per_route_db['DB_Early_AM_Peak']
                route_level_df.loc[index,'DB_AM_Peak']=sum_per_route_db['DB_AM_Peak']
                route_level_df.loc[index,'DB_Midday']=sum_per_route_db['DB_Midday']
                route_level_df.loc[index,'DB_PM_Peak']=sum_per_route_db['DB_PM_Peak']
                route_level_df.loc[index,'DB_Evening']=sum_per_route_db['DB_Evening']
                route_level_df.loc[index,'DB_Total']=sum_per_route_db['DB_Total']   
                route_level_df.loc[index,'DB_PRE_Early_AM_IDS']=', '.join(str(value) for value in subset_df['DB_PRE_Early_AM_IDS'].values)    
                route_level_df.loc[index,'DB_Early_AM_IDS']=', '.join(str(value) for value in subset_df['DB_Early_AM_IDS'].values)    
                route_level_df.loc[index,'DB_AM_IDS']=', '.join(str(value) for value in subset_df['DB_AM_IDS'].values)    
                route_level_df.loc[index,'DB_Midday_IDS']=', '.join(str(value) for value in subset_df['DB_Midday_IDS'].values)    
                route_level_df.loc[index,'DB_PM_IDS']=', '.join(str(value) for value in subset_df['DB_PM_IDS'].values)    
                route_level_df.loc[index,'DB_Evening_IDS']=', '.join(str(value) for value in subset_df['DB_Evening_IDS'].values)

            # route_level_df.to_csv('Route Level Comparison(Value_Check).csv',index=False)
                
            # calculating the difference between values of database and compeletion report for Route_Level
            for index, row in route_level_df.iterrows():
                pre_early_am_peak_diff=row['CR_PRE_Early_AM']-row['DB_PRE_Early_AM_Peak']
                early_am_peak_diff=row['CR_Early_AM']-row['DB_Early_AM_Peak']
                am_peak_diff=row['CR_AM_Peak']-row['DB_AM_Peak']
                midday_diff=row['CR_Midday']-row['DB_Midday']    
                pm_peak_diff=row['CR_PM_Peak']-row['DB_PM_Peak']
                evening_diff=row['CR_Evening']-row['DB_Evening']
                total_diff=row['CR_Total']-row['DB_Total']
                overall_difference=row['CR_Overall_Goal']-row['DB_Total']
                route_level_df.loc[index, 'PRE_Early_AM_DIFFERENCE'] = math.ceil(max(0, pre_early_am_peak_diff))
                route_level_df.loc[index, 'Early_AM_DIFFERENCE'] = math.ceil(max(0, early_am_peak_diff))
                route_level_df.loc[index, 'AM_DIFFERENCE'] = math.ceil(max(0, am_peak_diff))
                route_level_df.loc[index, 'Midday_DIFFERENCE'] = math.ceil(max(0, midday_diff))
                route_level_df.loc[index, 'PM_DIFFERENCE'] = math.ceil(max(0, pm_peak_diff))
                route_level_df.loc[index, 'Evening_DIFFERENCE'] = math.ceil(max(0, evening_diff))
                # route_level_df.loc[index, 'Total_DIFFERENCE'] = math.ceil(max(0, total_diff))
                route_level_df.loc[index, 'Total_DIFFERENCE'] =math.ceil(max(0, pre_early_am_peak_diff))+math.ceil(max(0, early_am_peak_diff))+math.ceil(max(0, am_peak_diff))+math.ceil(max(0, midday_diff))+math.ceil(max(0, pm_peak_diff))+math.ceil(max(0, evening_diff))
                route_level_df.loc[index, 'Overall_Goal_DIFFERENCE'] = math.ceil(max(0,overall_difference))
        return route_level_df


    weekday_df.dropna(subset=[time_column[0]],inplace=True)
    weekday_raw_df=weekday_df[['id', 'Completed', route_survey_column[0],'ROUTE_SURVEYED',stopon_clntid_column[0],stopoff_clntid_column[0],time_column[0],time_period_column[0],'Day','ELVIS_STATUS']]
    weekend_df.dropna(subset=[time_column[0]],inplace=True)
    weekend_raw_df=weekend_df[['id', 'Completed', route_survey_column[0],'ROUTE_SURVEYED',stopon_clntid_column[0],stopoff_clntid_column[0],time_column[0],time_period_column[0],'Day','ELVIS_STATUS']]
    weekend_raw_df.rename(columns={stopon_clntid_column[0]:'BOARDING LOCATION',stopoff_clntid_column[0]:'ALIGHTING LOCATION'},inplace=True)
    weekday_raw_df.rename(columns={stopon_clntid_column[0]:'BOARDING LOCATION',stopoff_clntid_column[0]:'ALIGHTING LOCATION'},inplace=True)


    wkday_route_level =create_route_level_df(wkday_overall_df,wkday_route_df,weekday_df,project)
    wkend_route_level =create_route_level_df(wkend_overall_df,wkend_route_df,weekend_df,project)
    # wkday_route_df.to_csv("CHECk TOTAL_Difference.csv",index=False)
    # wkend_route_df.to_csv("WKENDCHECk TOTAL_Difference.csv",index=False)
    wkday_comparison_df=copy.deepcopy(wkday_route_level)
    wkday_new_route_level_df=copy.deepcopy(wkday_route_level)

    wkend_comparison_df=copy.deepcopy(wkend_route_level)
    wkend_new_route_level_df=copy.deepcopy(wkend_route_level)
    # this is for time value data
    wkend_time_value_df = create_time_value_df_with_display(wkend_comparison_df,weekend_df,project)
    wkday_time_value_df = create_time_value_df_with_display(wkday_comparison_df,weekday_df,project)

    if not wkday_comparison_df.empty:
        for index , row in wkday_comparison_df.iterrows():
            wkday_comparison_df.loc[index,'Total_DIFFERENCE']=math.ceil(max(0,(row['CR_Total']-row['DB_Total'])))
    else:
        wkday_comparison_df['Total_DIFFERENCE']=0


    if not wkend_comparison_df.empty:
        for index , row in wkend_comparison_df.iterrows():
            wkend_comparison_df.loc[index,'Total_DIFFERENCE']=math.ceil(max(0,(row['CR_Total']-row['DB_Total'])))
    else:
        wkend_comparison_df['Total_DIFFERENCE']=0

    if project=='TUCSON':

        wkend_comparison_df.rename(columns={'CR_AM_Peak':'(1) Goal','CR_Midday':'(2) Goal','CR_PM_Peak':'(3) Goal','CR_Evening':'(4) Goal',
                'DB_AM_Peak':'(1) Collect',
            'DB_Midday':'(2) Collect', 'DB_PM_Peak':'(3) Collect', 'DB_Evening':'(4) Collect', 'AM_DIFFERENCE':'(1) Remain', 'Midday_DIFFERENCE':'(2) Remain',
            'PM_DIFFERENCE':'(3) Remain', 'Evening_DIFFERENCE':'(4) Remain','CR_Overall_Goal':'Route Level Goal','DB_Total':'# of Surveys','Overall_Goal_DIFFERENCE':'Remaining'},inplace=True)

        wkday_comparison_df.rename(columns={'CR_AM_Peak':'(1) Goal','CR_Midday':'(2) Goal','CR_PM_Peak':'(3) Goal','CR_Evening':'(4) Goal',
            'DB_AM_Peak':'(1) Collect',
            'DB_Midday':'(2) Collect', 'DB_PM_Peak':'(3) Collect', 'DB_Evening':'(4) Collect','AM_DIFFERENCE':'(1) Remain', 'Midday_DIFFERENCE':'(2) Remain',
            'PM_DIFFERENCE':'(3) Remain', 'Evening_DIFFERENCE':'(4) Remain','CR_Overall_Goal':'Route Level Goal','DB_Total':'# of Surveys','Overall_Goal_DIFFERENCE':'Remaining'},inplace=True)

        wkday_route_direction_df.rename(columns={'CR_AM_Peak':'(1) Goal','CR_Midday':'(2) Goal','CR_PM_Peak':'(3) Goal','CR_Evening':'(4) Goal',
            'DB_AM_Peak':'(1) Collect',
            'DB_Midday':'(2) Collect', 'DB_PM_Peak':'(3) Collect', 'DB_Evening':'(4) Collect','AM_DIFFERENCE':'(1) Remain', 'Midday_DIFFERENCE':'(2) Remain',
            'PM_DIFFERENCE':'(3) Remain', 'Evening_DIFFERENCE':'(4) Remain','CR_Overall_Goal':'Route Level Goal','DB_Total':'# of Surveys','Overall_Goal_DIFFERENCE':'Remaining'},inplace=True)

        wkend_route_direction_df.rename(columns={'CR_AM_Peak':'(1) Goal','CR_Midday':'(2) Goal','CR_PM_Peak':'(3) Goal','CR_Evening':'(4) Goal',
                'DB_AM_Peak':'(1) Collect',
            'DB_Midday':'(2) Collect', 'DB_PM_Peak':'(3) Collect', 'DB_Evening':'(4) Collect','AM_DIFFERENCE':'(1) Remain', 'Midday_DIFFERENCE':'(2) Remain',
            'PM_DIFFERENCE':'(3) Remain', 'Evening_DIFFERENCE':'(4) Remain'},inplace=True)

        wkday_comparison_df = wkday_comparison_df.merge(
            detail_df_xfers[['ETC_ROUTE_ID', 'ETC_ROUTE_NAME']],
            left_on='ROUTE_SURVEYEDCode',
            right_on='ETC_ROUTE_ID',
            how='left'
        )

        # Rename the column as per requirement
        wkday_comparison_df.rename(columns={'ETC_ROUTE_NAME': 'ROUTE_SURVEYED'}, inplace=True)
        wkday_comparison_df.drop(columns=['ETC_ROUTE_ID'], inplace=True)

        wkend_comparison_df = wkend_comparison_df.merge(
            detail_df_xfers[['ETC_ROUTE_ID', 'ETC_ROUTE_NAME']],
            left_on='ROUTE_SURVEYEDCode',
            right_on='ETC_ROUTE_ID',
            how='left'
        )

        # Rename the column as per requirement
        wkend_comparison_df.rename(columns={'ETC_ROUTE_NAME': 'ROUTE_SURVEYED'}, inplace=True)
        wkend_comparison_df.drop(columns=['ETC_ROUTE_ID'], inplace=True)

        for _,row in wkday_route_direction_df.iterrows():
            route_surveyed=detail_df_stops[detail_df_stops['ETC_ROUTE_ID']==row['ROUTE_SURVEYEDCode']]['ETC_ROUTE_NAME'].iloc[0]
            route_surveyed_ID=detail_df_stops[detail_df_stops['ETC_ROUTE_ID']==row['ROUTE_SURVEYEDCode']]['ETC_ROUTE_ID'].iloc[0]
            wkday_route_direction_df.loc[row.name,'ROUTE_SURVEYED']=route_surveyed  

        for _,row in wkend_route_direction_df.iterrows():
            route_surveyed=detail_df_stops[detail_df_stops['ETC_ROUTE_ID']==row['ROUTE_SURVEYEDCode']]['ETC_ROUTE_NAME'].iloc[0]
            route_surveyed_ID=detail_df_stops[detail_df_stops['ETC_ROUTE_ID']==row['ROUTE_SURVEYEDCode']]['ETC_ROUTE_ID'].iloc[0]
            wkend_route_direction_df.loc[row.name,'ROUTE_SURVEYED']=route_surveyed

    elif project=='VTA':
        wkend_comparison_df.rename(columns={'CR_PRE_Early_AM':'(0) Goal','CR_Early_AM':'(1) Goal','CR_AM_Peak':'(2) Goal','CR_Midday':'(3) Goal','CR_PM_Peak':'(4) Goal','CR_Evening':'(5) Goal',
                'DB_PRE_Early_AM_Peak':'(0) Collect', 'DB_Early_AM_Peak':'(1) Collect', 'DB_AM_Peak':'(2) Collect',
            'DB_Midday':'(3) Collect', 'DB_PM_Peak':'(4) Collect', 'DB_Evening':'(5) Collect','PRE_Early_AM_DIFFERENCE':'(0) Remain',
            'Early_AM_DIFFERENCE':'(1) Remain', 'AM_DIFFERENCE':'(2) Remain', 'Midday_DIFFERENCE':'(3) Remain',
            'PM_DIFFERENCE':'(4) Remain', 'Evening_DIFFERENCE':'(5) Remain','CR_Overall_Goal':'Route Level Goal','DB_Total':'# of Surveys','Overall_Goal_DIFFERENCE':'Remaining'},inplace=True)

        wkday_comparison_df.rename(columns={'CR_PRE_Early_AM':'(0) Goal','CR_Early_AM':'(1) Goal','CR_AM_Peak':'(2) Goal','CR_Midday':'(3) Goal','CR_PM_Peak':'(4) Goal','CR_Evening':'(5) Goal',
                'DB_PRE_Early_AM_Peak':'(0) Collect', 'DB_Early_AM_Peak':'(1) Collect', 'DB_AM_Peak':'(2) Collect',
            'DB_Midday':'(3) Collect', 'DB_PM_Peak':'(4) Collect', 'DB_Evening':'(5) Collect','PRE_Early_AM_DIFFERENCE':'(0) Remain',
            'Early_AM_DIFFERENCE':'(1) Remain', 'AM_DIFFERENCE':'(2) Remain', 'Midday_DIFFERENCE':'(3) Remain',
            'PM_DIFFERENCE':'(4) Remain', 'Evening_DIFFERENCE':'(5) Remain','CR_Overall_Goal':'Route Level Goal','DB_Total':'# of Surveys','Overall_Goal_DIFFERENCE':'Remaining'},inplace=True)

        wkday_route_direction_df.rename(columns={'CR_PRE_Early_AM':'(0) Goal','CR_Early_AM':'(1) Goal','CR_AM_Peak':'(2) Goal','CR_Midday':'(3) Goal','CR_PM_Peak':'(4) Goal','CR_Evening':'(5) Goal',
                'DB_PRE_Early_AM_Peak':'(0) Collect', 'DB_Early_AM_Peak':'(1) Collect', 'DB_AM_Peak':'(2) Collect',
            'DB_Midday':'(3) Collect', 'DB_PM_Peak':'(4) Collect', 'DB_Evening':'(5) Collect','PRE_Early_AM_DIFFERENCE':'(0) Remain',
            'Early_AM_DIFFERENCE':'(1) Remain', 'AM_DIFFERENCE':'(2) Remain', 'Midday_DIFFERENCE':'(3) Remain',
            'PM_DIFFERENCE':'(4) Remain', 'Evening_DIFFERENCE':'(5) Remain'},inplace=True)

        wkend_route_direction_df.rename(columns={'CR_PRE_Early_AM':'(0) Goal','CR_Early_AM':'(1) Goal','CR_AM_Peak':'(2) Goal','CR_Midday':'(3) Goal','CR_PM_Peak':'(4) Goal','CR_Evening':'(5) Goal',
                'DB_PRE_Early_AM_Peak':'(0) Collect', 'DB_Early_AM_Peak':'(1) Collect', 'DB_AM_Peak':'(2) Collect',
            'DB_Midday':'(3) Collect', 'DB_PM_Peak':'(4) Collect', 'DB_Evening':'(5) Collect','PRE_Early_AM_DIFFERENCE':'(0) Remain',
            'Early_AM_DIFFERENCE':'(1) Remain', 'AM_DIFFERENCE':'(2) Remain', 'Midday_DIFFERENCE':'(3) Remain',
            'PM_DIFFERENCE':'(4) Remain', 'Evening_DIFFERENCE':'(5) Remain'},inplace=True)



        wkday_comparison_df = wkday_comparison_df.merge(
            detail_df_xfers[['ETC_ROUTE_ID', 'ETC_ROUTE_NAME']],
            left_on='ROUTE_SURVEYEDCode',
            right_on='ETC_ROUTE_ID',
            how='left'
        )

        # Rename the column as per requirement
        wkday_comparison_df.rename(columns={'ETC_ROUTE_NAME': 'ROUTE_SURVEYED'}, inplace=True)
        wkday_comparison_df.drop(columns=['ETC_ROUTE_ID'], inplace=True)

        wkend_comparison_df = wkend_comparison_df.merge(
            detail_df_xfers[['ETC_ROUTE_ID', 'ETC_ROUTE_NAME']],
            left_on='ROUTE_SURVEYEDCode',
            right_on='ETC_ROUTE_ID',
            how='left'
        )

        # Rename the column as per requirement
        wkend_comparison_df.rename(columns={'ETC_ROUTE_NAME': 'ROUTE_SURVEYED'}, inplace=True)
        wkend_comparison_df.drop(columns=['ETC_ROUTE_ID'], inplace=True)

        for _,row in wkday_route_direction_df.iterrows():
            route_surveyed=detail_df_stops[detail_df_stops['ETC_ROUTE_ID']==row['ROUTE_SURVEYEDCode']]['ETC_ROUTE_NAME'].iloc[0]
            route_surveyed_ID=detail_df_stops[detail_df_stops['ETC_ROUTE_ID']==row['ROUTE_SURVEYEDCode']]['ETC_ROUTE_ID'].iloc[0]
            wkday_route_direction_df.loc[row.name,'ROUTE_SURVEYED']=route_surveyed  

        for _,row in wkend_route_direction_df.iterrows():
            route_surveyed=detail_df_stops[detail_df_stops['ETC_ROUTE_ID']==row['ROUTE_SURVEYEDCode']]['ETC_ROUTE_NAME'].iloc[0]
            route_surveyed_ID=detail_df_stops[detail_df_stops['ETC_ROUTE_ID']==row['ROUTE_SURVEYEDCode']]['ETC_ROUTE_ID'].iloc[0]
            wkend_route_direction_df.loc[row.name,'ROUTE_SURVEYED']=route_surveyed  

    elif project=='UTA':
        weekday_raw_df = weekday_df[weekday_df['ROUTE_SURVEYEDCode_Splited'].isin(wkday_comparison_df['ROUTE_SURVEYEDCode'])]
        weekend_raw_df = weekend_df[weekend_df['ROUTE_SURVEYEDCode_Splited'].isin(wkend_comparison_df['ROUTE_SURVEYEDCode'])]



        wkend_comparison_df.rename(columns={'CR_PRE_Early_AM':'(0) Goal','CR_Early_AM':'(1) Goal','CR_AM_Peak':'(2) Goal','CR_Midday':'(3) Goal','CR_PM_Peak':'(4) Goal','CR_Evening':'(5) Goal',
                'DB_PRE_Early_AM_Peak':'(0) Collect', 'DB_Early_AM_Peak':'(1) Collect', 'DB_AM_Peak':'(2) Collect',
            'DB_Midday':'(3) Collect', 'DB_PM_Peak':'(4) Collect', 'DB_Evening':'(5) Collect','PRE_Early_AM_DIFFERENCE':'(0) Remain',
            'Early_AM_DIFFERENCE':'(1) Remain', 'AM_DIFFERENCE':'(2) Remain', 'Midday_DIFFERENCE':'(3) Remain',
            'PM_DIFFERENCE':'(4) Remain', 'Evening_DIFFERENCE':'(5) Remain','CR_Overall_Goal':'Route Level Goal','DB_Total':'# of Surveys','Overall_Goal_DIFFERENCE':'Remaining'},inplace=True)

        wkday_comparison_df.rename(columns={'CR_PRE_Early_AM':'(0) Goal','CR_Early_AM':'(1) Goal','CR_AM_Peak':'(2) Goal','CR_Midday':'(3) Goal','CR_PM_Peak':'(4) Goal','CR_Evening':'(5) Goal',
                'DB_PRE_Early_AM_Peak':'(0) Collect', 'DB_Early_AM_Peak':'(1) Collect', 'DB_AM_Peak':'(2) Collect',
            'DB_Midday':'(3) Collect', 'DB_PM_Peak':'(4) Collect', 'DB_Evening':'(5) Collect','PRE_Early_AM_DIFFERENCE':'(0) Remain',
            'Early_AM_DIFFERENCE':'(1) Remain', 'AM_DIFFERENCE':'(2) Remain', 'Midday_DIFFERENCE':'(3) Remain',
            'PM_DIFFERENCE':'(4) Remain', 'Evening_DIFFERENCE':'(5) Remain','CR_Overall_Goal':'Route Level Goal','DB_Total':'# of Surveys','Overall_Goal_DIFFERENCE':'Remaining'},inplace=True)

        wkday_route_direction_df.rename(columns={'CR_PRE_Early_AM':'(0) Goal','CR_Early_AM':'(1) Goal','CR_AM_Peak':'(2) Goal','CR_Midday':'(3) Goal','CR_PM_Peak':'(4) Goal','CR_Evening':'(5) Goal',
                'DB_PRE_Early_AM_Peak':'(0) Collect', 'DB_Early_AM_Peak':'(1) Collect', 'DB_AM_Peak':'(2) Collect',
            'DB_Midday':'(3) Collect', 'DB_PM_Peak':'(4) Collect', 'DB_Evening':'(5) Collect','PRE_Early_AM_DIFFERENCE':'(0) Remain',
            'Early_AM_DIFFERENCE':'(1) Remain', 'AM_DIFFERENCE':'(2) Remain', 'Midday_DIFFERENCE':'(3) Remain',
            'PM_DIFFERENCE':'(4) Remain', 'Evening_DIFFERENCE':'(5) Remain'},inplace=True)

        wkend_route_direction_df.rename(columns={'CR_PRE_Early_AM':'(0) Goal','CR_Early_AM':'(1) Goal','CR_AM_Peak':'(2) Goal','CR_Midday':'(3) Goal','CR_PM_Peak':'(4) Goal','CR_Evening':'(5) Goal',
                'DB_PRE_Early_AM_Peak':'(0) Collect', 'DB_Early_AM_Peak':'(1) Collect', 'DB_AM_Peak':'(2) Collect',
            'DB_Midday':'(3) Collect', 'DB_PM_Peak':'(4) Collect', 'DB_Evening':'(5) Collect','PRE_Early_AM_DIFFERENCE':'(0) Remain',
            'Early_AM_DIFFERENCE':'(1) Remain', 'AM_DIFFERENCE':'(2) Remain', 'Midday_DIFFERENCE':'(3) Remain',
            'PM_DIFFERENCE':'(4) Remain', 'Evening_DIFFERENCE':'(5) Remain'},inplace=True)

        wkday_stationwise_route_df.rename(columns={'CR_PRE_Early_AM':'(0) Goal','CR_Early_AM':'(1) Goal','CR_AM_Peak':'(2) Goal','CR_Midday':'(3) Goal','CR_PM_Peak':'(4) Goal','CR_Evening':'(5) Goal',
                'DB_PRE_Early_AM_Peak':'(0) Collect', 'DB_Early_AM_Peak':'(1) Collect', 'DB_AM_Peak':'(2) Collect',
            'DB_Midday':'(3) Collect', 'DB_PM_Peak':'(4) Collect', 'DB_Evening':'(5) Collect','PRE_Early_AM_DIFFERENCE':'(0) Remain',
            'Early_AM_DIFFERENCE':'(1) Remain', 'AM_DIFFERENCE':'(2) Remain', 'Midday_DIFFERENCE':'(3) Remain',
            'PM_DIFFERENCE':'(4) Remain', 'Evening_DIFFERENCE':'(5) Remain'},inplace=True)

        wkend_stationwise_route_df.rename(columns={'CR_PRE_Early_AM':'(0) Goal','CR_Early_AM':'(1) Goal','CR_AM_Peak':'(2) Goal','CR_Midday':'(3) Goal','CR_PM_Peak':'(4) Goal','CR_Evening':'(5) Goal',
                'DB_PRE_Early_AM_Peak':'(0) Collect', 'DB_Early_AM_Peak':'(1) Collect', 'DB_AM_Peak':'(2) Collect',
            'DB_Midday':'(3) Collect', 'DB_PM_Peak':'(4) Collect', 'DB_Evening':'(5) Collect','PRE_Early_AM_DIFFERENCE':'(0) Remain',
            'Early_AM_DIFFERENCE':'(1) Remain', 'AM_DIFFERENCE':'(2) Remain', 'Midday_DIFFERENCE':'(3) Remain',
            'PM_DIFFERENCE':'(4) Remain', 'Evening_DIFFERENCE':'(5) Remain'},inplace=True)


        wkday_comparison_df = wkday_comparison_df.merge(
            detail_df_xfers[['ETC_ROUTE_ID', 'ETC_ROUTE_NAME']],
            left_on='ROUTE_SURVEYEDCode',
            right_on='ETC_ROUTE_ID',
            how='left'
        )

        # Rename the column as per requirement
        wkday_comparison_df.rename(columns={'ETC_ROUTE_NAME': 'ROUTE_SURVEYED'}, inplace=True)
        wkday_comparison_df.drop(columns=['ETC_ROUTE_ID'], inplace=True)

        wkend_comparison_df = wkend_comparison_df.merge(
            detail_df_xfers[['ETC_ROUTE_ID', 'ETC_ROUTE_NAME']],
            left_on='ROUTE_SURVEYEDCode',
            right_on='ETC_ROUTE_ID',
            how='left'
        )

        # Rename the column as per requirement
        wkend_comparison_df.rename(columns={'ETC_ROUTE_NAME': 'ROUTE_SURVEYED'}, inplace=True)
        wkend_comparison_df.drop(columns=['ETC_ROUTE_ID'], inplace=True)

        for _, row in wkday_route_direction_df.iterrows():
            # Filter the DataFrame by 'ETC_ROUTE_ID'
            filtered_df = detail_df_stops[detail_df_stops['ETC_ROUTE_ID'] == row['ROUTE_SURVEYEDCode']]
            
            # Check if filtered_df is not empty
            if not filtered_df.empty:
                route_surveyed = filtered_df['ETC_ROUTE_NAME'].iloc[0]
                route_surveyed_ID = filtered_df['ETC_ROUTE_ID'].iloc[0]
            else:
                route_surveyed = None  # or a default value like 'Unknown'
                route_surveyed_ID = None  # or a default value like 'Unknown'
            station_name=wkday_overall_df[wkday_overall_df['STATION_ID']==row['STATION_ID']]['STATION_NAME'].iloc[0]
            wkday_route_direction_df.loc[row.name, 'ROUTE_SURVEYED'] = route_surveyed
            wkday_route_direction_df.loc[row.name, 'STATION_NAME'] = station_name

        for _,row in wkend_route_direction_df.iterrows():
            route_surveyed=detail_df_stops[detail_df_stops['ETC_ROUTE_ID']==row['ROUTE_SURVEYEDCode']]['ETC_ROUTE_NAME'].iloc[0]
            route_surveyed_ID=detail_df_stops[detail_df_stops['ETC_ROUTE_ID']==row['ROUTE_SURVEYEDCode']]['ETC_ROUTE_ID'].iloc[0]
            station_name=wkend_overall_df[wkend_overall_df['STATION_ID']==row['STATION_ID']]['STATION_NAME'].iloc[0]
            wkend_route_direction_df.loc[row.name,'ROUTE_SURVEYED']=route_surveyed
            wkend_route_direction_df.loc[row.name,'STATION_NAME']=station_name

        # this is for getting STATION NAME and ROUTE_SURVEYED values in Route_Stationwise_DATAFRAME
        for _, row in wkday_stationwise_route_df.iterrows():
            # Filter the DataFrame by 'ETC_ROUTE_ID'
            filtered_df = detail_df_stops[detail_df_stops['ETC_ROUTE_ID'] == row['ROUTE_SURVEYEDCode']]
            
            # Check if filtered_df is not empty
            if not filtered_df.empty:
                route_surveyed = filtered_df['ETC_ROUTE_NAME'].iloc[0]
                route_surveyed_ID = filtered_df['ETC_ROUTE_ID'].iloc[0]
            else:
                route_surveyed = None  # or a default value like 'Unknown'
                route_surveyed_ID = None  # or a default value like 'Unknown'
            station_name=wkday_overall_df[wkday_overall_df['STATION_ID']==row['STATION_ID']]['STATION_NAME'].iloc[0]
            wkday_stationwise_route_df.loc[row.name, 'ROUTE_SURVEYED'] = route_surveyed
            wkday_stationwise_route_df.loc[row.name,'STATION_NAME']=station_name    


        for _,row in wkend_stationwise_route_df.iterrows():
            route_surveyed=detail_df_stops[detail_df_stops['ETC_ROUTE_ID']==row['ROUTE_SURVEYEDCode']]['ETC_ROUTE_NAME'].iloc[0]
            route_surveyed_ID=detail_df_stops[detail_df_stops['ETC_ROUTE_ID']==row['ROUTE_SURVEYEDCode']]['ETC_ROUTE_ID'].iloc[0]
            station_name=wkend_overall_df[wkend_overall_df['STATION_ID']==row['STATION_ID']]['STATION_NAME'].iloc[0]
            wkend_stationwise_route_df.loc[row.name,'ROUTE_SURVEYED']=route_surveyed    
            wkend_stationwise_route_df.loc[row.name,'STATION_NAME']=station_name    
        print("After this, I'll create snowflake connection") 


    def create_snowflake_connection():
        print("Creating connection with snowflake")
        conn = snowflake.connector.connect(
            user=os.getenv('user'),
            password=os.getenv('password'),
            account=os.getenv('account'),
            warehouse=os.getenv('warehouse'),
            database=os.getenv('database'),
            schema=schema,
            role=os.getenv('role')
        )
        print("Connection successfull")
        return conn
        

    def create_tables_and_insert_data(dataframes, table_info):
        print("creating table")
        # File path and dtype mapping
        dtype_mapping = {
            'object': 'VARCHAR',
            'int64': 'INTEGER',
            'float64': 'FLOAT',
            'datetime64[ns]': 'TIMESTAMP',
            'bool': 'BOOLEAN'
        }
        
        # Create Snowflake connection
        conn = create_snowflake_connection()
        cur = conn.cursor()

        for sheet_name, table_name in table_info.items():
            print("In loop 1483")
            df = dataframes.get(sheet_name)
            if df is not None:
                # Drop the table if it exists
                drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
                cur.execute(drop_table_sql)
                print(f"Table {table_name} dropped successfully (if it existed).")

                # Dynamically generate the CREATE TABLE statement
                create_table_sql = f"CREATE TABLE {table_name} (\n"
                for column, dtype in df.dtypes.items():
                    sanitized_column = f'"{column}"'  # Handle special characters
                    snowflake_dtype = dtype_mapping.get(str(dtype), 'VARCHAR')  # Default to VARCHAR for unknown types
                    create_table_sql += f"  {sanitized_column} {snowflake_dtype},\n"
                create_table_sql = create_table_sql.rstrip(",\n") + "\n);"

                # Print the create table SQL for reference (optional)
                # Execute the CREATE TABLE statement
                cur.execute(create_table_sql)
                print(f"Table {table_name} created successfully.")

                # Insert data into the Snowflake table
                write_pandas(conn, df, table_name=table_name.upper())
                print(f"Data inserted into table {table_name} successfully.")

        # Close the Snowflake connection
        cur.close()
        conn.close()

    if project=='TUCSON':
        # DataFrames preparation
        dataframes = {
            'WkDAY Route DIR Comparison': wkday_route_direction_df.drop(columns=['CR_Total', 'Total_DIFFERENCE']),
            'WkEND Route DIR Comparison': wkend_route_direction_df.drop(columns=['CR_Total', 'Total_DIFFERENCE']),
            'WkDAY RAW DATA': weekday_raw_df,
            'WkEND RAW DATA': weekend_raw_df,
            'WkEND Time Data': wkend_time_value_df,
            'WkDAY Time Data': wkday_time_value_df,
            'WkDAY Route Comparison': wkday_comparison_df.drop(columns=['CR_Total', 'DB_AM_IDS', 'DB_Midday_IDS', 'DB_PM_IDS', 'DB_Evening_IDS', 'Total_DIFFERENCE']),
            'WkEND Route Comparison': wkend_comparison_df.drop(columns=['CR_Total', 'DB_AM_IDS', 'DB_Midday_IDS', 'DB_PM_IDS', 'DB_Evening_IDS', 'Total_DIFFERENCE']),
            'LAST SURVEY DATE': latest_date_df
        }

        # Table mapping
        table_info = {
            'WkDAY RAW DATA': 'wkday_raw', 
            'WkEND RAW DATA': 'wkend_raw', 
            'WkDAY Route Comparison': 'wkday_comparison', 
            'WkDAY Route DIR Comparison': 'wkday_dir_comparison', 
            'WkEND Route Comparison': 'wkend_comparison', 
            'WkEND Route DIR Comparison': 'wkend_dir_comparison', 
            'WkEND Time Data': 'wkend_time_data', 
            'WkDAY Time Data': 'wkday_time_data',
            'LAST SURVEY DATE': 'last_survey_date'
        }

    elif project=='VTA':
        dataframes = {
            'WkDAY Route DIR Comparison': wkday_route_direction_df.drop(columns=['CR_Total', 'Total_DIFFERENCE']),
            'WkEND Route DIR Comparison': wkend_route_direction_df.drop(columns=['CR_Total', 'Total_DIFFERENCE']),
            'WkDAY RAW DATA': weekday_raw_df,
            'WkEND RAW DATA': weekend_raw_df,
            'WkEND Time Data': wkend_time_value_df,
            'WkDAY Time Data': wkday_time_value_df,
            'WkDAY Route Comparison': wkday_comparison_df.drop(columns=['CR_Total', 'DB_AM_IDS', 'DB_Midday_IDS', 'DB_PM_IDS', 'DB_Evening_IDS', 'Total_DIFFERENCE']),
            'WkEND Route Comparison': wkend_comparison_df.drop(columns=['CR_Total', 'DB_AM_IDS', 'DB_Midday_IDS', 'DB_PM_IDS', 'DB_Evening_IDS', 'Total_DIFFERENCE']),
            'LAST SURVEY DATE': latest_date_df
        }

        # Table mapping
        table_info = {
            'WkDAY RAW DATA': 'wkday_raw', 
            'WkEND RAW DATA': 'wkend_raw', 
            'WkDAY Route Comparison': 'wkday_comparison', 
            'WkDAY Route DIR Comparison': 'wkday_dir_comparison', 
            'WkEND Route Comparison': 'wkend_comparison', 
            'WkEND Route DIR Comparison': 'wkend_dir_comparison', 
            'WkEND Time Data': 'wkend_time_data', 
            'WkDAY Time Data': 'wkday_time_data',
            'LAST SURVEY DATE': 'last_survey_date'
        }
    elif project=='UTA':
        dataframes={
            'WkDAY Route DIR Comparison':wkday_route_direction_df.drop(columns=['STATION_ID_SPLITTED','CR_Total','Total_DIFFERENCE','DB_Total']),
        'WkEND Route DIR Comparison':wkend_route_direction_df.drop(columns=['STATION_ID_SPLITTED','CR_Total','Total_DIFFERENCE','DB_Total']),
        'WkEND Time Data':wkend_time_value_df,
        'WkDAY Time Data':wkday_time_value_df,

        'WkEND Stationwise Comparison':wkend_stationwise_route_df.drop(columns=['CR_Total','Total_DIFFERENCE','DB_Total']),
        'WkDAY Stationwise Comparison':wkday_stationwise_route_df.drop(columns=['CR_Total','Total_DIFFERENCE','DB_Total']),

        # wkday_comparison_df.to_excel(writer,sheet_name='WkDAY Route Comparison',index=False)
        'WkDAY Route Comparison':wkday_comparison_df.drop(columns=['CR_Total','DB_PRE_Early_AM_IDS','DB_Early_AM_IDS','DB_AM_IDS','DB_Midday_IDS','DB_PM_IDS','DB_Evening_IDS','Total_DIFFERENCE']),
       'WkEND Route Comparison':wkend_comparison_df.drop(columns=['CR_Total','DB_PRE_Early_AM_IDS','DB_Early_AM_IDS','DB_AM_IDS','DB_Midday_IDS','DB_PM_IDS','DB_Evening_IDS','Total_DIFFERENCE']),
        
        "WkDAY RAW DATA":weekday_raw_df[['id','Completed',route_survey_column[0],'ROUTE_SURVEYED',stopon_clntid_column[0],stopoff_clntid_column[0],time_column[0],time_period_column[0],'Day','ELVIS_STATUS']],
        'WkEND RAW DATA':weekend_raw_df[['id','Completed',route_survey_column[0],'ROUTE_SURVEYED',stopon_clntid_column[0],stopoff_clntid_column[0],time_column[0],time_period_column[0],'Day','ELVIS_STATUS']],
        'LAST SURVEY DATE':latest_date_df
        }
        table_info = {
        'WkDAY RAW DATA': 'wkday_raw', 
        'WkEND RAW DATA': 'wkend_raw', 
        'WkEND Stationwise Comparison': 'wkday_stationwise_comparison', 
        'WkDAY Stationwise Comparison': 'wkend_stationwise_comparison',
        'WkDAY Route Comparison': 'wkday_comparison', 
        'WkDAY Route DIR Comparison': 'wkday_dir_comparison', 
        'WkEND Route Comparison': 'wkend_comparison', 
        'WkEND Route DIR Comparison': 'wkend_dir_comparison', 
        'WkEND Time Data': 'wkend_time_data', 
        'WkDAY Time Data': 'wkday_time_data',
        'LAST SURVEY DATE': 'last_survey_date',
            }
    # Call the function
    print("Final call")
    create_tables_and_insert_data(dataframes, table_info)

    print("Files Uploaded SuccessFully")