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
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from automated_sync_flow_utils import *

warnings.filterwarnings('ignore')

load_dotenv()

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

PROJECTS = {
    "TUCSON": {
        "databases": {
                    "main": {
                        "database": os.getenv("TUCSON_DATABASE_NAME"),
                        "table": os.getenv("TUCSON_TABLE_NAME")
                    },
                    "elvis": {
                        "database": os.getenv("TUCSON_ELVIS_DATABASE_NAME"),
                        "table": os.getenv("TUCSON_ELVIS_TABLE_NAME")
                    }
                },
        "files": {
            "details": "details_TUCSON_AZ_od_excel.xlsx",
            "cr": "TUCSON_AZ_CR.xlsx",
            'kingelvis':'Tucson_az_2025_KINGElvis.xlsx'
        }
    },
    "TUCSON RAIL": {
        "databases": {
                    "main": {
                        "database": os.getenv("TUCSON_DATABASE_NAME"),
                        "table": os.getenv("TUCSON_TABLE_NAME")
                    },
                    "elvis": {
                        "database": os.getenv("TUCSON_ELVIS_DATABASE_NAME"),
                        "table": os.getenv("TUCSON_ELVIS_TABLE_NAME")
                    }
                },
        "files": {
            "details": "details_TUCSON_AZ_od_excel.xlsx",
            "cr": "TUCSON_AZ_CR.xlsx",
            'kingelvis':'Tucson_az_2025_KINGElvis.xlsx'
        }
    },
    "UTA": {
        "databases": {
                    "elvis": {
                        "database": os.getenv("UTA_ELVIS_DATABASE_NAME"),
                        "table": os.getenv("UTA_ELVIS_TABLE_NAME")
                    }
                },
        "files": {
            "details": "details_project_od_excel_UTA.xlsx",
            "cr": "UTA_SL_CR.xlsx"
        }
    },
    "VTA": {
        "databases": {
                    "elvis": {
                        "database": os.getenv("VTA_ELVIS_DATABASE_NAME"),
                        "table": os.getenv("VTA_ELVIS_TABLE_NAME")
                    }
                },
        "files": {
            "details": "details_vta_CA_od_excel.xlsx",
            "cr": "VTA_CA_CR.xlsx",
            'kingelvis':'VTA_CA_OB_KINGElvis.xlsx'
        }
    },
    "STL": {
        "databases": {
                    "elvis": {
                        "database": os.getenv("STL_ELVIS_DATABASE_NAME"),
                        "table": os.getenv("STL_ELVIS_TABLE_NAME")
                    },
                    "baby_elvis": {
                        "database": os.getenv("STL_BABY_ELVIS_DATABASE_NAME"),
                        "table": os.getenv("STL_BABY_ELVIS_TABLE_NAME")
                    }
                },
        "files": {
            "details": "details_saint_louis_MO_od_excel.xlsx",
            "cr": "STL_MO_CR.xlsx",
            # 'kingelvis':'VTA_CA_OB_KINGElvis.xlsx'
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
            print(df.tail(2))
            
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
    
    elvis_config=project_config['databases']["elvis"]
    table_name=elvis_config['table']
    database_name=elvis_config["database"]
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
        time_value_code_check=['have5minforsurvecode']
        route_surveyed_code_check=['routesurveyedcode']
        route_surveyed_code=check_all_characters_present(df,route_surveyed_code_check)
        time_value_code_df=check_all_characters_present(df,time_value_code_check)
        df=df[df[time_value_code_df[0]]==1]
        df=df[df['INTERV_INIT']!='999']
        df=df[df['INTERV_INIT']!=999]
        df = df[df[time_value_code_df[0]] == 1]
        df=df[df['INTERV_INIT']!=999]
        elvis_status_column_check=['elvisstatus']
        elvis_status_column=check_all_characters_present(df,elvis_status_column_check)
        df=df[df[elvis_status_column[0]].str.lower()!='delete']
        df.drop_duplicates(subset='id',inplace=True)
        time_column_check=['timeoncode']
        time_period_column_check=['timeon']
        df.rename(columns={route_surveyed_code[0]:'ROUTE_SURVEYEDCode'},inplace=True)

        time_column_df=check_all_characters_present(df,time_column_check)
        time_period_column_df=check_all_characters_present(df,time_period_column_check)

        
        # st.write(df.head())  # Display the filtered data
    else:
        st.warning("No data available. Click 'Fetch Data' to load the dataset.")
    
    df1=None
    if "main" in project_config["databases"]:
        main_config = project_config["databases"]["main"]
        main_table_name = main_config["table"]
        main_database_name = main_config["database"]
        main_csv_buffer = fetch_data(main_database_name, main_table_name)
        df1 = pd.read_csv(main_csv_buffer) if main_csv_buffer else None

        column_mapping = {}
        for df1_col in df1.columns:
            cleaned_df1_col = clean_string(df1_col)
            for df_col in df.columns:
                if cleaned_df1_col == clean_string(df_col):
                    column_mapping[df1_col] = df_col
                    break  # Move to next df1 column once we find a match

        # Rename df1 columns to match df column names exactly
        df1 = df1.rename(columns=column_mapping)
        time_column_check=['timeoncode']
        time_period_column_check=['timeon']
        time_column_df1=check_all_characters_present(df1,time_column_check)
        time_period_column_df1=check_all_characters_present(df1,time_period_column_check)

    if df is not None and df1 is not None:

        df3 = df.copy()
        # Code for Adding new records from baby elvis to elvis database file 
        # added 
        missing_ids = set(df1['id']) - set(df['id'])

        # Filter df1 to get only records with missing IDs
        df1_new = df1[df1['id'].isin(missing_ids)]

        # Concatenate df3 (original df) with the filtered df1_new
        df = pd.concat([df, df1_new], ignore_index=True)
        
        df.drop_duplicates(subset=['id'],inplace=True)
        # Sort by ID (optional)
        df = df.sort_values('id').reset_index(drop=True)
        # Code for Adding new records from baby elvis to elvis database file ends here

        # Code for Adding Time_ONCode values from baby elvis to elvis database file 
        # Identify rows where time_column_df[0] is either NaN or empty string
        mask = df[time_column_df[0]].isna() | (df[time_column_df[0]].str.strip() == '')

        # Create a mapping dictionary from df1 using 'id' as key and time_column_df1[0] as value
        time_mapping = dict(zip(df1['id'], df1[time_column_df1[0]]))

        # Fill the missing/empty values in df using the mapping
        df.loc[mask, time_column_df[0]] = df.loc[mask, 'id'].map(time_mapping)
        # Code for Adding Time_ONCode values from baby elvis to elvis database file ends here

        print("Data merged successfully!")
    else:
        print("One or both dataframes failed to load.")

    bucket_name = os.getenv('bucket_name')

    s3_client = boto3.client(
    's3',
    aws_access_key_id = os.getenv('aws_access_key_id'),
    aws_secret_access_key = os.getenv('aws_secret_access_key')
    )

    # Fetch baby_elvis data (new code)
    if "baby_elvis" in PROJECTS["STL"]["databases"]:
        baby_elvis_config = PROJECTS["STL"]["databases"]["baby_elvis"]
        baby_table_name = baby_elvis_config['table']
        baby_database_name = baby_elvis_config["database"]

        # Initialize session state for baby_elvis_df if it doesn't exist
        if "baby_elvis_df" not in st.session_state:
            st.session_state.baby_elvis_df = None

        # Streamlit button to fetch baby_elvis data
        baby_csv_buffer = fetch_data(baby_database_name, baby_table_name)
        if baby_csv_buffer:  # Ensure data was fetched successfully
            st.session_state.baby_elvis_df = pd.read_csv(baby_csv_buffer)  # Load into DataFrame
            baby_elvis_df = st.session_state.baby_elvis_df  # Create local reference

            # Display success message
            st.success(f"Successfully loaded {len(baby_elvis_df)} records from baby_elvis")
        else:
            st.error("Failed to load baby_elvis data.")


    # Function to read an Excel file from S3 into a DataFrame
    def read_excel_from_s3(bucket_name, file_key, sheet_name):
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        excel_data = response['Body'].read()
        return pd.read_excel(BytesIO(excel_data), sheet_name=sheet_name)

    if project=='TUCSON':
        ke_df = read_excel_from_s3(bucket_name,project_config["files"]["kingelvis"], 'Elvis_Review')

        detail_df_stops = read_excel_from_s3(bucket_name,project_config["files"]["details"], 'STOPS')
        detail_df_xfers = read_excel_from_s3(bucket_name, project_config["files"]["details"], 'XFERS')

        wkend_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkEND-Overall')
        wkend_route_df = read_excel_from_s3(bucket_name,project_config["files"]["cr"], 'WkEND-RouteTotal')

        wkday_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-Overall')
        wkday_route_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-RouteTotal')

        ke_df=ke_df[ke_df['INTERV_INIT']!='999']
        ke_df=ke_df[ke_df['INTERV_INIT']!=999]
        ke_df=ke_df[ke_df['1st Cleaner']!='No 5 MIN']
        ke_df=ke_df[ke_df['1st Cleaner']!='Test']
        ke_df=ke_df[ke_df['1st Cleaner']!='Test/No 5 MIN']
        ke_df=ke_df[ke_df['Final_Usage'].str.lower()=='use']
        df['ROUTE_SURVEYEDCode'] = df['ROUTE_SURVEYEDCode'].apply(lambda x: '_'.join([str(x).split('_')[0], '1'] + str(x).split('_')[2:]))
        df=pd.merge(df,ke_df['id'],on='id',how='inner')


        print("Files read for TUCSON")

    if project=='STL':
        # ke_df = read_excel_from_s3(bucket_name,project_config["files"]["kingelvis"], 'Elvis_Review')

        detail_df_stops = read_excel_from_s3(bucket_name,project_config["files"]["details"], 'STOPS')
        detail_df_xfers = read_excel_from_s3(bucket_name, project_config["files"]["details"], 'XFERS')

        wkend_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkEND-Overall')
        wkend_route_df = read_excel_from_s3(bucket_name,project_config["files"]["cr"], 'WkEND-RouteTotal')

        wkday_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-Overall')
        wkday_route_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-RouteTotal')

        # ke_df=ke_df[ke_df['INTERV_INIT']!='999']
        # ke_df=ke_df[ke_df['INTERV_INIT']!=999]
        # ke_df=ke_df[ke_df['1st Cleaner']!='No 5 MIN']
        # ke_df=ke_df[ke_df['1st Cleaner']!='Test']
        # ke_df=ke_df[ke_df['1st Cleaner']!='Test/No 5 MIN']
        # ke_df=ke_df[ke_df['Final_Usage'].str.lower()=='use']
        # df['ROUTE_SURVEYEDCode'] = df['ROUTE_SURVEYEDCode'].apply(lambda x: '_'.join([str(x).split('_')[0], '1'] + str(x).split('_')[2:]))
        # df=pd.merge(df,ke_df['id'],on='id',how='inner')


        print("Files read for STL")

    elif project=='TUCSON RAIL':
        ke_df = read_excel_from_s3(bucket_name,project_config["files"]["kingelvis"], 'Elvis_Review')

        detail_df_stops = read_excel_from_s3(bucket_name,project_config["files"]["details"], 'STOPS')
        detail_df_xfers = read_excel_from_s3(bucket_name, project_config["files"]["details"], 'XFERS')

        wkend_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkEND-RAIL')
        wkend_route_df = read_excel_from_s3(bucket_name,project_config["files"]["cr"], 'WkEND-RailTotal')

        wkday_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-RAIL')
        wkday_route_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-RailTotal')

        print("Files read for TUCSON")
        ke_df=ke_df[ke_df['INTERV_INIT']!='999']
        ke_df=ke_df[ke_df['INTERV_INIT']!=999]
        ke_df=ke_df[ke_df['1st Cleaner']!='No 5 MIN']
        ke_df=ke_df[ke_df['1st Cleaner']!='Test']
        ke_df=ke_df[ke_df['1st Cleaner']!='Test/No 5 MIN']
        ke_df=ke_df[ke_df['Final_Usage'].str.lower()=='use']
        df['ROUTE_SURVEYEDCode'] = df['ROUTE_SURVEYEDCode'].apply(lambda x: '_'.join([str(x).split('_')[0], '1'] + str(x).split('_')[2:]))
        df=pd.merge(df,ke_df['id'],on='id',how='inner')


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


    elif project=='VTA':

        ke_df = read_excel_from_s3(bucket_name,project_config["files"]["kingelvis"], 'Elvis_Review')

        detail_df_stops = read_excel_from_s3(bucket_name,project_config["files"]["details"], 'STOPS')
        detail_df_xfers = read_excel_from_s3(bucket_name, project_config["files"]["details"], 'XFERS')

        wkend_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkEND-Overall')
        wkend_route_df = read_excel_from_s3(bucket_name,project_config["files"]["cr"], 'WkEND-RouteTotal')

        wkday_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-Overall')
        wkday_route_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-RouteTotal')
        df['ROUTE_SURVEYEDCode_Splited']=df['ROUTE_SURVEYEDCode'].apply(lambda x:('_').join(str(x).split('_')[:-1]) )

        
        print("Files read for VTA")
        ke_df=ke_df[ke_df['INTERV_INIT']!='999']
        ke_df=ke_df[ke_df['INTERV_INIT']!=999]
        ke_df=ke_df[ke_df['1st Cleaner']!='No 5 MIN']
        ke_df=ke_df[ke_df['1st Cleaner']!='Test']
        ke_df=ke_df[ke_df['1st Cleaner']!='Test/No 5 MIN']
        ke_df=ke_df[ke_df['Final_Usage'].str.lower()=='use']

        df=pd.merge(df,ke_df['id'],on='id',how='inner')


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

    have5min_column_check=['have5minforsurvecode']
    have5min_column=check_all_characters_present(df,have5min_column_check)
    df=df[df[have5min_column[0]]==1]
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


    df['ROUTE_SURVEYEDCode_SPLITED']=df['ROUTE_SURVEYEDCode'].apply(lambda x : '_'.join(str(x).split('_')[0:-1]))
    # df[['ROUTE_SURVEYEDCode_SPLITED']]

    detail_df_stops['ETC_ROUTE_ID_SPLITED']=detail_df_stops['ETC_ROUTE_ID'].apply(lambda x : '_'.join(str(x).split('_')[0:-1]))
    detail_df_stops[['ETC_ROUTE_ID_SPLITED']].head(2)

    # ke_df=ke_df[ke_df['id']==8492]


    detail_df_stops['ETC_STOP_DIRECTION']=detail_df_stops['ETC_STOP_ID'].apply(lambda x : str(x).split('_')[-2])
    detail_df_stops[['ETC_STOP_DIRECTION']].head(2)


    # Assuming you already have a DataFrame `df`
    # df['STOP_ON_SEQ'] = None
    # df['STOP_OFF_SEQ'] = None
    # Iterate through df rows to get the STOP_ON points
    for _, row in df.iterrows():
        nearest_stop_seq = []    
        
        stop_on_id=row[stop_on_id_column[0]]    
        
        stop_on_lat = row[stop_on_lat_lon_columns[0]]
        stop_on_long = row[stop_on_lat_lon_columns[1]]
    #     if pd.isna(origin_lat) or pd.isna(origin_long):
    #         continue 
        
        # Filtered data if you want to change the comparison based on DIRECTION/DIRECTIONLess
        
    #     filtered_df = detail_df_stops[detail_df_stops['ETC_ROUTE_ID_SPLITED'] == row['ROUTE_SURVEYEDCode_SPLITED']][
    #         ['stop_lat6', 'stop_lon6', 'seq_fixed', 'ETC_STOP_ID','ETC_STOP_NAME']
    #     ]
        filtered_df = detail_df_stops[detail_df_stops['ETC_ROUTE_ID'] == row['ROUTE_SURVEYEDCode']][
            ['stop_lat6', 'stop_lon6', 'seq_fixed', 'ETC_STOP_ID','ETC_STOP_NAME']
        ]
        
        # List to store distances
        distances = []
        
        # Calculate distances for all rows in filtered_df
        for _, detail_row in filtered_df.iterrows():
            stop_lat6 = detail_row['stop_lat6']
            stop_lon6 = detail_row['stop_lon6']
            
            # Compute distance
            distance = get_distance_between_coordinates(stop_on_lat, stop_on_long, stop_lat6, stop_lon6)
            
            # Skip distance if it is 0
    #         if distance == 0:
    #             continue
            
            distances.append((distance, detail_row['seq_fixed'], detail_row['ETC_STOP_ID'],detail_row['ETC_STOP_NAME'],detail_row['stop_lat6'],detail_row['stop_lon6']))
        
        # Find the nearest stop (minimum distance)
        if distances:
            valid_distances = [d for d in distances if d[0] is not None]  # Filter out None values
            if valid_distances:  # Ensure there's at least one valid entry
                nearest_stop = min(valid_distances, key=lambda x: x[0])  # x[0] is the distance
                nearest_stop_seq.append(nearest_stop)
            else:
                print("No valid distances found.")
        
        # Process nearest_stop_seq as needed

        if nearest_stop_seq:
            df.loc[row.name, 'STOP_ON_ADDR_NEW'] = nearest_stop_seq[0][3]  # ETC_STOP_NAME
            df.loc[row.name, 'STOP_ON_SEQ'] = nearest_stop_seq[0][1]      # seq_fixed
            df.loc[row.name, 'STOP_ON_CLINTID_NEW'] = nearest_stop_seq[0][2]  # ETC_STOP_ID
            df.loc[row.name, 'STOP_ON_LAT_NEW'] = nearest_stop_seq[0][4]      # stop_lat6
            df.loc[row.name, 'STOP_ON_LONG_NEW'] = nearest_stop_seq[0][5]     # stop_lon6

    # Iterate through df rows to get the STOP_OFF points
    # Iterate through new_df rows
    for _, row in df.iterrows():
        nearest_stop_seq = []
        
    #     stop_on_id=row[stop_on_id_column[0]]    
        stop_off_lat = row[stop_off_lat_lon_columns[0]]
        stop_off_long = row[stop_off_lat_lon_columns[1]]

    #     stop_on_lat = row['STOP_ON_LAT_NEW']
    #     stop_on_long = row['STOP_ON_LONG_NEW']
        if pd.isna(stop_off_lat) or pd.isna(stop_off_long):
            continue
        stop_on_direction = str(row['STOP_ON_CLINTID_NEW']).split('_')[-2] if len(str(row['STOP_ON_CLINTID_NEW']).split('_')) >= 2 else None
        if stop_on_direction is None:
            # Skip the current iteration if the direction cannot be determined
            continue

        # Filtered data if you want to change the comparison based on DIRECTION/DIRECTIONLess
    #     filtered_df = detail_df_stops[(detail_df_stops['ETC_ROUTE_ID_SPLITED'] == row['ROUTE_SURVEYEDCode_SPLITED'])&(detail_df_stops['ETC_STOP_DIRECTION']==stop_on_direction)][
    #         ['stop_lat6', 'stop_lon6', 'seq_fixed', 'ETC_STOP_ID','ETC_STOP_NAME']
    #     ]
        filtered_df = detail_df_stops[(detail_df_stops['ETC_ROUTE_ID'] == row['ROUTE_SURVEYEDCode'])&(detail_df_stops['ETC_STOP_DIRECTION']==stop_on_direction)][
            ['stop_lat6', 'stop_lon6', 'seq_fixed', 'ETC_STOP_ID','ETC_STOP_NAME']
        ]
    #     filtered_df = detail_df_stops[detail_df_stops['ETC_ROUTE_ID'] == row['ROUTE_SURVEYEDCode']][
    #         ['stop_lat6', 'stop_lon6', 'seq_fixed', 'ETC_STOP_ID','ETC_STOP_NAME']

        # List to store distances
        distances = []
        
        # Calculate distances for all rows in filtered_df
        for _, detail_row in filtered_df.iterrows():
            stop_lat6 = detail_row['stop_lat6']
            stop_lon6 = detail_row['stop_lon6']
            
            # Compute distance
            distance = get_distance_between_coordinates(stop_off_lat, stop_off_long, stop_lat6, stop_lon6)
            # Skip distance if it is 0
    #         if distance == 0:
    #             continue
    #         if distance>0.5:
            distances.append((distance, detail_row['seq_fixed'], detail_row['ETC_STOP_ID'],detail_row['ETC_STOP_NAME'],detail_row['stop_lat6'],detail_row['stop_lon6']))
        
        # Find the nearest stop (minimum distance)
        if distances:
            valid_distances = [d for d in distances if d[0] is not None]  # Filter out None values
            if valid_distances:  # Ensure there's at least one valid entry
                nearest_stop = min(valid_distances, key=lambda x: x[0])  # x[0] is the distance
                nearest_stop_seq.append(nearest_stop)
            else:
                print("No valid distances found.")
        

        # Process nearest_stop_seq as needed
        if nearest_stop_seq:
            df.loc[row.name, 'STOP_OFF_ADDRESS_NEW'] = nearest_stop_seq[0][3]  # ETC_STOP_NAME
            df.loc[row.name, 'STOP_OFF_SEQ'] = nearest_stop_seq[0][1]      # seq_fixed
            df.loc[row.name, 'STOP_OFF_CLINTID_NEW'] = nearest_stop_seq[0][2]  # ETC_STOP_ID
            df.loc[row.name, 'STOP_OFF_LAT_NEW'] = nearest_stop_seq[0][4]      # stop_lat6
            df.loc[row.name, 'STOP_OFF_LONG_NEW'] = nearest_stop_seq[0][5]     # stop_lon6

    df['SEQ_DIFFERENCE']=df['STOP_OFF_SEQ']-df['STOP_ON_SEQ']

    ids_list = []
    for _,row in df.iterrows():
        nearest_stop_on_seq=[]
        nearest_stop_off_seq=[]
        route_code = row[route_surveyed_column[0]]
        if row['SEQ_DIFFERENCE'] < 0:
            ids_list.append(row['id'])
            stop_on_lat = row['STOP_ON_LAT_NEW']
            stop_on_long = row['STOP_ON_LONG_NEW']

            stop_off_lat = row['STOP_OFF_LAT_NEW']
            stop_off_long = row['STOP_OFF_LONG_NEW']
            
            stop_on_direction = row[ 'STOP_ON_CLINTID_NEW'].split('_')[-2]
            stop_off_direction = row[ 'STOP_OFF_CLINTID_NEW'].split('_')[-2]
            new_route_code = (
                f"{'_'.join(route_code.split('_')[:-1])}_01" 
                if route_code.split('_')[-1] == '00' 
                else f"{'_'.join(route_code.split('_')[:-1])}_00"
            )
            df.loc[row.name, 'ROUTE_SURVEYEDCode_New'] = route_code
            df.loc[row.name, 'ROUTE_SURVEYED_NEW'] = df.loc[row.name, 'ROUTE_SURVEYED']
            new_route_name_row = detail_df_stops[detail_df_stops['ETC_ROUTE_ID'] == new_route_code]
            if not new_route_name_row.empty:
                new_route_name = new_route_name_row['ETC_ROUTE_NAME'].iloc[0]
                
                df.loc[row.name, 'ROUTE_SURVEYEDCode_New'] = new_route_code
                df.loc[row.name, 'ROUTE_SURVEYED_NEW'] = new_route_name

                filtered_stop_on_df = detail_df_stops[(detail_df_stops['ETC_ROUTE_ID_SPLITED'] == row['ROUTE_SURVEYEDCode_SPLITED'])&(detail_df_stops['ETC_STOP_DIRECTION']!=stop_on_direction)][
                ['stop_lat6', 'stop_lon6', 'seq_fixed', 'ETC_STOP_ID','ETC_STOP_NAME']
            ]
                filtered_stop_off_df = detail_df_stops[(detail_df_stops['ETC_ROUTE_ID_SPLITED'] == row['ROUTE_SURVEYEDCode_SPLITED'])&(detail_df_stops['ETC_STOP_DIRECTION']!=stop_off_direction)][
                ['stop_lat6', 'stop_lon6', 'seq_fixed', 'ETC_STOP_ID','ETC_STOP_NAME']
            ]

                stop_on_distances = []

                # Calculate distances for all rows in filtered_df
                for _, detail_row in filtered_stop_on_df.iterrows():
                    stop_lat6 = detail_row['stop_lat6']
                    stop_lon6 = detail_row['stop_lon6']

                    # Compute distance
                    stop_on_distance = get_distance_between_coordinates(stop_on_lat, stop_on_long,stop_lat6, stop_lon6)

                    # Skip distance if it is 0
        #             if stop_on_distance == 0:
        #                 continue

                    stop_on_distances.append((stop_on_distance, detail_row['seq_fixed'], detail_row['ETC_STOP_ID'],detail_row['ETC_STOP_NAME'],detail_row['stop_lat6'],detail_row['stop_lon6']))
                # Find the nearest stop (minimum distance)
                if stop_on_distances:
                    nearest_stop_on = min(stop_on_distances, key=lambda x: x[0])  # x[0] is the distance
                    nearest_stop_on_seq.append(nearest_stop_on)
    #             print(f"Nearest stop details for row: {nearest_stop_on_seq}")
                if nearest_stop_on_seq:
                    df.loc[row.name, 'STOP_ON_ADDRESS_NEW'] = nearest_stop_on_seq[0][3]  # ETC_STOP_NAME
                    df.loc[row.name, 'STOP_ON_SEQ'] = nearest_stop_on_seq[0][1]      # seq_fixed
                    df.loc[row.name, 'STOP_ON_CLINTID_NEW'] = nearest_stop_on_seq[0][2]  # ETC_STOP_ID
                    df.loc[row.name, 'STOP_ON_LAT_NEW'] = nearest_stop_on_seq[0][4]      # stop_lat6
                    df.loc[row.name, 'STOP_ON_LONG_NEW'] = nearest_stop_on_seq[0][5]     # stop_lon6
                stop_off_distances = []

                # Calculate distances for all rows in filtered_df
                for _, detail_row in filtered_stop_off_df.iterrows():
                    stop_lat6 = detail_row['stop_lat6']
                    stop_lon6 = detail_row['stop_lon6']

                    # Compute distance
                    stop_off_distance = get_distance_between_coordinates(stop_off_lat, stop_off_long,stop_lat6, stop_lon6)

        #             Skip distance if it is 0
        #             if stop_off_distance == 0:
        #                 continue

                    stop_off_distances.append((stop_off_distance, detail_row['seq_fixed'], detail_row['ETC_STOP_ID'],detail_row['ETC_STOP_NAME'],detail_row['stop_lat6'],detail_row['stop_lon6']))
                # Find the nearest stop (minimum distance)0
                
                if stop_off_distances:
                    nearest_stop_off = min(stop_off_distances, key=lambda x: x[0])  # x[0] is the distance
                    nearest_stop_off_seq.append(nearest_stop_off)

                if nearest_stop_off_seq:
                    df.loc[row.name, 'STOP_OFF_ADDRESS_NEW'] = nearest_stop_off_seq[0][3]  # ETC_STOP_NAME
                    df.loc[row.name, 'STOP_OFF_SEQ'] = nearest_stop_off_seq[0][1]      # seq_fixed
                    df.loc[row.name, 'STOP_OFF_CLINTID_NEW'] = nearest_stop_off_seq[0][2]  # ETC_STOP_ID
                    df.loc[row.name, 'STOP_OFF_LAT_NEW'] = nearest_stop_off_seq[0][4]      # stop_lat6
                    df.loc[row.name, 'STOP_OFF_LONG_NEW'] = nearest_stop_off_seq[0][5]
        else:
            df.loc[row.name, 'ROUTE_SURVEYEDCode_New'] = route_code
            df.loc[row.name, 'ROUTE_SURVEYED_NEW'] = df.loc[row.name, 'ROUTE_SURVEYED']

    # with open(f'{project_name}_SEQUENCE_DIFFERENCEIDS.txt','w') as f:
    #     for item in ids_list:
    #         f.write(f"{item}\n")



    df.drop(columns=['ROUTE_SURVEYEDCode_SPLITED','SEQ_DIFFERENCE'],inplace=True)
    df.drop_duplicates(subset=['id'],inplace=True)
    # df.to_csv(f'reviewtool_{today_date}_{project_name}_ROUTE_DIRECTION_CHECk.csv',index=False)

    print(f'reviewtool_{today_date}_{project_name}_ROUTE_DIRECTION_CHECk CREATED SUCCESSFULLY')

    # print("df.columns", df.columns.tolist())
    # # if we have generated route_direction_database file using route_direction_refator_database.py file then have to replace and rename the columns
    df.drop(columns=['ROUTE_SURVEYEDCode','ROUTE_SURVEYED'],inplace=True)
    df.rename(columns={'ROUTE_SURVEYEDCode_New':'ROUTE_SURVEYEDCode','ROUTE_SURVEYED_NEW':'ROUTE_SURVEYED'},inplace=True) 



    df['ROUTE_SURVEYEDCode_Splited']=df['ROUTE_SURVEYEDCode'].apply(lambda x:('_').join(str(x).split('_')[:-1]) )

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


    df[['id','Day',route_survey_column[0]]].to_csv('Checking Day Names.csv',index=False)


    wkend_overall_df.dropna(subset=['LS_NAME_CODE'],inplace=True)
    wkday_overall_df.dropna(subset=['LS_NAME_CODE'],inplace=True)

 
    if project not in ["UTA", "TUCSON RAIL"]:
        wkend_time_value_df=create_time_value_df_with_display(wkend_overall_df,weekend_df,time_column,project)
        wkday_time_value_df=create_time_value_df_with_display(wkday_overall_df,weekday_df,time_column,project)

    if project=='UTA':
            wkend_route_direction_df=create_uta_route_direction_level_df(wkend_overall_df,weekend_df,time_column,'weekend')
            wkday_route_direction_df=create_uta_route_direction_level_df(wkday_overall_df,weekday_df,time_column,None)        
    else:
        if project in ["TUCSON", "TUCSON RAIL"]:
            wkend_route_direction_df=create_tucson_weekend_route_direction_level_df(wkend_overall_df,weekend_df,time_column,project)
        else:
            wkend_route_direction_df=create_route_direction_level_df(wkend_overall_df,weekend_df,time_column,project)
        wkday_route_direction_df=create_route_direction_level_df(wkday_overall_df,weekday_df,time_column,project)


    if project=='UTA':
        wkend_stationwise_route_df=create_uta_station_wise_route_level_df(wkend_overall_df,weekend_df,time_column,'weekend')
        wkday_stationwise_route_df=create_uta_station_wise_route_level_df(wkday_overall_df,weekday_df,time_column,None)
        
    elif project=='TUCSON RAIL':
        wkend_stationwise_route_df=create_station_wise_route_level_df(wkend_overall_df,weekend_df,time_column)
        wkday_stationwise_route_df=create_station_wise_route_level_df(wkday_overall_df,weekday_df,time_column)
    
    else:
        pass

    weekday_df.dropna(subset=[time_column[0]],inplace=True)
    weekday_raw_df=weekday_df[['id', 'Completed', route_survey_column[0],'ROUTE_SURVEYED',stopon_clntid_column[0],stopoff_clntid_column[0],time_column[0],time_period_column[0],'Day','ELVIS_STATUS']]
    weekend_df.dropna(subset=[time_column[0]],inplace=True)
    weekend_raw_df=weekend_df[['id', 'Completed', route_survey_column[0],'ROUTE_SURVEYED',stopon_clntid_column[0],stopoff_clntid_column[0],time_column[0],time_period_column[0],'Day','ELVIS_STATUS']]
    weekend_raw_df.rename(columns={stopon_clntid_column[0]:'BOARDING LOCATION',stopoff_clntid_column[0]:'ALIGHTING LOCATION'},inplace=True)
    weekday_raw_df.rename(columns={stopon_clntid_column[0]:'BOARDING LOCATION',stopoff_clntid_column[0]:'ALIGHTING LOCATION'},inplace=True)


    wkday_route_level =create_route_level_df(wkday_overall_df,wkday_route_df,weekday_df,time_column,project)
    if project=='TUCSON':
        wkend_route_level =create_wkend_route_level_df(wkend_overall_df,wkend_route_df,weekend_df,time_column,project)
    else:
        wkend_route_level =create_route_level_df(wkend_overall_df,wkend_route_df,weekend_df,time_column,project)
    # wkday_route_df.to_csv("CHECk TOTAL_Difference.csv",index=False)
    # wkend_route_df.to_csv("WKENDCHECk TOTAL_Difference.csv",index=False)
    wkday_comparison_df=copy.deepcopy(wkday_route_level)
    wkday_new_route_level_df=copy.deepcopy(wkday_route_level)

    wkend_comparison_df=copy.deepcopy(wkend_route_level)
    wkend_new_route_level_df=copy.deepcopy(wkend_route_level)
    # this is for time value data
    if project in ["UTA", "TUCSON RAIL"]:
        wkend_time_value_df = create_time_value_df_with_display(wkend_comparison_df,weekend_df,time_column,project)
        wkday_time_value_df = create_time_value_df_with_display(wkday_comparison_df,weekday_df,time_column,project)

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

    elif project=='TUCSON RAIL':
        weekday_raw_df = weekday_df[weekday_df['ROUTE_SURVEYEDCode_Splited'].isin(wkday_comparison_df['ROUTE_SURVEYEDCode'])]
        weekend_raw_df = weekend_df[weekend_df['ROUTE_SURVEYEDCode_Splited'].isin(wkend_comparison_df['ROUTE_SURVEYEDCode'])]


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


        wkday_stationwise_route_df.rename(columns={'CR_AM_Peak':'(1) Goal','CR_Midday':'(2) Goal','CR_PM_Peak':'(3) Goal','CR_Evening':'(4) Goal',
                'DB_AM_Peak':'(1) Collect',
            'DB_Midday':'(2) Collect', 'DB_PM_Peak':'(3) Collect', 'DB_Evening':'(4) Collect', 'AM_DIFFERENCE':'(1) Remain', 'Midday_DIFFERENCE':'(2) Remain',
            'PM_DIFFERENCE':'(3) Remain', 'Evening_DIFFERENCE':'(4) Remain'},inplace=True)

        wkend_stationwise_route_df.rename(columns={'CR_AM_Peak':'(1) Goal','CR_Midday':'(2) Goal','CR_PM_Peak':'(3) Goal','CR_Evening':'(4) Goal',
                'DB_AM_Peak':'(1) Collect',
            'DB_Midday':'(2) Collect', 'DB_PM_Peak':'(3) Collect', 'DB_Evening':'(4) Collect', 'AM_DIFFERENCE':'(1) Remain', 'Midday_DIFFERENCE':'(2) Remain',
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

    elif project=='STL':
        
        wkend_comparison_df.rename(columns={'CR_AM_Peak':'(1) Goal','CR_Midday':'(2) Goal','CR_PM':'(3) Goal','CR_Evening':'(4) Goal',
                'DB_AM_Peak':'(1) Collect',
            'DB_Midday':'(2) Collect', 'DB_PM':'(3) Collect',  'DB_Evening':'(4) Collect', 'AM_DIFFERENCE':'(1) Remain', 'Midday_DIFFERENCE':'(2) Remain',
            'PM_DIFFERENCE':'(3) Remain','Evening_DIFFERENCE':'(4) Remain','CR_Overall_Goal':'Route Level Goal','DB_Total':'# of Surveys','Overall_Goal_DIFFERENCE':'Remaining'},inplace=True)

        wkday_comparison_df.rename(columns={'CR_AM_Peak':'(1) Goal','CR_Midday':'(2) Goal','CR_PM':'(3) Goal','CR_Evening':'(4) Goal',
                'DB_AM_Peak':'(1) Collect',
            'DB_Midday':'(2) Collect', 'DB_PM':'(3) Collect',  'DB_Evening':'(4) Collect',  'AM_DIFFERENCE':'(1) Remain', 'Midday_DIFFERENCE':'(2) Remain',
            'PM_DIFFERENCE':'(3) Remain', 'Evening_DIFFERENCE':'(4) Remain','CR_Overall_Goal':'Route Level Goal','DB_Total':'# of Surveys','Overall_Goal_DIFFERENCE':'Remaining'},inplace=True)

        wkday_route_direction_df.rename(columns={'CR_AM_Peak':'(1) Goal','CR_Midday':'(2) Goal','CR_PM':'(3) Goal','CR_Evening':'(4) Goal',
                'DB_AM_Peak':'(1) Collect',
            'DB_Midday':'(2) Collect', 'DB_PM':'(3) Collect', 'DB_Evening':'(4) Collect','AM_DIFFERENCE':'(1) Remain', 'Midday_DIFFERENCE':'(2) Remain',
            'PM_DIFFERENCE':'(3) Remain', 'Evening_DIFFERENCE':'(4) Remain','CR_Overall_Goal':'Route Level Goal','DB_Total':'# of Surveys','Overall_Goal_DIFFERENCE':'Remaining'},inplace=True)

        wkend_route_direction_df.rename(columns={'CR_AM_Peak':'(1) Goal','CR_Midday':'(2) Goal','CR_PM':'(3) Goal','CR_Evening':'(4) Goal',
                'DB_AM_Peak':'(1) Collect',
            'DB_Midday':'(2) Collect', 'DB_PM':'(3) Collect', 'DB_Evening':'(4) Collect', 'AM_DIFFERENCE':'(1) Remain', 'Midday_DIFFERENCE':'(2) Remain',
            'PM_DIFFERENCE':'(3) Remain', 'Evening_DIFFERENCE':'(4) Remain','CR_Overall_Goal':'Route Level Goal','DB_Total':'# of Surveys','Overall_Goal_DIFFERENCE':'Remaining'},inplace=True)


        wkday_comparison_df = wkday_comparison_df.merge(
            detail_df_xfers[['ETC_ROUTE_ID', 'ETC_ROUTE_NAME']],
            left_on='ROUTE_SURVEYEDCode',
            right_on='ETC_ROUTE_ID',
            how='left'
        )

        interviewer_pivot, route_pivot, detail_table = process_survey_data(baby_elvis_df)

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

    def create_snowflake_connection():
        print("Creating connection with snowflake")
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            private_key=private_key_bytes,
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            authenticator="SNOWFLAKE_JWT",
            schema=schema,
            role=os.getenv('SNOWFLAKE_ROLE'),
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
            'WkEND Route Comparison': wkend_comparison_df.drop(columns=['CR_Total', 'Total_DIFFERENCE']),
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
    elif project=='TUCSON RAIL':
        dataframes={
            'WkDAY Route DIR Comparison':wkday_route_direction_df.drop(columns=['STATION_ID_SPLITTED','CR_Total','Total_DIFFERENCE']),
        'WkEND Route DIR Comparison':wkend_route_direction_df.drop(columns=['STATION_ID_SPLITTED','CR_Total','Total_DIFFERENCE']),
        'WkEND Time Data':wkend_time_value_df,
        'WkDAY Time Data':wkday_time_value_df,

        'WkEND Stationwise Comparison':wkend_stationwise_route_df.drop(columns=['CR_Total','Total_DIFFERENCE','DB_Total']),
        'WkDAY Stationwise Comparison':wkday_stationwise_route_df.drop(columns=['CR_Total','Total_DIFFERENCE','DB_Total']),

        # wkday_comparison_df.to_excel(writer,sheet_name='WkDAY Route Comparison',index=False)
        'WkDAY Route Comparison':wkday_comparison_df.drop(columns=['CR_Total','DB_AM_IDS','DB_Midday_IDS','DB_PM_IDS','DB_Evening_IDS','Total_DIFFERENCE']),
       'WkEND Route Comparison':wkend_comparison_df.drop(columns=['CR_Total','DB_AM_IDS','DB_Midday_IDS','DB_PM_IDS','DB_Evening_IDS','Total_DIFFERENCE']),
        
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
        
    elif project=='STL':
        # DataFrames preparation
        dataframes = {
            'WkDAY Route DIR Comparison': wkday_route_direction_df.drop(columns=['CR_Total','Total_DIFFERENCE']),
            'WkEND Route DIR Comparison': wkend_route_direction_df.drop(columns=['CR_Total','Total_DIFFERENCE']),
            'WkDAY RAW DATA': weekday_raw_df,
            'WkEND RAW DATA': weekend_raw_df,
            'WkEND Time Data': wkend_time_value_df,
            'WkDAY Time Data': wkday_time_value_df,
            'WkDAY Route Comparison': wkday_comparison_df.drop(columns=['CR_Total','Total_DIFFERENCE']),
            'WkEND Route Comparison': wkend_comparison_df.drop(columns=['CR_Total', 'Total_DIFFERENCE']),
            'LAST SURVEY DATE': latest_date_df,
            'By_Interviewer': interviewer_pivot,
            'By_Route': route_pivot,
            'Survey_Detail': detail_table,
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
            'LAST SURVEY DATE': 'last_survey_date',
            'By_Interviewer': 'by_interv_totals',
            'By_Route': 'by_route_totals',
            'Survey_Detail': 'survey_detail_totals',
        }

    # Call the function
    print("Final call")
    create_tables_and_insert_data(dataframes, table_info)

    print("Files Uploaded SuccessFully")