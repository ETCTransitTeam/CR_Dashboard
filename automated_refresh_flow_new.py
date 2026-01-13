import pandas as pd
import numpy as np
from datetime import date
import warnings
import copy
import os
from dotenv import load_dotenv
import math
import streamlit as st
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import boto3
from io import BytesIO
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from automated_sync_flow_utils import *
from automated_sync_flow_constants_maps import KCATA_HEADER_MAPPING
from utils import fetch_data

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
            'kingelvis':'STL_MO_2025_KINGElvis.xlsx'
        }
    },
    "KCATA": {
        "databases": {
                    "elvis": {
                        "database": os.getenv("KCATA_ELVIS_DATABASE_NAME"),
                        "table": os.getenv("KCATA_ELVIS_TABLE_NAME")
                    },
                    "baby_elvis": {
                        "database": os.getenv("KCATA_BABY_ELVIS_DATABASE_NAME"),
                        "table": os.getenv("KCATA_BABY_ELVIS_TABLE_NAME")
                    }
                },
        "files": {
            "details": "details_KCATA_od_excel.xlsx",
            "cr": "KCATA_MO_CR_UPDATE.xlsx",
            'kingelvis':'KCATA_2025_KINGElvis.xlsx'
        }
    },
    "KCATA RAIL": {
        "databases": {
                    "elvis": {
                        "database": os.getenv("KCATA_ELVIS_DATABASE_NAME"),
                        "table": os.getenv("KCATA_ELVIS_TABLE_NAME")
                    },
                    "baby_elvis": {
                        "database": os.getenv("KCATA_BABY_ELVIS_DATABASE_NAME"),
                        "table": os.getenv("KCATA_BABY_ELVIS_TABLE_NAME")
                    }
                },
        "files": {
            "details": "details_KCATA_od_excel.xlsx",
            "cr": "KCATA_MO_CR_UPDATE.xlsx",
            'kingelvis':'KCATA_2025_KINGElvis.xlsx'
        }
    },
    "ACTRANSIT": {
        "databases": {
                    "elvis": {
                        "database": os.getenv("ACTRANSIT_ELVIS_DATABASE_NAME"),
                        "table": os.getenv("ACTRANSIT_ELVIS_TABLE_NAME")
                    },
                    "main": {
                        "database": os.getenv("ACTRANSIT_BABY_ELVIS_DATABASE_NAME"),
                        "table": os.getenv("ACTRANSIT_BABY_ELVIS_TABLE_NAME")
                    }
                },
        "files": {
            "details": "details_AC_Transit_od_excel.xlsx",
            "cr": "ACTRANSIT_CA_CR.xlsx",
            'kingelvis':'ACT_2025_KINGElvis.xlsx'
        }
    }
    ,
    "SALEM": {
        "databases": {
                    "elvis": {
                        "database": os.getenv("SALEM_ELVIS_DATABASE_NAME"),
                        "table": os.getenv("SALEM_ELVIS_TABLE_NAME")
                    },
                    "main": {
                        "database": os.getenv("SALEM_BABY_ELVIS_DATABASE_NAME"),
                        "table": os.getenv("SALEM_BABY_ELVIS_TABLE_NAME")
                    }
                },
        "files": {
            "details": "details_project_od_Salem.xlsx",
            "cr": "SALEM_OR_CR_UPDATE.xlsx",
            'kingelvis':'SALEM_OR_2025_KINGElvis.xlsx'
        }
    }
    ,
    "LACMTA_FEEDER": {
        "databases": {
                    "elvis": {
                        "database": os.getenv("LACMTA_FEEDER_ELVIS_DATABASE_NAME"),
                        "table": os.getenv("LACMTA_FEEDER_ELVIS_TABLE_NAME")
                    },
                    "main": {
                        "database": os.getenv("LACMTA_FEEDER_BABY_ELVIS_DATABASE_NAME"),
                        "table": os.getenv("LACMTA_FEEDER_BABY_ELVIS_TABLE_NAME")
                    }
                },
        "files": {
            "details": "details_lacmta-feeder_733524_od_excel.xlsx",
            "cr": "LACMTA_FEEDER_CR.xlsx",
            'kingelvis':'LACMTA_FEEDER_2025_KINGElvis.xlsx'
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
    
    # Fetch data from both databases only once
    elvis_config = project_config['databases']["elvis"]
    table_name = elvis_config['table']
    database_name = elvis_config["database"]

    main_config = project_config['databases'].get("main", None)
    main_table_name = main_config["table"] if main_config else None
    main_database_name = main_config["database"] if main_config else None

    # -----------------------
    # Fetch fresh data every run (do NOT store raw data in session)
    # -----------------------
    # csv_buffer = fetch_data(database_name, table_name)
    # if csv_buffer:
    #     df = pd.read_csv(csv_buffer)
    # else:
    #     st.error("Failed to load elvis data.")
    #     df = None

    # df1 = None
    # if main_config:
    #     main_csv_buffer = fetch_data(main_database_name, main_table_name)
    #     if main_csv_buffer:
    #         df1 = pd.read_csv(main_csv_buffer)
    #     else:
    #         st.error("Failed to load main data.")

    # # -----------------------
    # # Process elvis data
    # # -----------------------
    # if df is not None:
    #     if project in ["KCATA", "KCATA RAIL", "ACTRANSIT", "SALEM"]:
    #         df.columns = df.columns.str.strip()
    #         df = df.rename(columns=KCATA_HEADER_MAPPING)
    #         elvis_df = df.drop(index=0).reset_index(drop=True)

    #     # Filtering & cleaning
    #     time_value_code_check = ['have5minforsurvecode']
    #     route_surveyed_code_check = ['routesurveyedcode']
    #     route_surveyed_code = check_all_characters_present(df, route_surveyed_code_check)
    #     time_value_code_df = check_all_characters_present(df, time_value_code_check)

    #     df[time_value_code_df[0]] = df[time_value_code_df[0]].astype(str)
    #     df['INTERV_INIT'] = df['INTERV_INIT'].astype(str)
    #     df = df[df[time_value_code_df[0]] == '1']
    #     df = df[df['INTERV_INIT'] != '999']

    #     elvis_status_column_check = ['elvisstatus']
    #     elvis_status_column = check_all_characters_present(df, elvis_status_column_check)
    #     df = df[df[elvis_status_column[0]].str.lower() != 'delete']

    #     df.drop_duplicates(subset='id', inplace=True)
    #     time_column_check = ['timeoncode']
    #     time_period_column_check = ['timeon']
    #     df.rename(columns={route_surveyed_code[0]: 'ROUTE_SURVEYEDCode'}, inplace=True)

    #     time_column_df = check_all_characters_present(df, time_column_check)
    #     time_period_column_df = check_all_characters_present(df, time_period_column_check)
    # else:
    #     st.warning("No data available. Click 'Fetch Data' to load the dataset.")

    # # -----------------------
    # # Process main data
    # # -----------------------
    # if df1 is not None:
    #     column_mapping = {}
    #     for df1_col in df1.columns:
    #         cleaned_df1_col = clean_string(df1_col)
    #         for df_col in df.columns:
    #             if cleaned_df1_col == clean_string(df_col):
    #                 column_mapping[df1_col] = df_col
    #                 break

    #     df1 = df1.rename(columns=column_mapping)

    #     time_column_df1 = check_all_characters_present(df1, ['timeoncode'])
    #     time_period_column_df1 = check_all_characters_present(df1, ['timeon'])

    # # -----------------------
    # # Merge logic
    # # -----------------------
    # baby_elvis_df_merged = None
    # if df is not None and df1 is not None:
    #     df3 = df.copy()
    #     missing_ids = set(df1['id']) - set(df['id'])
    #     df1_new = df1[df1['id'].isin(missing_ids)]
    #     df = pd.concat([df, df1_new], ignore_index=True)
    #     df.drop_duplicates(subset=['id'], inplace=True)
    #     df = df.sort_values('id').reset_index(drop=True)

    #     # Fill missing Time_ONCode values
    #     mask = df[time_column_df[0]].isna() | (df[time_column_df[0]].str.strip() == '')
    #     time_mapping = dict(zip(df1['id'], df1[time_column_df1[0]]))
    #     df.loc[mask, time_column_df[0]] = df.loc[mask, 'id'].map(time_mapping)

    #     baby_elvis_df_merged = df
    #     print("Data merged successfully!")

    # # -----------------------
    # # Prepare baby_elvis_df (already fetched in df1)
    # # -----------------------
    # baby_elvis_df = None
    # if "main" in PROJECTS[project]["databases"]:
    #     baby_elvis_config = PROJECTS[project]["databases"]["main"]
    #     baby_table_name = baby_elvis_config['table']
    #     baby_database_name = baby_elvis_config["database"]

    #     # Use df1 as the baby_elvis_df
    #     if df1 is not None:
    #         baby_elvis_df = df1.copy()

    #     if project in ["KCATA", "KCATA RAIL", "ACTRANSIT", "SALEM"] and baby_elvis_df is not None:
    #         baby_elvis_df.columns = baby_elvis_df.columns.str.strip()
    #         baby_elvis_df = baby_elvis_df.rename(columns=KCATA_HEADER_MAPPING)

    # # -----------------------
    # # Store only the final merged & cleaned dataframe in session_state
    # # -----------------------
    # # st.session_state.merged_clean_df = baby_elvis_df_merged

    # # Display success
    # if baby_elvis_df_merged is not None:
    #     st.success(f"Collected {len(baby_elvis_df_merged)} records from baby_elvis and elvis 📊 … now normalizing, cleaning, and reshaping the dataset ⏳💪")
        # -----------------------
    # Fetch fresh data every run (do NOT store raw data in session)
    # -----------------------
    csv_buffer = fetch_data(database_name, table_name)
    if csv_buffer:
        df = pd.read_csv(csv_buffer)
    else:
        st.error("Failed to load elvis data.")
        df = None

    df1 = None
    if main_config:
        main_csv_buffer = fetch_data(main_database_name, main_table_name)
        if main_csv_buffer:
            df1 = pd.read_csv(main_csv_buffer)
        else:
            st.error("Failed to load main data.")

    # -----------------------
    # Step 1: Merge df and df1
    # -----------------------
    merged_df = None
    if df is not None and df1 is not None:
        # Align columns between df1 and df before merging
        column_mapping = {}
        for df1_col in df1.columns:
            cleaned_df1_col = clean_string(df1_col)
            for df_col in df.columns:
                if cleaned_df1_col == clean_string(df_col):
                    column_mapping[df1_col] = df_col
                    break
        
        df1 = df1.rename(columns=column_mapping)
        
        # Merge logic
        df3 = df.copy()  # Keep original df copy as df3
        missing_ids = set(df1['id']) - set(df['id'])
        df1_new = df1[df1['id'].isin(missing_ids)]
        merged_df = pd.concat([df, df1_new], ignore_index=True)
        merged_df.drop_duplicates(subset=['id'], inplace=True)
        merged_df = merged_df.sort_values('id').reset_index(drop=True)
        print("Data merged successfully!")

    # -----------------------
    # Step 2: Apply header mapping to merged dataframe
    # -----------------------
    if merged_df is not None and project in ["KCATA", "KCATA RAIL", "ACTRANSIT", "SALEM", "LACMTA_FEEDER"]:
        merged_df.columns = merged_df.columns.str.strip()
        merged_df = merged_df.rename(columns=KCATA_HEADER_MAPPING)
        # Remove first row if it exists (as in original code)
        merged_df = merged_df.drop(index=0).reset_index(drop=True)

    # -----------------------
    # Step 3: Create baby_elvis_df for refusal analysis (NO CLEANING)
    # -----------------------
    # This should be the merged data BEFORE any cleaning/filtering
    baby_elvis_df = merged_df.copy() if merged_df is not None else None
    
    # Apply header mapping to baby_elvis_df if needed (but NO cleaning)
    if baby_elvis_df is not None and project in ["KCATA", "KCATA RAIL", "ACTRANSIT", "SALEM", "LACMTA_FEEDER"]:
        baby_elvis_df.columns = baby_elvis_df.columns.str.strip()
        baby_elvis_df = baby_elvis_df.rename(columns=KCATA_HEADER_MAPPING)
        baby_elvis_df = baby_elvis_df.drop(index=0).reset_index(drop=True)

    # -----------------------
    # Step 4: Process and clean df for main analysis
    # -----------------------
    # df will be the cleaned version for main analysis
    df = merged_df.copy() if merged_df is not None else None
    
    if df is not None:
        # Get required column names
        time_value_code_check = ['have5minforsurvecode']
        route_surveyed_code_check = ['routesurveyedcode']
        elvis_status_column_check = ['elvisstatus']
        time_column_check = ['timeoncode']
        time_period_column_check = ['timeon']
        
        route_surveyed_code = check_all_characters_present(df, route_surveyed_code_check)
        time_value_code_df = check_all_characters_present(df, time_value_code_check)
        elvis_status_column = check_all_characters_present(df, elvis_status_column_check)
        time_column_df = check_all_characters_present(df, time_column_check)
        time_period_column_df = check_all_characters_present(df, time_period_column_check)
        
        # Apply filters and cleaning (SAME AS YOUR ORIGINAL CODE)
        df[time_value_code_df[0]] = df[time_value_code_df[0]].astype(str)
        df['INTERV_INIT'] = df['INTERV_INIT'].astype(str)
        df = df[df[time_value_code_df[0]] == '1']
        df = df[df['INTERV_INIT'] != '999']
        
        if elvis_status_column:
            df = df[df[elvis_status_column[0]].str.lower() != 'delete']
        
        df.drop_duplicates(subset='id', inplace=True)
        
        # Rename route surveyed column
        if route_surveyed_code:
            df.rename(columns={route_surveyed_code[0]: 'ROUTE_SURVEYEDCode'}, inplace=True)
        
        # Fill missing Time_ONCode values from original df1
        if time_column_df and df1 is not None:
            time_column_df1 = check_all_characters_present(df1, ['timeoncode'])
            if time_column_df1:
                mask = df[time_column_df[0]].isna() | (df[time_column_df[0]].str.strip() == '')
                time_mapping = dict(zip(df1['id'], df1[time_column_df1[0]]))
                df.loc[mask, time_column_df[0]] = df.loc[mask, 'id'].map(time_mapping)

    # -----------------------
    # Step 5: Create other required variables
    # -----------------------
    # baby_elvis_df_merged - cleaned merged dataframe (same as df)
    baby_elvis_df_merged = df.copy() if df is not None else None
    
    # elvis_df - copy of cleaned dataframe (for backward compatibility)
    elvis_df = df.copy() if df is not None else None
    
    # Also keep time_column_df, time_period_column_df variables available
    # These were already created in Step 4
    
    # For df1 time columns (if needed later)
    time_column_df1 = None
    time_period_column_df1 = None
    if df1 is not None:
        time_column_df1 = check_all_characters_present(df1, ['timeoncode'])
        time_period_column_df1 = check_all_characters_present(df1, ['timeon'])

    # -----------------------
    # Display success
    # -----------------------
    if baby_elvis_df_merged is not None:
        st.success(f"Collected {len(baby_elvis_df_merged)} records from baby_elvis and elvis 📊 … now normalizing, cleaning, and reshaping the dataset ⏳💪")


    bucket_name = os.getenv('bucket_name')
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('aws_access_key_id'),
        aws_secret_access_key=os.getenv('aws_secret_access_key')
    )

    # Now ALL your original variables are available:
    # df, df1, elvis_df, baby_elvis_df, baby_elvis_df_merged
    # time_column_df, time_period_column_df, time_column_df1, time_period_column_df1
    # And all other variables you had in your original code

    # Function to read an Excel file from S3 into a DataFrame
    def read_excel_from_s3(bucket_name, file_key, sheet_name):
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        excel_data = response['Body'].read()
        return pd.read_excel(BytesIO(excel_data), sheet_name=sheet_name)

    if project=='KCATA':
        ke_df = read_excel_from_s3(bucket_name,project_config["files"]["kingelvis"], 'Elvis_Review')

        detail_df_stops = read_excel_from_s3(bucket_name,project_config["files"]["details"], 'STOPS')
        detail_df_xfers = read_excel_from_s3(bucket_name, project_config["files"]["details"], 'XFERS')

        wkend_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkEND-Overall')
        wkend_route_df = read_excel_from_s3(bucket_name,project_config["files"]["cr"], 'WkEND-RouteTotal')

        wkday_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-Overall')
        wkday_route_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-RouteTotal')

        print("Files read for KCATA")
        
    elif project=='ACTRANSIT':
        ke_df = read_excel_from_s3(bucket_name,project_config["files"]["kingelvis"], 'Elvis_Review')

        detail_df_stops = read_excel_from_s3(bucket_name,project_config["files"]["details"], 'STOPS')
        stops_df = detail_df_stops.copy()
        detail_df_xfers = read_excel_from_s3(bucket_name, project_config["files"]["details"], 'XFERS')

        wkend_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkEND-Overall')
        wkend_route_df = read_excel_from_s3(bucket_name,project_config["files"]["cr"], 'WkEND-RouteTotal')

        wkday_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-Overall')
        wkday_route_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-RouteTotal')

        print("Files read for ACTRANSIT from S3")

    elif project=='SALEM':
        ke_df = read_excel_from_s3(bucket_name,project_config["files"]["kingelvis"], 'Elvis_Review')

        detail_df_stops = read_excel_from_s3(bucket_name,project_config["files"]["details"], 'STOPS')
        stops_df = detail_df_stops.copy()
        detail_df_xfers = read_excel_from_s3(bucket_name, project_config["files"]["details"], 'XFERS')

        wkend_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkEND-Overall')
        wkend_route_df = read_excel_from_s3(bucket_name,project_config["files"]["cr"], 'WkEND-RouteTotal')

        wkday_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-Overall')
        wkday_route_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-RouteTotal')

        print("Files read for SALEM from S3")

    elif project=='LACMTA_FEEDER':
        ke_df = read_excel_from_s3(bucket_name,project_config["files"]["kingelvis"], 'Elvis_Review')

        detail_df_stops = read_excel_from_s3(bucket_name,project_config["files"]["details"], 'STOPS')
        stops_df = detail_df_stops.copy()
        detail_df_xfers = read_excel_from_s3(bucket_name, project_config["files"]["details"], 'XFERS')

        wkend_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkEND-Overall')
        wkend_route_df = read_excel_from_s3(bucket_name,project_config["files"]["cr"], 'WkEND-RouteTotal')

        wkday_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-Overall')
        wkday_route_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-RouteTotal')

        print("Files read for SALEM from S3")

    elif project=='KCATA RAIL':
        ke_df = read_excel_from_s3(bucket_name,project_config["files"]["kingelvis"], 'Elvis_Review')

        detail_df_stops = read_excel_from_s3(bucket_name,project_config["files"]["details"], 'STOPS')
        detail_df_xfers = read_excel_from_s3(bucket_name, project_config["files"]["details"], 'XFERS')

        wkend_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkEND-RAIL')
        wkend_route_df = read_excel_from_s3(bucket_name,project_config["files"]["cr"], 'WkEND-RailTotal')

        wkday_overall_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-RAIL')
        wkday_route_df = read_excel_from_s3(bucket_name, project_config["files"]["cr"], 'WkDAY-RailTotal')

        print("Files read for KCATA RAIL")
        ke_df['1st Cleaner'] = ke_df['1st Cleaner'].astype(str)
        ke_df['INTERV_INIT'] = ke_df['INTERV_INIT'].astype(str)
        ke_df=ke_df[ke_df['INTERV_INIT']!='999']
        # ke_df=ke_df[ke_df['INTERV_INIT']!=999]
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


    print("Filtered df data =", len(df))

    # Safe Final_Usage filtering
    fu = ke_df['Final_Usage'].astype(str).str.strip().str.lower()

    # Get IDs to EXCLUDE (Remove or No Data)
    exclude_ids = ke_df[
        fu.isin(['remove', 'no data'])
    ]['elvis_id' if 'elvis_id' in ke_df.columns else 'id'].unique()
    
        # Get IDs to INCLUDE (everything except Remove/No Data)
        # This includes: Use, Empty, and any other values
    include_ids = ke_df[
        ~fu.isin(['remove', 'no data'])
    ]['elvis_id' if 'elvis_id' in ke_df.columns else 'id'].unique()
        
    print(f"KingElvis - Excluding {len(exclude_ids)} records (Remove/No Data)")
    print(f"KingElvis - Including {len(include_ids)} records (everything else)")
    print(f"Total unique IDs in kingelvis: {len(ke_df)}")

    # Apply kingelvis filter to ALL records
    if ke_df is not None and df is not None:
        # Method 1: Exclude Remove/No Data IDs
        df = df[~df['id'].isin(exclude_ids)]
    
        # OR Method 2: Include all except Remove/No Data (same result)
        # df = df[df['id'].isin(include_ids)]
    
    print(f"After KingElvis filter (exclude Remove/No Data): {len(df)} records")

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
    # detail_df_stops[['ETC_ROUTE_ID_SPLITED']].head(2)



    detail_df_stops['ETC_STOP_DIRECTION']=detail_df_stops['ETC_STOP_ID'].apply(lambda x : str(x).split('_')[-2])
    # detail_df_stops[['ETC_STOP_DIRECTION']].head(2)
    import time
    # Start the timer
    start_time = time.time()
    # -------------------------------------------------------------
    # ✅ STOP_ON: Assign nearest stop for each survey point (MATCHING OLD CODE)
    # -------------------------------------------------------------
    for i, row in df.iterrows():
        route_code = row['ROUTE_SURVEYEDCode']
        
        # Check if route exists in detail_df_stops (same as old code validation)
        if route_code not in detail_df_stops['ETC_ROUTE_ID'].values:
            continue

        stop_on_lat = row[stop_on_lat_lon_columns[0]]
        stop_on_long = row[stop_on_lat_lon_columns[1]]
        
        # Same filtering logic as old code
        filtered_df = detail_df_stops[
            detail_df_stops['ETC_ROUTE_ID'] == route_code
        ][['stop_lat6', 'stop_lon6', 'seq_fixed', 'ETC_STOP_ID', 'ETC_STOP_NAME']]

        if filtered_df.empty:
            continue

        # Vectorized distance calculation for speed
        distances = haversine_distance(
            stop_on_lat,
            stop_on_long,
            filtered_df['stop_lat6'].values,
            filtered_df['stop_lon6'].values,
        )

        # Find nearest stop
        idx_min = np.nanargmin(distances)
        nearest = filtered_df.iloc[idx_min]

        # Use EXACT same column names as old code
        df.loc[i, 'STOP_ON_ADDR_NEW'] = nearest['ETC_STOP_NAME']
        df.loc[i, 'STOP_ON_SEQ'] = nearest['seq_fixed']
        df.loc[i, 'STOP_ON_CLINTID_NEW'] = nearest['ETC_STOP_ID']
        df.loc[i, 'STOP_ON_LAT_NEW'] = nearest['stop_lat6']
        df.loc[i, 'STOP_ON_LONG_NEW'] = nearest['stop_lon6']

    # -------------------------------------------------------------
    # ✅ STOP_OFF: Assign nearest stop in same route & direction (MATCHING OLD CODE)
    # -------------------------------------------------------------
    for i, row in df.iterrows():
        stop_off_lat = row[stop_off_lat_lon_columns[0]]
        stop_off_long = row[stop_off_lat_lon_columns[1]]

        # Same validation as old code
        if pd.isna(stop_off_lat) or pd.isna(stop_off_long):
            continue

        route_code = row['ROUTE_SURVEYEDCode']
        stop_on_clintid = row.get('STOP_ON_CLINTID_NEW')
        
        # Same direction extraction logic as old code
        if not stop_on_clintid or '_' not in str(stop_on_clintid):
            continue

        stop_on_direction = str(stop_on_clintid).split('_')[-2]

        # Same filtering logic as old code with direction constraint
        filtered_df = detail_df_stops[
            (detail_df_stops['ETC_ROUTE_ID'] == route_code)
            & (detail_df_stops['ETC_STOP_DIRECTION'] == stop_on_direction)
        ][['stop_lat6', 'stop_lon6', 'seq_fixed', 'ETC_STOP_ID', 'ETC_STOP_NAME']]

        if filtered_df.empty:
            continue

        # Vectorized distance calculation
        distances = haversine_distance(
            stop_off_lat,
            stop_off_long,
            filtered_df['stop_lat6'].values,
            filtered_df['stop_lon6'].values,
        )

        idx_min = np.nanargmin(distances)
        nearest = filtered_df.iloc[idx_min]

        # Use EXACT same column names as old code
        df.loc[i, 'STOP_OFF_ADDRESS_NEW'] = nearest['ETC_STOP_NAME']
        df.loc[i, 'STOP_OFF_SEQ'] = nearest['seq_fixed']
        df.loc[i, 'STOP_OFF_CLINTID_NEW'] = nearest['ETC_STOP_ID']
        df.loc[i, 'STOP_OFF_LAT_NEW'] = nearest['stop_lat6']
        df.loc[i, 'STOP_OFF_LONG_NEW'] = nearest['stop_lon6']

    # -------------------------------------------------------------
    # ✅ Handle reverse route case when SEQ difference < 0 (MATCHING OLD CODE LOGIC)
    # -------------------------------------------------------------
    df['SEQ_DIFFERENCE'] = df['STOP_OFF_SEQ'] - df['STOP_ON_SEQ']
    ids_list = []

    for i, row in df.iterrows():
        route_code = row['ROUTE_SURVEYEDCode']
        
        # Same condition as old code
        if row['SEQ_DIFFERENCE'] >= 0:
            df.loc[i, 'ROUTE_SURVEYEDCode_New'] = route_code
            df.loc[i, 'ROUTE_SURVEYED_NEW'] = row['ROUTE_SURVEYED']
            continue

        ids_list.append(row['id'])

        # Get coordinates from newly assigned stops (same as old code)
        stop_on_lat = row['STOP_ON_LAT_NEW']
        stop_on_long = row['STOP_ON_LONG_NEW']
        stop_off_lat = row['STOP_OFF_LAT_NEW']
        stop_off_long = row['STOP_OFF_LONG_NEW']
        
        # Same direction extraction logic as old code
        stop_on_direction = str(row['STOP_ON_CLINTID_NEW']).split('_')[-2] if len(str(row['STOP_ON_CLINTID_NEW']).split('_')) >= 2 else None
        stop_off_direction = str(row['STOP_OFF_CLINTID_NEW']).split('_')[-2] if len(str(row['STOP_OFF_CLINTID_NEW']).split('_')) >= 2 else None
        
        if stop_on_direction is None or stop_off_direction is None:
            continue

        # Same route code transformation logic as old code
        new_route_code = (
            f"{'_'.join(route_code.split('_')[:-1])}_01" 
            if route_code.split('_')[-1] == '00' 
            else f"{'_'.join(route_code.split('_')[:-1])}_00"
        )
        
        # Update route information (same as old code)
        df.loc[i, 'ROUTE_SURVEYEDCode_New'] = route_code  # Temporary assignment
        df.loc[i, 'ROUTE_SURVEYED_NEW'] = row['ROUTE_SURVEYED']  # Temporary assignment
        
        new_route_name_row = detail_df_stops[detail_df_stops['ETC_ROUTE_ID'] == new_route_code]
        if not new_route_name_row.empty:
            new_route_name = new_route_name_row['ETC_ROUTE_NAME'].iloc[0]
            
            df.loc[i, 'ROUTE_SURVEYEDCode_New'] = new_route_code
            df.loc[i, 'ROUTE_SURVEYED_NEW'] = new_route_name

            # SAME FILTERING LOGIC AS OLD CODE - using ETC_ROUTE_ID_SPLITED and opposite direction
            filtered_stop_on_df = detail_df_stops[
                (detail_df_stops['ETC_ROUTE_ID_SPLITED'] == row['ROUTE_SURVEYEDCode_SPLITED'])
                & (detail_df_stops['ETC_STOP_DIRECTION'] != stop_on_direction)
            ][['stop_lat6', 'stop_lon6', 'seq_fixed', 'ETC_STOP_ID', 'ETC_STOP_NAME']]

            if not filtered_stop_on_df.empty:
                # Vectorized distance calculation
                stop_on_dist = haversine_distance(
                    stop_on_lat,
                    stop_on_long,
                    filtered_stop_on_df['stop_lat6'].values,
                    filtered_stop_on_df['stop_lon6'].values,
                )
                nearest_on = filtered_stop_on_df.iloc[np.nanargmin(stop_on_dist)]
                
                # Update STOP_ON with EXACT same column names as old code
                df.loc[i, 'STOP_ON_ADDR_NEW'] = nearest_on['ETC_STOP_NAME']  # Note: ADDR not ADDRESS
                df.loc[i, 'STOP_ON_SEQ'] = nearest_on['seq_fixed']
                df.loc[i, 'STOP_ON_CLINTID_NEW'] = nearest_on['ETC_STOP_ID']
                df.loc[i, 'STOP_ON_LAT_NEW'] = nearest_on['stop_lat6']
                df.loc[i, 'STOP_ON_LONG_NEW'] = nearest_on['stop_lon6']

            # SAME FILTERING LOGIC AS OLD CODE - using ETC_ROUTE_ID_SPLITED and opposite direction  
            filtered_stop_off_df = detail_df_stops[
                (detail_df_stops['ETC_ROUTE_ID_SPLITED'] == row['ROUTE_SURVEYEDCode_SPLITED'])
                & (detail_df_stops['ETC_STOP_DIRECTION'] != stop_off_direction)
            ][['stop_lat6', 'stop_lon6', 'seq_fixed', 'ETC_STOP_ID', 'ETC_STOP_NAME']]

            if not filtered_stop_off_df.empty:
                # Vectorized distance calculation
                stop_off_dist = haversine_distance(
                    stop_off_lat,
                    stop_off_long,
                    filtered_stop_off_df['stop_lat6'].values,
                    filtered_stop_off_df['stop_lon6'].values,
                )
                nearest_off = filtered_stop_off_df.iloc[np.nanargmin(stop_off_dist)]
                
                # Update STOP_OFF with EXACT same column names as old code
                df.loc[i, 'STOP_OFF_ADDRESS_NEW'] = nearest_off['ETC_STOP_NAME']
                df.loc[i, 'STOP_OFF_SEQ'] = nearest_off['seq_fixed']
                df.loc[i, 'STOP_OFF_CLINTID_NEW'] = nearest_off['ETC_STOP_ID']
                df.loc[i, 'STOP_OFF_LAT_NEW'] = nearest_off['stop_lat6']
                df.loc[i, 'STOP_OFF_LONG_NEW'] = nearest_off['stop_lon6']

    # -------------------------------------------------------------
    # ✅ FINAL CLEANUP
    # -------------------------------------------------------------

    # with open(f'{project_name}_SEQUENCE_DIFFERENCEIDS.txt','w') as f:
    #     for item in ids_list:
    #         f.write(f"{item}\n")

    df.drop(columns=['ROUTE_SURVEYEDCode_SPLITED','SEQ_DIFFERENCE'],inplace=True)
    df.drop_duplicates(subset=['id'],inplace=True)


    # # if we have generated route_direction_database file using route_direction_refator_database.py file then have to replace and rename the columns
    df.drop(columns=['ROUTE_SURVEYEDCode','ROUTE_SURVEYED'],inplace=True)
    df.rename(columns={'ROUTE_SURVEYEDCode_New':'ROUTE_SURVEYEDCode','ROUTE_SURVEYED_NEW':'ROUTE_SURVEYED'},inplace=True) 
    # End the timer
    end_time = time.time()

    total_time = end_time - start_time
    print(f"Total time taken: {total_time:.2f} seconds")


    df['ROUTE_SURVEYEDCode_Splited']=df['ROUTE_SURVEYEDCode'].apply(lambda x:('_').join(str(x).split('_')[:-1]) )

    date_columns_check=['completed','datestarted']
    date_columns=check_all_characters_present(df,date_columns_check)

    # def determine_date(row):
    #     if not pd.isnull(row[date_columns[0]]):
    #         return row[date_columns[0]]
    #     elif not pd.isnull(row[date_columns[1]]):
    #         return row[date_columns[1]]
    #     else:
    #         return pd.NaT

    print("✅ Using LocalTime column for date classification")
    df['Date'] = pd.to_datetime(df['LocalTime'], errors='coerce')
    df['Day'] = df['Date'].dt.day_name()

    # Fill any NaN values
    df['Day'] = df['Day'].fillna('Unknown')
    unknown_count = len(df[df['Day'] == 'Unknown'])
    print("Day = Unknown Count",unknown_count)

    try:
        df['LAST_SURVEY_DATE'] = pd.to_datetime(df['Date'], format='%d/%m/%Y %H:%M', errors='coerce')
    except Exception as e:
        print(f"Error encountered: {e}")
        df['LAST_SURVEY_DATE'] = pd.to_datetime(df['Date'], errors='coerce', dayfirst=True)
    latest_date = df['LAST_SURVEY_DATE'].max()
    latest_date_df = pd.DataFrame({'Latest_Survey_Date': [latest_date]})


    weekend_df=df[df['Day'].isin(['Saturday','Sunday'])]
    print("Weekend DF length:", len(weekend_df))
    weekday_df=df[~(df['Day'].isin(['Saturday','Sunday']))]
    print("Weekday DF length:", len(weekday_df))
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


    wkend_overall_df.dropna(subset=['LS_NAME_CODE'],inplace=True)
    wkday_overall_df.dropna(subset=['LS_NAME_CODE'],inplace=True)

 
    if project not in ["KCATA RAIL"]:
        wkend_time_value_df=create_time_value_df_with_display(wkend_overall_df,weekend_df,time_column,project)
        wkday_time_value_df=create_time_value_df_with_display(wkday_overall_df,weekday_df,time_column,project)
    
    # ----- Route Direction DF -----
    # if project in ["ACTRANSIT", "SALEM", "KCATA", "LACMTA_FEEDER"]:
    #     wkend_route_direction_df = create_route_direction_level_df(wkend_overall_df, weekend_df, time_column, project)
    #     wkday_route_direction_df = create_route_direction_level_df(wkday_overall_df, weekday_df, time_column, project)
    # else:
    print("Creating weekend route direction df for other projects")
    wkend_route_direction_df = create_route_direction_level_df(wkend_overall_df, weekend_df, time_column, project)
    wkday_route_direction_df = create_route_direction_level_df(wkday_overall_df, weekday_df, time_column, project)

    # ----- Station-wise Route DF -----
    if project=='KCATA RAIL':
        wkend_stationwise_route_df=create_station_wise_route_level_df_kcata(wkend_overall_df,weekend_df,time_column)
        wkday_stationwise_route_df=create_station_wise_route_level_df_kcata(wkday_overall_df,weekday_df,time_column)

    else:
        pass


    # if project=='KCATA' or project=='KCATA RAIL' or project=='ACTRANSIT' or project=='SALEM':
    #     weekday_df.dropna(subset=[time_column[0]],inplace=True)
    #     weekday_raw_df=weekday_df[['id', 'DATE_SUBMITTED', route_survey_column[0],'ROUTE_SURVEYED',stopon_clntid_column[0],stopoff_clntid_column[0],time_column[0],time_period_column[0],'Day','ElvisStatus']]
    #     weekend_df.dropna(subset=[time_column[0]],inplace=True)
    #     weekend_raw_df=weekend_df[['id', 'DATE_SUBMITTED', route_survey_column[0],'ROUTE_SURVEYED',stopon_clntid_column[0],stopoff_clntid_column[0],time_column[0],time_period_column[0],'Day','ElvisStatus']]
    #     weekend_raw_df.rename(columns={stopon_clntid_column[0]:'BOARDING LOCATION',stopoff_clntid_column[0]:'ALIGHTING LOCATION'},inplace=True)
    #     weekday_raw_df.rename(columns={stopon_clntid_column[0]:'BOARDING LOCATION',stopoff_clntid_column[0]:'ALIGHTING LOCATION'},inplace=True)
    # else:
    #     weekday_df.dropna(subset=[time_column[0]],inplace=True)
    #     weekday_raw_df=weekday_df[['id', 'Completed', route_survey_column[0],'ROUTE_SURVEYED',stopon_clntid_column[0],stopoff_clntid_column[0],time_column[0],time_period_column[0],'Day','ELVIS_STATUS']]
    #     weekend_df.dropna(subset=[time_column[0]],inplace=True)
    #     weekend_raw_df=weekend_df[['id', 'Completed', route_survey_column[0],'ROUTE_SURVEYED',stopon_clntid_column[0],stopoff_clntid_column[0],time_column[0],time_period_column[0],'Day','ELVIS_STATUS']]
    #     weekend_raw_df.rename(columns={stopon_clntid_column[0]:'BOARDING LOCATION',stopoff_clntid_column[0]:'ALIGHTING LOCATION'},inplace=True)
    #     weekday_raw_df.rename(columns={stopon_clntid_column[0]:'BOARDING LOCATION',stopoff_clntid_column[0]:'ALIGHTING LOCATION'},inplace=True)
    # Include LocalTime column in raw data exports
    if project=='KCATA' or project=='KCATA RAIL' or project=='ACTRANSIT' or project=='SALEM' or project=='LACMTA_FEEDER':
        weekday_df.dropna(subset=[time_column[0]],inplace=True)
        weekday_raw_df=weekday_df[['id', 'LocalTime', 'DATE_SUBMITTED', route_survey_column[0],'ROUTE_SURVEYED',stopon_clntid_column[0],stopoff_clntid_column[0],time_column[0],time_period_column[0],'Day','ElvisStatus']]
        weekend_df.dropna(subset=[time_column[0]],inplace=True)
        weekend_raw_df=weekend_df[['id', 'LocalTime', 'DATE_SUBMITTED', route_survey_column[0],'ROUTE_SURVEYED',stopon_clntid_column[0],stopoff_clntid_column[0],time_column[0],time_period_column[0],'Day','ElvisStatus']]
        weekend_raw_df.rename(columns={stopon_clntid_column[0]:'BOARDING LOCATION',stopoff_clntid_column[0]:'ALIGHTING LOCATION'},inplace=True)
        weekday_raw_df.rename(columns={stopon_clntid_column[0]:'BOARDING LOCATION',stopoff_clntid_column[0]:'ALIGHTING LOCATION'},inplace=True)
    else:
        weekday_df.dropna(subset=[time_column[0]],inplace=True)
        weekday_raw_df=weekday_df[['id', 'LocalTime', 'Completed', route_survey_column[0],'ROUTE_SURVEYED',stopon_clntid_column[0],stopoff_clntid_column[0],time_column[0],time_period_column[0],'Day','ELVIS_STATUS']]
        weekend_df.dropna(subset=[time_column[0]],inplace=True)
        weekend_raw_df=weekend_df[['id', 'LocalTime', 'Completed', route_survey_column[0],'ROUTE_SURVEYED',stopon_clntid_column[0],stopoff_clntid_column[0],time_column[0],time_period_column[0],'Day','ELVIS_STATUS']]
        weekend_raw_df.rename(columns={stopon_clntid_column[0]:'BOARDING LOCATION',stopoff_clntid_column[0]:'ALIGHTING LOCATION'},inplace=True)
        weekday_raw_df.rename(columns={stopon_clntid_column[0]:'BOARDING LOCATION',stopoff_clntid_column[0]:'ALIGHTING LOCATION'},inplace=True)

    wkday_route_level =create_route_level_df(wkday_overall_df,wkday_route_df,weekday_df,time_column,project)
    wkend_route_level =create_route_level_df(wkend_overall_df,wkend_route_df,weekend_df,time_column,project)
    wkday_comparison_df=copy.deepcopy(wkday_route_level)
    wkday_new_route_level_df=copy.deepcopy(wkday_route_level)

    wkend_comparison_df=copy.deepcopy(wkend_route_level)
    wkend_new_route_level_df=copy.deepcopy(wkend_route_level)
    # this is for time value data
    if project in ["KCATA RAIL"]:
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

    if project == 'KCATA' or project == 'ACTRANSIT' or project == 'SALEM' or project == 'LACMTA_FEEDER':
        rename_dict = {
            'CR_Early_AM': '(1) Goal',
            'CR_AM_Peak': '(2) Goal',
            'CR_Midday': '(3) Goal',
            'CR_PM_Peak': '(4) Goal',
            'CR_Evening': '(5) Goal',
            'DB_Early_AM_Peak': '(1) Collect',
            'DB_AM_Peak': '(2) Collect',
            'DB_Midday': '(3) Collect',
            'DB_PM_Peak': '(4) Collect',
            'DB_Evening': '(5) Collect',
            'Early_AM_DIFFERENCE': '(1) Remain',
            'AM_DIFFERENCE': '(2) Remain',
            'Midday_DIFFERENCE': '(3) Remain',
            'PM_PEAK_DIFFERENCE': '(4) Remain',
            'Evening_DIFFERENCE': '(5) Remain'
        }
        # Check if weekend data exists using try/except for robustness
        has_weekend_data = False
        try:
            if 'wkend_comparison_df' in locals() and wkend_comparison_df is not None:
                wkend_comparison_df.rename(columns={**rename_dict, 
                    'CR_Overall_Goal': 'Route Level Goal',
                    'DB_Total': '# of Surveys',
                    'Overall_Goal_DIFFERENCE': 'Remaining'}, inplace=True)
                has_weekend_data = True
        except NameError:
            has_weekend_data = False
            print(f"No weekend comparison data available for {project}")
        try:
            if 'wkend_route_direction_df' in locals() and wkend_route_direction_df is not None:
                wkend_route_direction_df.rename(columns=rename_dict, inplace=True)
                has_weekend_data = has_weekend_data or True
        except NameError:
            print(f"No weekend route direction data available for {project}")

        # Process weekday data (always exists)
        wkday_comparison_df.rename(columns={**rename_dict,
            'CR_Overall_Goal': 'Route Level Goal',
            'DB_Total': '# of Surveys',
            'Overall_Goal_DIFFERENCE': 'Remaining'}, inplace=True)
        wkday_route_direction_df.rename(columns=rename_dict, inplace=True)

        # Create a unified route lookup from detail_df_stops
        route_lookup = detail_df_stops[['ETC_ROUTE_ID', 'ETC_ROUTE_NAME']].drop_duplicates()
        
        # Function to safely merge route names
        def add_route_names(df):
            df = df.merge(
                route_lookup,
                left_on='ROUTE_SURVEYEDCode',
                right_on='ETC_ROUTE_ID',
                how='left'
            )
            df['ROUTE_SURVEYED'] = df['ETC_ROUTE_NAME'].fillna('Unknown Route (' + df['ROUTE_SURVEYEDCode'] + ')')
            return df.drop(columns=['ETC_ROUTE_ID', 'ETC_ROUTE_NAME'], errors='ignore')

        # Apply to all DataFrames that need route names
        wkday_comparison_df = add_route_names(wkday_comparison_df)
        wkday_route_direction_df = add_route_names(wkday_route_direction_df)
        
        # Only process weekend data if it exists
        if has_weekend_data:
            try:
                wkend_comparison_df = add_route_names(wkend_comparison_df)
                wkend_route_direction_df = add_route_names(wkend_route_direction_df)
            except NameError as e:
                print(f"Error processing weekend route names: {e}")
                has_weekend_data = False
        print("Processing survey data...")  
        # Process survey data
        df_for_processing = baby_elvis_df_merged.rename(columns={
            'LocalTime': 'Completed',
            'HAVE_5_MIN_FOR_SURVECode': 'HAVE_5_MIN_FOR_SURVE_Code_',
            'ROUTE_SURVEYEDCode': 'ROUTE_SURVEYED_Code_'
        })

        interviewer_pivot, route_pivot, detail_table = process_survey_data(df_for_processing)

        print("Processing route comparison data...")
        # Process the route comparison data
        new_df = process_route_comparison_data(wkday_overall_df, baby_elvis_df_merged, ke_df, project)
        route_level_df = create_route_level_comparison(new_df)
        comparison_df, all_type_df, reverse_df = process_reverse_direction_logic(wkday_overall_df ,baby_elvis_df_merged, route_level_df, project, stops_df)
        print("Route comparison data processed successfully.")

        survey_report_df = process_surveyor_data_transit_ls6(ke_df, df)
        route_report_df = process_route_data_transit_ls6(ke_df, df)
        low_response_questions_df = create_low_response_report(df)
        refusal_analysis_df, refusal_race_df = create_survey_stats_master_table(baby_elvis_df)

        demographic_review_df = generate_demographic_summary(df, project_name)

        # Convert both columns to datetime, handling errors
        ke_df['Elvis_Date'] = pd.to_datetime(ke_df['Elvis_Date'], errors='coerce')
        df['LocalTime'] = pd.to_datetime(df['LocalTime'], errors='coerce')
        # Get valid dates (non-NaN)
        ke_valid_dates = ke_df['Elvis_Date'].dropna()
        df_valid_dates = df['LocalTime'].dropna()

        # Clean route names
        def clean_route_name(route_series):
            return (
                route_series
                .astype(str)
                .str.replace(r' \[(INBOUND|OUTBOUND)\]', '', regex=True)
                .str.strip()
            )
        
        ke_df['ROUTE_ROOT'] = clean_route_name(ke_df['ROUTE_SURVEYED'])
        df['ROUTE_ROOT'] = clean_route_name(df['ROUTE_SURVEYED'])

        # Create date-surveyor and date-route mappings
        if len(ke_valid_dates) > 0 and len(df_valid_dates) > 0:
            min_date = min(ke_valid_dates.min(), df_valid_dates.min())
            max_date = max(ke_valid_dates.max(), df_valid_dates.max())
        elif len(ke_valid_dates) > 0:
            min_date = ke_valid_dates.min()
            max_date = ke_valid_dates.max()
        elif len(df_valid_dates) > 0:
            min_date = df_valid_dates.min()
            max_date = df_valid_dates.max()
        else:
            min_date = pd.Timestamp.today()
            max_date = pd.Timestamp.today()

        all_dates = pd.date_range(min_date, max_date).date

        all_surveyors = sorted(set(ke_df['INTERV_INIT'].astype(str).unique()) | set(df['INTERV_INIT'].astype(str).unique()))
        all_routes = sorted(set(ke_df['ROUTE_ROOT'].unique()) | set(df['ROUTE_ROOT'].unique()))

        # Create mappings with concatenated keys
        survey_date_surveyor = pd.MultiIndex.from_product(
            [all_dates, all_surveyors],
            names=['Date', 'INTERV_INIT']
        ).to_frame(index=False)
        survey_date_surveyor['Date_Surveyor'] = (
            survey_date_surveyor['Date'].astype(str) + "_" + 
            survey_date_surveyor['INTERV_INIT']
        )

        survey_date_route = pd.MultiIndex.from_product(
            [all_dates, all_routes],
            names=['Date', 'ROUTE_ROOT']
        ).to_frame(index=False)
        survey_date_route['Date_Route'] = (
            survey_date_route['Date'].astype(str) + "_" + 
            survey_date_route['ROUTE_ROOT']
        )

        # Convert survey_date dataframes' Date column to datetime to match ke_df and df
        survey_date_surveyor['Date'] = pd.to_datetime(survey_date_surveyor['Date'])
        survey_date_route['Date'] = pd.to_datetime(survey_date_route['Date'])

        # Merge into main DataFrames
        ke_df = ke_df.merge(
            survey_date_surveyor,
            left_on=['Elvis_Date', 'INTERV_INIT'],
            right_on=['Date', 'INTERV_INIT'],
            how='left'
        )

        ke_df = ke_df.merge(
            survey_date_route,
            left_on=['Elvis_Date', 'ROUTE_ROOT'],
            right_on=['Date', 'ROUTE_ROOT'],
            how='left',
            suffixes=('', '_r')
        )

        df = df.merge(
            survey_date_surveyor,
            left_on=['LocalTime', 'INTERV_INIT'],
            right_on=['Date', 'INTERV_INIT'],
            how='left'
        )

        df = df.merge(
            survey_date_route,
            left_on=['LocalTime', 'ROUTE_ROOT'],
            right_on=['Date', 'ROUTE_ROOT'],
            how='left',
            suffixes=('', '_r')
        )

        # Now process with the merged data
        survey_report_by_date_df = process_surveyor_date_data_transit_ls6(ke_df, df, survey_date_surveyor)
        route_report_by_date_df = process_route_date_data_transit_ls6(ke_df, df, survey_date_route)

        # Final DataFrame cleanup
        wkday_comparison_df.rename(columns={'ETC_ROUTE_NAME': 'ROUTE_SURVEYED'}, inplace=True)

        # Only process weekend comparison if data exists
        if has_weekend_data:
            try:
                wkend_comparison_df = wkend_comparison_df.merge(
                    detail_df_xfers[['ETC_ROUTE_ID', 'ETC_ROUTE_NAME']],
                    left_on='ROUTE_SURVEYEDCode',
                    right_on='ETC_ROUTE_ID',
                    how='left'
                )
                wkend_comparison_df.rename(columns={'ETC_ROUTE_NAME': 'ROUTE_SURVEYED'}, inplace=True)
                wkend_comparison_df.drop(columns=['ETC_ROUTE_ID'], inplace=True)
            except NameError as e:
                print(f"Error processing weekend comparison merge: {e}")

        # Just for ROUTE COMPARISON PART #
        # Get column names for consistency
        route_survey_column = check_all_characters_present(elvis_df, ['routesurveyedcode'])
        route_survey_name_column = check_all_characters_present(elvis_df, ['routesurveyed'])

        # Create dataframes that match the exact Excel structure
        # 1. Route Comparison sheet
        route_comparison_export = comparison_df.copy()

        # Ensure all columns are present and in correct order as in Excel
        expected_route_comp_columns = [
            'ROUTE_SURVEYEDCode', 'CR_Early_AM', 'CR_AM_Peak', 'CR_Midday', 
            'CR_PM_Peak', 'CR_Evening', 'CR_Total', 'DB_Early_AM', 'DB_AM_Peak', 
            'DB_Midday', 'DB_PM_Peak', 'DB_Evening', 'DB_Total', 'DB_Early_AM_IDS', 
            'DB_AM_IDS', 'DB_Midday_IDS', 'DB_PM_IDS', 'DB_Evening_IDS', 
            'EARLY_AM_DIFFERENCE', 'AM_DIFFERENCE', 'Midday_DIFFERENCE', 
            'PM_DIFFERENCE', 'Evening_DIFFERENCE', 'Total_DIFFERENCE'
        ]

        # Add missing columns with default values
        for col in expected_route_comp_columns:
            if col not in route_comparison_export.columns:
                route_comparison_export[col] = 0 if 'DIFFERENCE' in col or col.startswith(('CR_', 'DB_')) else ''

        # Reorder columns to match Excel
        route_comparison_export = route_comparison_export[expected_route_comp_columns]

        # 2. Reverse Routes sheet
        reverse_routes_export = all_type_df[[
            'id', route_survey_column[0], route_survey_name_column[0], 'TIME_PERIOD', 'DAY_TYPE', 'FINAL_DIRECTION_CODE', 'Type', 'REVERSE_TRIPS_STATUS', 'COMPLETED By', 'URL'
        ]].copy()

        # Rename columns to match Excel format if needed
        reverse_routes_export = reverse_routes_export.rename(columns={
            route_survey_column[0]: 'ROUTE_SURVEYEDCode',
            route_survey_name_column[0]: 'ROUTE_SURVEYED'
        })

        # Ensure all expected columns are present
        expected_reverse_columns = ['id', 'ROUTE_SURVEYEDCode', 'ROUTE_SURVEYED', 'TIME_PERIOD', 'DAY_TYPE', 'FINAL_DIRECTION_CODE', 'Type', 'REVERSE_TRIPS_STATUS', 'COMPLETED By', 'URL']
        for col in expected_reverse_columns:
            if col not in reverse_routes_export.columns:
                reverse_routes_export[col] = ''

        reverse_routes_export = reverse_routes_export[expected_reverse_columns]

        # 3. Reverse Routes Difference sheet
        reverse_diff_export = reverse_df[reverse_df['Type'] != ''][[
            'id', route_survey_column[0], route_survey_name_column[0], 'TIME_PERIOD', 'DAY_TYPE', 'FINAL_DIRECTION_CODE', 'Type', 'REVERSE_TRIPS_STATUS', 'COMPLETED By', 'URL'
        ]].copy()

        # Rename columns to match Excel format
        reverse_diff_export = reverse_diff_export.rename(columns={
            route_survey_column[0]: 'ROUTE_SURVEYEDCode',
            route_survey_name_column[0]: 'ROUTE_SURVEYED'
        })

        # Ensure all expected columns are present
        for col in expected_reverse_columns:
            if col not in reverse_diff_export.columns:
                reverse_diff_export[col] = ''

        reverse_diff_export = reverse_diff_export[expected_reverse_columns]

        # Data type conversion to match Excel/Snowflake schema
        # Convert numeric columns to appropriate types
        numeric_columns = ['CR_Early_AM', 'CR_AM_Peak', 'CR_Midday', 'CR_PM_Peak', 'CR_Evening', 
                        'CR_Total', 'DB_Early_AM', 'DB_AM_Peak', 'DB_Midday', 'DB_PM_Peak', 
                        'DB_Evening', 'DB_Total', 'EARLY_AM_DIFFERENCE', 'AM_DIFFERENCE', 
                        'Midday_DIFFERENCE', 'PM_DIFFERENCE', 'Evening_DIFFERENCE', 'Total_DIFFERENCE']

        for col in numeric_columns:
            if col in route_comparison_export.columns:
                route_comparison_export[col] = pd.to_numeric(route_comparison_export[col], errors='coerce').fillna(0).astype(int)

        # Convert ID columns to string
        for df_export in [reverse_routes_export, reverse_diff_export]:
            if 'id' in df_export.columns:
                df_export['id'] = df_export['id'].astype(str)

        # Handle NaN values
        route_comparison_export = route_comparison_export.fillna(0)
        reverse_routes_export = reverse_routes_export.fillna('')
        reverse_diff_export = reverse_diff_export.fillna('')

    elif project == 'KCATA RAIL':
        # For comparison dataframes (with route level goals)
        for df in [wkend_comparison_df, wkday_comparison_df]:
            df.rename(columns={
                'CR_Early_AM': '(1) Goal',
                'CR_AM_Peak': '(2) Goal',
                'CR_Midday': '(3) Goal',
                'CR_PM_Peak': '(4) Goal', 
                'CR_Evening': '(5) Goal',
                'DB_Early_AM_Peak': '(1) Collect',
                'DB_AM_Peak': '(2) Collect',
                'DB_Midday': '(3) Collect',
                'DB_PM_Peak': '(4) Collect',
                'DB_Evening': '(5) Collect',
                'Early_AM_DIFFERENCE': '(1) Remain',
                'AM_DIFFERENCE': '(2) Remain',
                'Midday_DIFFERENCE': '(3) Remain',
                'PM_PEAK_DIFFERENCE': '(4) Remain',
                'Evening_DIFFERENCE': '(5) Remain',
                'CR_Overall_Goal': 'Route Level Goal',
                'DB_Total': '# of Surveys',
                'Total_DIFFERENCE': 'Remaining'
            }, inplace=True)

        # For route direction dataframes (without route level goals)
        for df in [wkday_route_direction_df, wkend_route_direction_df]:
            df.rename(columns={
                'CR_Early_AM': '(1) Goal',
                'CR_AM_Peak': '(2) Goal',
                'CR_Midday': '(3) Goal',
                'CR_PM_Peak': '(4) Goal',
                'CR_Evening': '(5) Goal',
                'DB_Early_AM_Peak': '(1) Collect',
                'DB_AM_Peak': '(2) Collect',
                'DB_Midday': '(3) Collect',
                'DB_PM_Peak': '(4) Collect',
                'DB_Evening': '(5) Collect',
                'Early_AM_DIFFERENCE': '(1) Remain',
                'AM_DIFFERENCE': '(2) Remain',
                'Midday_DIFFERENCE': '(3) Remain',
                'PM_PEAK_DIFFERENCE': '(4) Remain',
                'Evening_DIFFERENCE': '(5) Remain'
            }, inplace=True)

        # For station-wise route dataframes
        for df in [wkday_stationwise_route_df, wkend_stationwise_route_df]:
            df.rename(columns={
                'CR_Early_AM': '(1) Goal',
                'CR_AM_Peak': '(2) Goal',
                'CR_Midday': '(3) Goal',
                'CR_PM_Peak': '(4) Goal',
                'CR_Evening': '(5) Goal',
                'DB_Early_AM_Peak': '(1) Collect',
                'DB_AM_Peak': '(2) Collect',
                'DB_Midday': '(3) Collect',
                'DB_PM_Peak': '(4) Collect',
                'DB_Evening': '(5) Collect',
                'Early_AM_DIFFERENCE': '(1) Remain',
                'AM_DIFFERENCE': '(2) Remain',
                'Midday_DIFFERENCE': '(3) Remain',
                'PM_PEAK_DIFFERENCE': '(4) Remain',
                'Evening_DIFFERENCE': '(5) Remain'
            }, inplace=True)

        # Merge route names for comparison dataframes
        wkday_comparison_df = wkday_comparison_df.merge(
            detail_df_xfers[['ETC_ROUTE_ID', 'ETC_ROUTE_NAME']],
            left_on='ROUTE_SURVEYEDCode',
            right_on='ETC_ROUTE_ID',
            how='left'
        )
        wkday_comparison_df.rename(columns={'ETC_ROUTE_NAME': 'ROUTE_SURVEYED'}, inplace=True)
        wkday_comparison_df.drop(columns=['ETC_ROUTE_ID'], inplace=True)

        wkend_comparison_df = wkend_comparison_df.merge(
            detail_df_xfers[['ETC_ROUTE_ID', 'ETC_ROUTE_NAME']],
            left_on='ROUTE_SURVEYEDCode',
            right_on='ETC_ROUTE_ID',
            how='left'
        )
        wkend_comparison_df.rename(columns={'ETC_ROUTE_NAME': 'ROUTE_SURVEYED'}, inplace=True)
        wkend_comparison_df.drop(columns=['ETC_ROUTE_ID'], inplace=True)

        # Function to add route and station info
        def add_route_and_station_info(df, overall_df, detail_df_stops, detail_df_xfers=None):
            """Helper function to add route and station information with error handling"""
            for _, row in df.iterrows():
                # Get route information
                try:
                    route_surveyed = detail_df_stops[detail_df_stops['ETC_ROUTE_ID'] == row['ROUTE_SURVEYEDCode']]['ETC_ROUTE_NAME'].iloc[0]
                except IndexError:
                    if detail_df_xfers is not None:
                        try:
                            route_surveyed = detail_df_xfers[detail_df_xfers['ETC_ROUTE_ID'] == row['ROUTE_SURVEYEDCode']]['ETC_ROUTE_NAME'].iloc[0]
                        except IndexError:
                            route_surveyed = "Unknown Route"
                    else:
                        route_surveyed = "Unknown Route"
                
                # Get station information
                try:
                    station_name = overall_df[overall_df['STATION_ID'] == row['STATION_ID']]['STATION_NAME'].iloc[0]
                except IndexError:
                    station_name = "Unknown Station"
                
                df.loc[row.name, 'ROUTE_SURVEYED'] = route_surveyed
                df.loc[row.name, 'STATION_NAME'] = station_name

        # Apply to all dataframes
        add_route_and_station_info(wkday_route_direction_df, wkday_overall_df, detail_df_stops, detail_df_xfers)
        add_route_and_station_info(wkend_route_direction_df, wkend_overall_df, detail_df_stops, detail_df_xfers)
        add_route_and_station_info(wkday_stationwise_route_df, wkday_overall_df, detail_df_stops, detail_df_xfers)
        add_route_and_station_info(wkend_stationwise_route_df, wkend_overall_df, detail_df_stops, detail_df_xfers)


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
            network_timeout=120
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
            print(f"Processing {sheet_name} into {table_name}")
            df = dataframes.get(sheet_name)
            if df is not None:
                # Drop duplicate columns before creating table
                df = df.loc[:, ~df.columns.duplicated()]
                
                # Convert column names to strings to avoid attribute errors
                df.columns = df.columns.astype(str)
                
                # SPECIAL HANDLING FOR PIVOT TABLES - Ensure first column is properly typed
                if sheet_name in ['By_Interviewer', 'By_Route']:
                    # Get the first column name
                    first_col = df.columns[0]
                    # Ensure the first column is treated as string
                    df[first_col] = df[first_col].astype(str)
                    print(f"Converted first column '{first_col}' to string for sheet {sheet_name}")
                
                # Convert date columns to proper datetime format
                for col in df.columns:
                    col_str = str(col)  # Ensure column name is string
                    if 'date' in col_str.lower() or 'completed' in col_str.lower():
                        try:
                            df[col] = pd.to_datetime(df[col], errors='coerce')
                            # Convert to date string format that Snowflake understands
                            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                        except Exception as e:
                            print(f"Could not convert column {col} to datetime: {str(e)}")
                            # If conversion fails, keep as string
                            df[col] = df[col].astype(str)

                # Ensure all columns are properly typed
                for col in df.columns:
                    # Skip the first column for pivot tables as we already handled it
                    if sheet_name in ['By_Interviewer', 'By_Route'] and col == df.columns[0]:
                        continue
                        
                    if df[col].dtype == 'object':
                        try:
                            # Try converting to numeric first for numeric-looking strings
                            df[col] = pd.to_numeric(df[col], errors='ignore')
                            # If conversion worked but we want to keep as string for certain columns
                            if col.lower() in ['interviewer', 'route', 'interv_init']:
                                df[col] = df[col].astype(str)
                        except:
                            # If that fails, keep as string
                            df[col] = df[col].astype(str)

                # Drop the table if it exists
                drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
                cur.execute(drop_table_sql)
                print(f"Table {table_name} dropped successfully (if it existed).")

                # Dynamically generate the CREATE TABLE statement
                create_table_sql = f"CREATE TABLE {table_name} (\n"
                for column, dtype in df.dtypes.items():
                    sanitized_column = f'"{column}"'  # Handle special characters
                    snowflake_dtype = dtype_mapping.get(str(dtype), 'VARCHAR')
                    
                    # Special handling for first columns of pivot tables
                    if sheet_name in ['By_Interviewer', 'By_Route'] and column == df.columns[0]:
                        snowflake_dtype = 'VARCHAR'  # Force VARCHAR for the first column
                        
                    create_table_sql += f"  {sanitized_column} {snowflake_dtype},\n"
                create_table_sql = create_table_sql.rstrip(",\n") + "\n);"

                # Execute the CREATE TABLE statement
                cur.execute(create_table_sql)
                print(f"Table {table_name} created successfully.")

                # Insert data into the Snowflake table
                try:
                    write_pandas(conn, df, table_name=table_name.upper())
                    print(f"Data inserted into table {table_name} successfully.")
                except Exception as e:
                    print(f"Error writing to table {table_name}: {str(e)}")
                    # Try again with all columns as strings if first attempt fails
                    df = df.astype(str)
                    write_pandas(conn, df, table_name=table_name.upper())
                    print(f"Data inserted as strings into table {table_name}")

        # Close the Snowflake connection
        cur.close()
        conn.close()



    if project=='KCATA' or project=='ACTRANSIT' or project=='SALEM' or project == 'LACMTA_FEEDER':
        # Check if weekend data exists
        has_weekend_data = False
        weekend_dataframes = {}
        # Define columns that might need to be dropped
        columns_to_possibly_drop = ['CR_Total', 'Total_DIFFERENCE']
        try:
            # Safe column dropping - only drop columns that actually exist
            wkend_route_dir_cols_to_drop = [col for col in columns_to_possibly_drop if col in wkend_route_direction_df.columns]
            wkend_comparison_cols_to_drop = [col for col in columns_to_possibly_drop if col in wkend_comparison_df.columns]
            if 'wkend_route_direction_df' in locals() and wkend_route_direction_df is not None:
                weekend_dataframes.update({
                    'WkEND Route DIR Comparison': wkend_route_direction_df.drop(columns=wkend_route_dir_cols_to_drop),
                    'WkEND RAW DATA': weekend_raw_df,
                    'WkEND Time Data': wkend_time_value_df,
                    'WkEND Route Comparison': wkend_comparison_df.drop(columns=wkend_comparison_cols_to_drop)
                })
                has_weekend_data = True
        except NameError:
            has_weekend_data = False

        # Safe column dropping for weekday data - only drop columns that actually exist
        wkday_route_dir_cols_to_drop = [col for col in columns_to_possibly_drop if col in wkday_route_direction_df.columns]
        wkday_comparison_cols_to_drop = [col for col in columns_to_possibly_drop if col in wkday_comparison_df.columns]
        # DataFrames preparation - start with common dataframes
        dataframes = {
            'WkDAY Route DIR Comparison': wkday_route_direction_df.drop(columns=wkday_route_dir_cols_to_drop),
            'WkDAY RAW DATA': weekday_raw_df,
            'WkDAY Time Data': wkday_time_value_df,
            'WkDAY Route Comparison': wkday_comparison_df.drop(columns=wkday_comparison_cols_to_drop),
            'LAST SURVEY DATE': latest_date_df,
            'By_Interviewer': interviewer_pivot,
            'By_Route': route_pivot,
            'Survey_Detail': detail_table,
            'Surveyor Report': survey_report_df,
            'Route Report': route_report_df,
            'Surveyor Report with Date': survey_report_by_date_df,
            'Route Report with Date': route_report_by_date_df,
            'Route Comparison': route_comparison_export,
            'Reverse Routes': reverse_routes_export,
            'Reverse Routes Difference': reverse_diff_export,
            'LOW RESPONSE QUESTIONS': low_response_questions_df,
            'Refusal Analysis Report': refusal_analysis_df,
            'Refusal Race Report': refusal_race_df,
            'Demographic Review': demographic_review_df,
        }

        # Add weekend dataframes only if they exist
        if has_weekend_data:
            dataframes.update(weekend_dataframes)

        # Table mapping - include weekend tables conditionally
        table_info = {
            'WkDAY RAW DATA': 'wkday_raw', 
            'WkDAY Route Comparison': 'wkday_comparison', 
            'WkDAY Route DIR Comparison': 'wkday_dir_comparison', 
            'WkDAY Time Data': 'wkday_time_data',
            'LAST SURVEY DATE': 'last_survey_date',
            'By_Interviewer': 'by_interv_totals',
            'By_Route': 'by_route_totals',
            'Survey_Detail': 'survey_detail_totals',
            'Surveyor Report': 'surveyor_report_trends',
            'Route Report': 'route_report_trends',
            'Surveyor Report with Date': 'surveyor_report_date_trends',
            'Route Report with Date': 'route_report_date_trends',
            'Route Comparison': 'route_comparison',
            'Reverse Routes': 'reverse_routes',
            'Reverse Routes Difference': 'reverse_routes_difference',
            'LOW RESPONSE QUESTIONS': 'low_response_questions',
            'Refusal Analysis Report': 'refusal_analysis_report',
            'Refusal Race Report': 'refusal_race_report',
            'Demographic Review': 'demographic_review',
        }

        # Add weekend table mappings only if weekend data exists
        if has_weekend_data:
            table_info.update({
                'WkEND RAW DATA': 'wkend_raw', 
                'WkEND Route Comparison': 'wkend_comparison', 
                'WkEND Route DIR Comparison': 'wkend_dir_comparison', 
                'WkEND Time Data': 'wkend_time_data'
            })

        # # CREATE EXCEL FILE WITH ALL SHEETS
        # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # excel_filename = f"{project}_survey_report_{timestamp}.xlsx"
        
        # try:
        #     with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
        #         # Write each dataframe to a separate sheet
        #         for sheet_name, dataframe in dataframes.items():
        #             if dataframe is not None and not dataframe.empty:
        #                 # Special handling for different sheet types
        #                 if sheet_name == 'Survey_Detail':
        #                     dataframe.to_excel(writer, sheet_name=sheet_name, index=False)
        #                 else:
        #                     dataframe.to_excel(writer, sheet_name=sheet_name, index=True)
                        
        #                 # Auto-adjust column widths for better readability
        #                 worksheet = writer.sheets[sheet_name]
        #                 for column in worksheet.columns:
        #                     max_length = 0
        #                     column_letter = column[0].column_letter
        #                     for cell in column:
        #                         try:
        #                             if len(str(cell.value)) > max_length:
        #                                 max_length = len(str(cell.value))
        #                         except:
        #                             pass
        #                     adjusted_width = min(max_length + 2, 50)
        #                     worksheet.column_dimensions[column_letter].width = adjusted_width
            
        #     print(f"✅ Excel file '{excel_filename}' created successfully!")
        #     print(f"📊 Sheets included: {len(dataframes)}")
            
        # except Exception as e:
        #     print(f"❌ Error creating Excel file: {e}")


    elif project == 'KCATA RAIL':
        def safe_drop_columns(df, columns_to_drop):
            """Safely drop columns that exist in the DataFrame"""
            existing_cols = [col for col in columns_to_drop if col in df.columns]
            return df.drop(columns=existing_cols, errors='ignore')
        # Create dictionaries for dataframes and table info similar to TUCSON RAIL
        dataframes = {
            'WkDAY Route DIR Comparison': safe_drop_columns(wkday_route_direction_df, ['STATION_ID_SPLITTED', 'CR_Total', 'Total_DIFFERENCE']),
            'WkEND Route DIR Comparison': safe_drop_columns(wkend_route_direction_df, ['STATION_ID_SPLITTED', 'CR_Total', 'Total_DIFFERENCE']),
            'WkEND Time Data': wkend_time_value_df,
            'WkDAY Time Data': wkday_time_value_df,
            'WkEND Stationwise Comparison': safe_drop_columns(wkend_stationwise_route_df, ['CR_Total', 'Total_DIFFERENCE', 'DB_Total']),
            'WkDAY Stationwise Comparison': safe_drop_columns(wkday_stationwise_route_df, ['CR_Total', 'Total_DIFFERENCE', 'DB_Total']),
            'WkDAY Route Comparison': safe_drop_columns(wkday_comparison_df, ['CR_Total', 'DB_AM_IDS', 'DB_Midday_IDS', 'DB_PM_IDS', 'DB_Evening_IDS', 'Total_DIFFERENCE']),
            'WkEND Route Comparison': safe_drop_columns(wkend_comparison_df, ['CR_Total', 'DB_AM_IDS', 'DB_Midday_IDS', 'DB_PM_IDS', 'DB_Evening_IDS', 'Total_DIFFERENCE']),
            "WkDAY RAW DATA": weekday_raw_df[['id', 'LocalTime', 'DATE_SUBMITTED', route_survey_column[0], 'ROUTE_SURVEYED', 'BOARDING LOCATION', 'ALIGHTING LOCATION', time_column[0], time_period_column[0], 'Day', 'ElvisStatus']],
            'WkEND RAW DATA': weekend_raw_df[['id', 'LocalTime', 'DATE_SUBMITTED', route_survey_column[0], 'ROUTE_SURVEYED', 'BOARDING LOCATION', 'ALIGHTING LOCATION', time_column[0], time_period_column[0], 'Day', 'ElvisStatus']],
            'LAST SURVEY DATE': latest_date_df
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