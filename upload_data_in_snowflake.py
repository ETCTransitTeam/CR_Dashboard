import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from decouple import config
import os

def create_snowflake_connection():
    conn = snowflake.connector.connect(
        user=config('SNOWFLAKE_USER'),
        password=config('SNOWFLAKE_PASSWORD'),
        account=config('SNOWFLAKE_ACCOUNT'),
        warehouse=config('SNOWFLAKE_WAREHOUSE'),
        database=config('SNOWFLAKE_DATABASE'),
        schema='kcata_bus',
        role=config('SNOWFLAKE_ROLE')
    )
    return conn

def create_tables_and_insert_data(file_path, sheet_info):
    print("Creating tables and inserting data into Snowflake...")
    # File path and dtype mapping
    dtype_mapping = {
        'object': 'VARCHAR',
        'int64': 'INTEGER',
        'float64': 'FLOAT',
        'datetime64[ns]': 'TIMESTAMP',
        'bool': 'BOOLEAN'
    }
    print("Connecting to Snowflake...")
    # Create Snowflake connection
    conn = create_snowflake_connection()
    print("Connection established successfully.")
    cur = conn.cursor()
    
    # Check if the file exists
    if os.path.exists(file_path):
        for sheet_name, table_name in sheet_info.items():
            try:
                # Read the sheet into a DataFrame
                df = pd.read_excel(file_path, sheet_name=sheet_name)

                # Special handling for Survey_Detail sheet
                if sheet_name == 'Survey_Detail':
                    df.columns = [col.strip().lower() for col in df.columns]
                    # Standardize column names
                    rename_map = {
                        'interv_init': 'INTERV_INIT',
                        'route': 'ROUTE',
                        'date_format': 'DATE',
                        'date': 'DATE',
                        'count': 'COUNT'
                    }

                    df = df.rename(columns=rename_map)
                    
                    # Validate required columns
                    required_cols = ['INTERV_INIT', 'ROUTE', 'DATE', 'COUNT']
                    if not all(col in df.columns for col in required_cols):
                        print(f"Missing required columns in Survey_Detail sheet. Found: {df.columns.tolist()}")
                        continue
                    
                    # Convert data types
                    df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce').dt.date
                    df['COUNT'] = pd.to_numeric(df['COUNT'], errors='coerce')
                    
                    # Remove invalid rows
                    df = df.dropna(subset=['DATE', 'COUNT'])

                # Special handling for sheets with date columns
                if sheet_name in ['Surveyor Report with Date', 'Route Report with Date']:
                    if 'Date' in df.columns:
                        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
                        # Keep as datetime or convert to date string
                        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

                # Clean column names (convert all to strings)
                df.columns = [str(col) for col in df.columns]

                # Drop the table if it exists
                drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
                cur.execute(drop_table_sql)
                print(f"Table {table_name} dropped successfully (if it existed).")
                
                # Dynamically generate the CREATE TABLE statement
                create_table_sql = f"CREATE TABLE {table_name} (\n"
                for column, dtype in df.dtypes.items():
                    # Quote column names to handle special characters
                    sanitized_column = f'"{column}"'
                    snowflake_dtype = dtype_mapping.get(str(dtype), 'VARCHAR')  # Default to VARCHAR for unknown types
                    create_table_sql += f"  {sanitized_column} {snowflake_dtype},\n"
                create_table_sql = create_table_sql.rstrip(",\n") + "\n);"
                
                # Print the create table SQL for reference (optional)
                print(create_table_sql)

                # Execute the CREATE TABLE statement
                cur.execute(create_table_sql)
                print(f"Table {table_name} created successfully.")
                
                # Insert data into the Snowflake table
                write_pandas(conn, df, table_name=table_name.upper())
                print(f"Data inserted into table {table_name} successfully.")
            
            except Exception as e:
                print(f"Error processing sheet {sheet_name}: {str(e)}")
                continue
    
    else:
        print(f"The file {file_path} does not exist.")

    # Close the Snowflake connection
    cur.close()
    conn.close()

file_path = 'reviewtool_20250807_KCATA_RouteLevelComparison(Wkday & WkEnd)_Latest_01.xlsx'
# #  For bus transport project
sheet_info = {
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
    'Surveyor Report': 'surveyor_report_trends',
    'Route Report': 'route_report_trends',
    'Surveyor Report with Date': 'surveyor_report_date_trends',
    'Route Report with Date': 'route_report_date_trends'
}

#  For rail project
# sheet_info = {
#     'WkDAY RAW DATA': 'wkday_raw', 
#     'WkEND RAW DATA': 'wkend_raw', 
#     'WkEND Stationwise Comparison': 'wkday_stationwise_comparison', 
#     'WkDAY Stationwise Comparison': 'wkend_stationwise_comparison',
#     'WkDAY Route Comparison': 'wkday_comparison', 
#     'WkDAY Route DIR Comparison': 'wkday_dir_comparison', 
#     'WkEND Route Comparison': 'wkend_comparison', 
#     'WkEND Route DIR Comparison': 'wkend_dir_comparison', 
#     'WkEND Time Data': 'wkend_time_data', 
#     'WkDAY Time Data': 'wkday_time_data',
#     'LAST SURVEY DATE': 'last_survey_date',
# }

# file_path = 'details_saint_louis_MO_od_excel.xlsx'
# # detail_df=pd.read_excel('details_TUCSON_AZ_od_excel.xlsx',sheet_name='TOD')
# # # detail_df=detail_df[['OPPO_TIME[CODE]', 'TIME_ON[Code]', 'TIME_ON', 'TIME_PERIOD[Code]',
# # #                               'TIME_PERIOD', 'START_TIME']]
# sheet_info = {
#     'TOD': 'TOD'
# }

# Call the function
create_tables_and_insert_data(file_path, sheet_info)   
