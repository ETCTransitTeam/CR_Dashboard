import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import pd_writer,write_pandas
from decouple import config
import os

def create_snowflake_connection():
    # Replace with your Snowflake credentials
    conn = snowflake.connector.connect(
        user=config('user'),  # Snowflake username
        password=config('password'),  # Snowflake password
        account=config('account'),  # Snowflake account (e.g., abc12345.us-east-1)
        warehouse=config('warehouse'),  # Snowflake warehouse name
        database=config('database'),  # Your Snowflake database name
        schema=config('schema') , # Default schema (you can change it)
        role=config('role')  # Your Snowflake database name
    )
    return conn


# Function to create tables and insert data
def create_tables_and_insert_data(file_path, sheet_info):
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
    
    # Check if the file exists
    if os.path.exists(file_path):
        for sheet_name, table_name in sheet_info.items():
            # Read the sheet into a DataFrame
            df = pd.read_excel(file_path, sheet_name=sheet_name)

            # Dynamically generate the CREATE TABLE statement
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
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

            # Insert data into the Snowflake table
            write_pandas(conn, df, table_name=table_name.upper())

            print(f"Table {table_name} created and data inserted successfully.")
    
    else:
        print(f"The file {file_path} does not exist.")

    # Close the Snowflake connection
    cur.close()
    conn.close()

# file_path = 'reviewtool_20250103_VTA_RouteLevelComparison(Wkday & WkEnd)_Latest_01.xlsx'
# sheet_info = {
#     'WkDAY Route Comparison': 'wkday_comparison', 
#     'WkDAY Route DIR Comparison': 'wkday_dir_comparison', 
#     'WkEND Route Comparison': 'wkend_comparison', 
#     'WkEND Route DIR Comparison': 'wkend_dir_comparison', 
#     'WkEND Time Data': 'wkend_time_data', 
#     'WkDAY Time Data': 'wkday_time_data'
# }

file_path = 'details_vta_CA_od_excel.xlsx'
detail_df=pd.read_excel('details_vta_CA_od_excel.xlsx',sheet_name='TOD')
detail_df=detail_df[['OPPO_TIME[CODE]', 'TIME_ON[Code]', 'TIME_ON', 'TIME_PERIOD[Code]',
                              'TIME_PERIOD', 'START_TIME']]
sheet_info = {
    'TOD': 'TOD'
}

# Call the function
create_tables_and_insert_data(file_path, sheet_info)   