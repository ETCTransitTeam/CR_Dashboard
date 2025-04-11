import snowflake.connector
from decouple import config


def create_schema(database_name, schema_name):
    # Establish a connection to Snowflake

    conn = snowflake.connector.connect(
        user=config('SNOWFLAKE_USER'),
        password=config('SNOWFLAKE_PASSWORD'),
        account=config('SNOWFLAKE_ACCOUNT'),
        warehouse=config('SNOWFLAKE_WAREHOUSE'),
        database=database_name,
        schema='public',
        role=config('SNOWFLAKE_ROLE')
    )

    try:
        # Create a new schema in the given database
        cursor = conn.cursor()
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        print(f"Schema '{schema_name}' created successfully in database '{database_name}'")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error creating schema: {e}")
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

# Provide your database and new schema name
database_name = 'CompletionReport'  # Replace with your actual database name
schema_name = 'stl_bus'  # Replace with your desired new schema name

create_schema(database_name, schema_name)
