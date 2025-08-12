import snowflake.connector
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
load_dotenv()


def create_snowflake_connection():
    with open("path/to/key.p8", "rb") as key:
        private_key = serialization.load_pem_private_key(
            key.read(),
            password=os.environ["SNOWFLAKE_PASSPHRASE"].encode(),
            backend=default_backend(),
        )

    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        private_key=private_key_bytes,
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        authenticator="SNOWFLAKE_JWT",
        schema='kcata_bus',
        role=os.getenv('SNOWFLAKE_ROLE'),
    )
    return conn
    


def drop_all_tables_in_schema(schema_name="kcata_bus"):
    conn = create_snowflake_connection()
    cur = conn.cursor()

    try:
        # Get all tables in the schema
        cur.execute(f"SHOW TABLES IN SCHEMA {schema_name}")
        tables = cur.fetchall()
        print(f"Found {len(tables)} tables in schema '{schema_name}'.")

        for table in tables:
            table_name = table[1]  # Table name is in the second column
            print(f"Dropping table: {table_name}")
            # cur.execute(f'DROP TABLE IF EXISTS {schema_name}."{table_name}"')
        
        print(f"All tables in schema '{schema_name}' have been dropped.")

    except Exception as e:
        print(f"Error while dropping tables: {e}")

    finally:
        cur.close()
        conn.close()


# Call this function
drop_all_tables_in_schema("kcata_bus")