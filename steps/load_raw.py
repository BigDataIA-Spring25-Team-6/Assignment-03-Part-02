import time
from snowflake.snowpark import Session

NOAA_TABLES = ["RAW_NOAA_OBSERVATION_STAGING","RAW_NOAA_STATION_STAGING"]
TABLE_DICT = {
    "noaa": {
        "schema": "RAW_NOAA",
        "tables": NOAA_TABLES
    }
}

# ------------------------------------------------------------------------------
# load_raw_table function:
#  - Switches to target schema
#  - Reads from an external stage (S3)
#  - Infers schema (if using Parquet) and copies data into a Snowflake table
#  - Adds a COMMENT for auditing/metadata
# ------------------------------------------------------------------------------

def load_raw_table(session, tname=None, schema=None):
    """
    Loads raw NOAA data from @RAW_NOAA.NOAA_RAW_STAGE (or another stage)
    into a table in RAW_NOAA. It loads from a general
    path like s3dir/tname.
    """
    
    session.use_schema(schema)
    tables = session.sql("SHOW TABLES IN RAW_NOAA").collect()
    existing_tables = {row["name"].upper() for row in tables}

    if "RAW_NOAA_OBSERVATION_STAGING" not in existing_tables:
        print("Creating RAW_NOAA_OBSERVATION_STAGING table...")
        session.sql("""
            CREATE OR REPLACE TABLE RAW_NOAA_OBSERVATION_STAGING (
                weather_json VARIANT
            )
        """).collect()

    if "RAW_NOAA_STATION_STAGING" not in existing_tables:
        print("Creating RAW_NOAA_STATION_STAGING table...")
        session.sql("""
            CREATE OR REPLACE TABLE RAW_NOAA_STATION_STAGING (
                station_json VARIANT
            )
        """).collect()
        
    if tname == "RAW_NOAA_OBSERVATION_STAGING":
        file_name = "weather_data.json"
    else:
        file_name = "station_data.json"

    location = f"@RAW_NOAA.NOAA_RAW_STAGE/{file_name}"
    
    session.sql(f"""
        COPY INTO {tname}
        FROM {location}
        FILE_FORMAT = (FORMAT_NAME = 'RAW_NOAA.JSON_FILE_FORMAT')
    """).collect()

    comment_text = '''{"origin":"noaa_lab","name":"snowpark_noaa_de","version":{"major":1,"minor":0},"attributes":{"is_quickstart":1,"source":"sql"}}'''
    sql_command = f"COMMENT ON TABLE {tname} IS '{comment_text}'"
    session.sql(sql_command).collect()

def load_all_raw_tables(session):
    """
    Iterates over the NOAA table definitions in TABLE_DICT, loads each table
    from the external stage into RAW_NOAA. Optionally handles multiple years.
    """
    # Example: scale up the warehouse for faster loads
    _ = session.sql("ALTER WAREHOUSE NOAA_LAB_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()

    # Each key in TABLE_DICT is a folder (like 'noaa'), with a schema and tables
    for _, data in TABLE_DICT.items():
        schema = data['schema']
        tnames = data['tables']

        for tname in tnames:
            print(f"Loading {tname} in schema {schema}")
            load_raw_table(session,tname=tname,schema=schema)

    # Scale warehouse back down
    _ = session.sql("ALTER WAREHOUSE NOAA_LAB_WH SET WAREHOUSE_SIZE = XSMALL").collect()


# ------------------------------------------------------------------------------
# validate_raw_tables function:
#  - Prints out the columns of each loaded table for quick verification
# ------------------------------------------------------------------------------
def validate_raw_tables(session):
    """
    Prints the column names from each table in RAW_NOAA for quick inspection.
    """
    for tname in NOAA_TABLES:
        cols = session.table(f'RAW_NOAA.{tname}').columns
        print(f"{tname} columns: \n\t{cols}\n")


# For local debugging

if __name__ == "__main__":
    # Create a local Snowpark session
    with Session.builder.getOrCreate() as session:
        # load all raw tables
        load_all_raw_tables(session)

        # Validate
        validate_raw_tables(session)