import time
from snowflake.snowpark import Session

def table_exists(session, database, schema, table_name):
    """Checks if a table exists in the given schema."""
    exists = session.sql(
        f"SELECT EXISTS (SELECT * FROM {database}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}') AS TABLE_EXISTS"
    ).collect()[0]['TABLE_EXISTS']
    return exists

def load_raw_table(session, database, schema, table_name, file_name):
    """Loads raw NOAA data from @NOAA_LAB_DB.RAW_NOAA.NOAA_RAW_STAGE into Snowflake staging tables."""
    
    location = f"@{database}.{schema}.NOAA_RAW_STAGE/{file_name}"
    
    session.sql(f"""
        COPY INTO {database}.{schema}.{table_name}
        FROM {location}
        FILE_FORMAT = (FORMAT_NAME = '{database}.{schema}.JSON_FILE_FORMAT')
    """).collect()

    print(f"Loaded data into {database}.{schema}.{table_name} from {location}")

def load_all_raw_tables(session):
    """Loads all raw NOAA tables from S3 into Snowflake staging tables."""
    
    database = "NOAA_LAB_DB"
    schema = "RAW_NOAA"

    # Ensure staging tables exist
    if not table_exists(session, database, schema, "RAW_NOAA_OBSERVATION_STAGING"):
        session.sql(f"""
            CREATE OR REPLACE TABLE {database}.{schema}.RAW_NOAA_OBSERVATION_STAGING (
                weather_json VARIANT
            )
        """).collect()

    if not table_exists(session, database, schema, "RAW_NOAA_STATION_STAGING"):
        session.sql(f"""
            CREATE OR REPLACE TABLE {database}.{schema}.RAW_NOAA_STATION_STAGING (
                station_json VARIANT
            )
        """).collect()

    # Load data from S3
    load_raw_table(session, database, schema, "RAW_NOAA_OBSERVATION_STAGING", "weather_data.json")
    load_raw_table(session, database, schema, "RAW_NOAA_STATION_STAGING", "station_data.json")

def main(session: Session) -> str:
    """Main function for the stored procedure to load raw NOAA data."""
    load_all_raw_tables(session)
    return "Successfully executed NOAA_LOAD_RAW_SP"

if __name__ == '__main__':
    with Session.builder.getOrCreate() as session:
        print(main(session))