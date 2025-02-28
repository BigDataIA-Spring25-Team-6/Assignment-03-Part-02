import time
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F

def table_exists(session, database, schema, table_name):
    """Checks if a table exists in the given schema."""
    exists = session.sql(
        f"SELECT EXISTS (SELECT * FROM {database}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}') AS TABLE_EXISTS"
    ).collect()[0]['TABLE_EXISTS']
    return exists

def ensure_harmonized_tables(session):
    """Ensures the existence of harmonized NOAA tables."""
    database = "NOAA_LAB_DB"
    schema = "HARMONIZED_NOAA"

    # Observation Table
    if not table_exists(session, database, schema, "NOAA_OBSERVATIONS"):
        session.sql(f"""
            CREATE TABLE {database}.{schema}.NOAA_OBSERVATIONS (
                OBSERVATION_DATE TIMESTAMP_NTZ,
                DATATYPE VARCHAR,
                STATION_ID VARCHAR,
                ATTRIBUTES VARCHAR,
                OBS_VALUE FLOAT
            )
        """).collect()

    # Station Table
    if not table_exists(session, database, schema, "NOAA_STATIONS"):
        session.sql(f"""
            CREATE TABLE {database}.{schema}.NOAA_STATIONS (
                STATION_ID VARCHAR PRIMARY KEY,
                NAME STRING,
                LATITUDE FLOAT,
                LONGITUDE FLOAT,
                ELEVATION FLOAT,
                MIN_DATE DATE,
                MAX_DATE DATE,
                DATA_COVERAGE FLOAT
            )
        """).collect()

    # Pivot Table (Daily Temperatures)
    if not table_exists(session, database, schema, "NOAA_DAILY_TEMPS"):
        session.sql(f"""
            CREATE TABLE {database}.{schema}.NOAA_DAILY_TEMPS (
                OBSERVATION_DATE DATE,
                STATION_ID STRING,
                TMAX DOUBLE,
                TMIN DOUBLE
            )
        """).collect()

def transform_station_data(session):
    """Transforms station metadata from raw JSON format."""
    database_raw = "NOAA_LAB_DB"
    schema_raw = "RAW_NOAA"
    database_harmonized = "NOAA_LAB_DB"
    schema_harmonized = "HARMONIZED_NOAA"

    print("Transforming station data...")

    df_stations_raw = session.table(f"{database_raw}.{schema_raw}.RAW_NOAA_STATION_STAGING")

    df_stations_parsed = (
        df_stations_raw
        .select(
            F.col("station_json")["id"].alias("STATION_ID"),
            F.col("station_json")["name"].alias("NAME"),
            F.col("station_json")["latitude"].alias("LATITUDE"),
            F.col("station_json")["longitude"].alias("LONGITUDE"),
            F.col("station_json")["elevation"].alias("ELEVATION"),
            F.col("station_json")["mindate"].alias("MIN_DATE"),
            F.col("station_json")["maxdate"].alias("MAX_DATE"),
            F.col("station_json")["datacoverage"].alias("DATA_COVERAGE"),
        )
    )

    df_stations_parsed.write.mode("overwrite").saveAsTable(f"{database_harmonized}.{schema_harmonized}.NOAA_STATIONS")

def transform_observation_data(session):
    """Transforms weather observations from raw JSON format."""
    database_raw = "NOAA_LAB_DB"
    schema_raw = "RAW_NOAA"
    database_harmonized = "NOAA_LAB_DB"
    schema_harmonized = "HARMONIZED_NOAA"

    print("Transforming weather observation data...")

    df_obs_raw = session.table(f"{database_raw}.{schema_raw}.RAW_NOAA_OBSERVATION_STAGING")

    df_obs_parsed = (
        df_obs_raw
        .select(
            F.col("weather_json")["date"].cast("TIMESTAMP_NTZ").alias("OBSERVATION_DATE"),
            F.col("weather_json")["datatype"].cast("STRING").alias("DATATYPE"),
            F.col("weather_json")["station"].cast("STRING").alias("STATION_ID"),
            F.col("weather_json")["attributes"].cast("STRING").alias("ATTRIBUTES"),
            F.col("weather_json")["value"].cast("FLOAT").alias("OBS_VALUE"),
        )
    )

    df_obs_parsed.write.mode("overwrite").saveAsTable(f"{database_harmonized}.{schema_harmonized}.NOAA_OBSERVATIONS")

def pivot_observation_data(session):
    """Creates a pivot table for TMAX and TMIN observations."""
    database = "NOAA_LAB_DB"
    schema = "HARMONIZED_NOAA"

    print("Pivoting TMAX/TMIN observations...")

    df_obs = session.table(f"{database}.{schema}.NOAA_OBSERVATIONS")

    # Filter only TMAX and TMIN
    df_obs_filtered = df_obs.filter(F.col("DATATYPE").isin(["TMAX", "TMIN"]))

    # Ensure correct data type for OBSERVATION_DATE
    df_obs_filtered = df_obs_filtered.with_column("OBSERVATION_DATE", F.col("OBSERVATION_DATE").cast("DATE"))

    # Pivot table transformation
    df_pivoted = (
        df_obs_filtered.group_by("OBSERVATION_DATE", "STATION_ID")
        .pivot("DATATYPE", ["TMAX", "TMIN"])
        .agg(F.max("OBS_VALUE"))
    )

    # Fix column names (removing quotes)
    cleaned_columns = {c: c.replace("'", "").replace('"', "").strip() for c in df_pivoted.columns}
    df_pivoted = df_pivoted.toDF(*[cleaned_columns[c] for c in df_pivoted.columns])

    # Ensure both TMAX and TMIN exist in the pivoted DataFrame
    pivoted_columns = set(df_pivoted.columns)

    if "TMAX" not in pivoted_columns:
        print("Warning: TMAX column missing, adding NULL column")
        df_pivoted = df_pivoted.with_column("TMAX", F.lit(None).cast("DOUBLE"))

    if "TMIN" not in pivoted_columns:
        print("Warning: TMIN column missing, adding NULL column")
        df_pivoted = df_pivoted.with_column("TMIN", F.lit(None).cast("DOUBLE"))

    # Save Pivoted Table
    df_pivoted.write.mode("overwrite").saveAsTable(f"{database}.{schema}.NOAA_DAILY_TEMPS")

def main(session: Session) -> str:
    """Main function for the stored procedure to transform NOAA data."""
    ensure_harmonized_tables(session)
    transform_station_data(session)
    transform_observation_data(session)
    pivot_observation_data(session)
    return "Successfully executed NOAA_TRANSFORM_RAW_SP"

if __name__ == '__main__':
    with Session.builder.getOrCreate() as session:
        print(main(session))