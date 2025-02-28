from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    parse_json, get, col, flatten, max as max_, lit
)

# ----------------------------------------------------------------------
# Ensure Tables & Streams Exist
# ----------------------------------------------------------------------
def ensure_tables_and_streams(session):
    session.use_database("NOAA_LAB_DB")
    session.use_schema("RAW_NOAA")

    tables = session.sql("SHOW TABLES IN SCHEMA RAW_NOAA").collect()
    existing_tables = {row["name"].upper() for row in tables}

    if "RAW_NOAA_STATION_STAGING" not in existing_tables:
        print("Creating RAW_NOAA_STATION_STAGING table...")
        session.sql("""
            CREATE TABLE RAW_NOAA.RAW_NOAA_STATION_STAGING (
                station_json VARIANT
            )
        """).collect()

    if "RAW_NOAA_OBSERVATION_STAGING" not in existing_tables:
        print("Creating RAW_NOAA_OBSERVATION_STAGING table...")
        session.sql("""
            CREATE TABLE RAW_NOAA.RAW_NOAA_OBSERVATION_STAGING (
                weather_json VARIANT
            )
        """).collect()

    streams = session.sql("SHOW STREAMS IN SCHEMA RAW_NOAA").collect()
    existing_streams = {row["name"].upper() for row in streams}

    if "RAW_NOAA_OBSERVATION_STREAM" not in existing_streams:
        print("Creating RAW_NOAA_OBSERVATION_STREAM...")
        session.sql("""
            CREATE OR REPLACE STREAM RAW_NOAA.RAW_NOAA_OBSERVATION_STREAM 
            ON TABLE RAW_NOAA.RAW_NOAA_OBSERVATION_STAGING
            SHOW_INITIAL_ROWS = FALSE;
        """).collect()

# ----------------------------------------------------------------------
# Ensure Harmonized Tables Exist
# ----------------------------------------------------------------------
def ensure_harmonized_tables_exist(session):
    session.use_schema("HARMONIZED_NOAA")

    tables = session.sql("SHOW TABLES IN SCHEMA HARMONIZED_NOAA").collect()
    existing_tables = {row["name"].upper() for row in tables}

    if "NOAA_OBSERVATIONS" not in existing_tables:
        print("Creating NOAA_OBSERVATIONS table...")
        session.sql("""
            CREATE TABLE HARMONIZED_NOAA.NOAA_OBSERVATIONS (
                OBSERVATION_DATE TIMESTAMP_NTZ,
                DATATYPE VARCHAR,
                STATION_ID VARCHAR,
                ATTRIBUTES VARCHAR,
                OBS_VALUE FLOAT
            )
        """).collect()

    if "NOAA_STATIONS" not in existing_tables:
        print("Creating NOAA_STATIONS table...")
        session.sql("""
            CREATE TABLE HARMONIZED_NOAA.NOAA_STATIONS (
                STATION_ID VARCHAR PRIMARY KEY,
                NAME STRING,
                LATITUDE FLOAT,
                LONGITUDE FLOAT,
                ELEVATION FLOAT,
                ELEVATION_UNIT STRING,
                MIN_DATE DATE,
                MAX_DATE DATE,
                DATA_COVERAGE FLOAT
            )
        """).collect()

# ----------------------------------------------------------------------
# Transform Station Data
# ----------------------------------------------------------------------
def transform_station_data(session):
    print("Transforming station data...")

    session.use_schema("RAW_NOAA")

    # Debugging: Check available tables
    print("Checking available tables in RAW_NOAA schema...")
    tables = session.sql("SHOW TABLES IN SCHEMA RAW_NOAA").collect()
    for table in tables:
        print(f"Table Found: {table['name']}")
    
    df_stations_raw = session.table("RAW_NOAA.RAW_NOAA_STATION_STAGING")

    # Debugging: Check if table has data
    row_count = df_stations_raw.count()
    print(f"Row count in df_stations_raw: {row_count}")

    if row_count == 0:
        print("No records to process in station data. Skipping transformation.")
        return

    df_stations_parsed = (
    df_stations_raw
    .select(
        col("station_json")["id"].alias("STATION_ID"),
        col("station_json")["name"].alias("NAME"),
        col("station_json")["latitude"].alias("LATITUDE"),
        col("station_json")["longitude"].alias("LONGITUDE"),
        col("station_json")["elevation"].alias("ELEVATION"),
        col("station_json")["elevationUnit"].alias("ELEVATION_UNIT"),
        col("station_json")["mindate"].alias("MIN_DATE"),
        col("station_json")["maxdate"].alias("MAX_DATE"),
        col("station_json")["datacoverage"].alias("DATA_COVERAGE"),
    )
)

    # Debugging: Print schema and row count
    print("Schema of df_stations_parsed before writing:")
    df_stations_parsed.printSchema()
    print(f"Rows in df_stations_parsed before writing: {df_stations_parsed.count()}")

    session.use_schema("HARMONIZED_NOAA")

    # Ensure TEMP_NOAA_STATIONS exists before writing
    check_temp_table = session.sql("SHOW TABLES LIKE 'TEMP_NOAA_STATIONS' IN SCHEMA HARMONIZED_NOAA").collect()
    if not check_temp_table:
        print("TEMP_NOAA_STATIONS does not exist. Creating it.")
        session.sql("""
            CREATE TABLE HARMONIZED_NOAA.TEMP_NOAA_STATIONS AS
            SELECT * FROM HARMONIZED_NOAA.NOAA_STATIONS WHERE 1=0;
        """).collect()

    df_stations_parsed.write.mode("overwrite").saveAsTable("HARMONIZED_NOAA.TEMP_NOAA_STATIONS")

    session.sql("""
        MERGE INTO HARMONIZED_NOAA.NOAA_STATIONS AS target
        USING HARMONIZED_NOAA.TEMP_NOAA_STATIONS AS source
        ON target.STATION_ID = source.STATION_ID
        WHEN MATCHED THEN
            UPDATE SET target.NAME = source.NAME,
                       target.LATITUDE = source.LATITUDE,
                       target.LONGITUDE = source.LONGITUDE,
                       target.ELEVATION = source.ELEVATION,
                       target.MIN_DATE = source.MIN_DATE,
                       target.MAX_DATE = source.MAX_DATE
        WHEN NOT MATCHED THEN
            INSERT (STATION_ID, NAME, LATITUDE, LONGITUDE, ELEVATION, ELEVATION_UNIT, MIN_DATE, MAX_DATE, DATA_COVERAGE)
            VALUES (source.STATION_ID, source.NAME, source.LATITUDE, source.LONGITUDE, source.ELEVATION, source.ELEVATION_UNIT, source.MIN_DATE, source.MAX_DATE, source.DATA_COVERAGE);
    """).collect()

    session.sql("DROP TABLE HARMONIZED_NOAA.TEMP_NOAA_STATIONS").collect()
    session.sql("COMMIT;").collect()
    print("Finished transforming station data.")

# ----------------------------------------------------------------------
# Transform Observation Data (Weather Data)
# ----------------------------------------------------------------------
def transform_observation_data(session):
    print("Transforming weather observation data...")

    session.use_schema("RAW_NOAA")

    # Check if NOAA_OBSERVATIONS already has data
    existing_records = session.sql("SELECT COUNT(*) FROM HARMONIZED_NOAA.NOAA_OBSERVATIONS").collect()[0][0]

    # Always process the staging table first if it has data
    staging_count = session.sql("SELECT COUNT(*) FROM RAW_NOAA.RAW_NOAA_OBSERVATION_STAGING").collect()[0][0]
    if staging_count > 0:
        print(f"Processing {staging_count} records from RAW_NOAA_OBSERVATION_STAGING first.")
        df_obs_raw = session.table("RAW_NOAA.RAW_NOAA_OBSERVATION_STAGING")
    else:
        print("No unprocessed staging data found.")

    # Debugging: Check if table has data
    row_count = df_obs_raw.count()
    print(f"Row count in df_obs_raw: {row_count}")

    if row_count == 0:
        print("No records to process in observation data. Skipping transformation.")
        return

    df_obs_parsed = (
        df_obs_raw
        .select(
            col("weather_json")["date"].cast("TIMESTAMP_NTZ").alias("OBSERVATION_DATE"),
            col("weather_json")["datatype"].cast("STRING").alias("DATATYPE"),
            col("weather_json")["station"].cast("STRING").alias("STATION_ID"),
            col("weather_json")["attributes"].cast("STRING").alias("ATTRIBUTES"),
            col("weather_json")["value"].cast("FLOAT").alias("OBS_VALUE"),
        )
    )

    # Deduplicate data before writing to TEMP_NOAA_OBSERVATIONS
    df_obs_parsed = df_obs_parsed.drop_duplicates(["OBSERVATION_DATE", "DATATYPE", "STATION_ID"])

    # Debugging: Print schema and row count before writing
    print("Schema of df_obs_parsed before writing:")
    df_obs_parsed.printSchema()
    print(f"Rows in df_obs_parsed before writing: {df_obs_parsed.count()}")

    session.use_schema("HARMONIZED_NOAA")

    # Overwrite temp table before merging
    df_obs_parsed.write.mode("overwrite").saveAsTable("HARMONIZED_NOAA.TEMP_NOAA_OBSERVATIONS")

    # Debugging: Check temp table before merging
    print("Checking TEMP_NOAA_OBSERVATIONS data before merging...")
    df_temp_obs = session.table("HARMONIZED_NOAA.TEMP_NOAA_OBSERVATIONS")
    df_temp_obs.show(10)

    # Ensure MERGE happens only if TEMP_NOAA_OBSERVATIONS has data
    temp_count = session.sql("SELECT COUNT(*) FROM HARMONIZED_NOAA.TEMP_NOAA_OBSERVATIONS").collect()[0][0]
    if temp_count > 0:
        print(f"Merging {temp_count} records from TEMP_NOAA_OBSERVATIONS into NOAA_OBSERVATIONS.")

        session.sql("""
            MERGE INTO HARMONIZED_NOAA.NOAA_OBSERVATIONS AS target
            USING (
                SELECT DISTINCT OBSERVATION_DATE, DATATYPE, STATION_ID, ATTRIBUTES, OBS_VALUE
                FROM HARMONIZED_NOAA.TEMP_NOAA_OBSERVATIONS
            ) AS source
            ON target.OBSERVATION_DATE = source.OBSERVATION_DATE
               AND target.STATION_ID = source.STATION_ID
               AND target.DATATYPE = source.DATATYPE
        WHEN MATCHED THEN
            UPDATE SET target.OBS_VALUE = source.OBS_VALUE,
                       target.ATTRIBUTES = source.ATTRIBUTES
        WHEN NOT MATCHED THEN
            INSERT (OBSERVATION_DATE, DATATYPE, STATION_ID, ATTRIBUTES, OBS_VALUE)
            VALUES (source.OBSERVATION_DATE, source.DATATYPE, source.STATION_ID, source.ATTRIBUTES, source.OBS_VALUE);
        """).collect()

        print("Merge completed successfully.")
    else:
        print("No new records to merge. Skipping MERGE INTO step.")

    # Drop the temp table only AFTER confirming merge was successful
    session.sql("DROP TABLE HARMONIZED_NOAA.TEMP_NOAA_OBSERVATIONS").collect()
    session.sql("COMMIT;").collect()
    print("Finished transforming observation data.")

# ----------------------------------------------------------------------
# Pivot Observation Data to Get TMAX and TMIN as Columns
# ----------------------------------------------------------------------
def pivot_observation_data(session):
    print("Pivoting TMAX/TMIN observations...")

    session.use_schema("HARMONIZED_NOAA")

    #Ensure the NOAA_DAILY_TEMPS table exists before proceeding
    tables = session.sql("SHOW TABLES LIKE 'NOAA_DAILY_TEMPS' IN SCHEMA HARMONIZED_NOAA").collect()
    if not tables:
        print("NOAA_DAILY_TEMPS does not exist. Creating it now.")
        session.sql("""
            CREATE TABLE HARMONIZED_NOAA.NOAA_DAILY_TEMPS (
                OBSERVATION_DATE DATE,
                STATION_ID STRING,
                TMAX DOUBLE,
                TMIN DOUBLE
            )
        """).collect()

    #Check if NOAA_DAILY_TEMPS already has data
    existing_records = session.sql("SELECT COUNT(*) FROM HARMONIZED_NOAA.NOAA_DAILY_TEMPS").collect()[0][0]

    if existing_records == 0:
        print("No data in NOAA_DAILY_TEMPS. Processing full NOAA_OBSERVATIONS first.")
        df_obs = session.table("HARMONIZED_NOAA.NOAA_OBSERVATIONS")  # Process full dataset
    else:
        print("NOAA_DAILY_TEMPS already contains data. Checking for new records.")

        # Always process full observations table first if there is unprocessed data
        observations_count = session.sql("SELECT COUNT(*) FROM HARMONIZED_NOAA.NOAA_OBSERVATIONS").collect()[0][0]
        if observations_count > existing_records:
            print(f"Processing all {observations_count} records from NOAA_OBSERVATIONS.")
            df_obs = session.table("HARMONIZED_NOAA.NOAA_OBSERVATIONS")
        else:
            print("No new records found. Skipping pivot transformation.")

    # Filter only TMAX and TMIN data
    df_obs_filtered = df_obs.filter(col("DATATYPE").isin(["TMAX", "TMIN"]))

    # Ensure correct data type for OBSERVATION_DATE
    df_obs_filtered = df_obs_filtered.withColumn("OBSERVATION_DATE", col("OBSERVATION_DATE").cast("DATE"))

    # Pivot data
    df_pivoted = (
        df_obs_filtered.group_by("OBSERVATION_DATE", "STATION_ID")
        .pivot("DATATYPE", ["TMAX", "TMIN"])
        .agg(max_("OBS_VALUE"))
    )

    # Fix column names (removing quotes)
    cleaned_columns = {c: c.replace("'", "").replace('"', "").strip() for c in df_pivoted.columns}
    df_pivoted = df_pivoted.toDF(*[cleaned_columns[c] for c in df_pivoted.columns])

    # Debugging: Print fixed column names
    print("Fixed Pivoted Columns:", df_pivoted.columns)

    # Ensure both TMAX and TMIN exist in the pivoted DataFrame
    pivoted_columns = set(df_pivoted.columns)

    if "TMAX" not in pivoted_columns:
        print("Warning: TMAX column missing, adding NULL column")
        df_pivoted = df_pivoted.withColumn("TMAX", lit(None).cast("DOUBLE"))

    if "TMIN" not in pivoted_columns:
        print("Warning: TMIN column missing, adding NULL column")
        df_pivoted = df_pivoted.withColumn("TMIN", lit(None).cast("DOUBLE"))

    # Select final columns dynamically
    df_pivoted = df_pivoted.select(
        col("OBSERVATION_DATE"),
        col("STATION_ID"),
        col("TMAX"),  
        col("TMIN")   
    )

    # Debugging: Show pivoted data before saving
    print("Sample pivoted data before saving:")
    df_pivoted.show(10)

    # Ensure TEMP_NOAA_DAILY_TEMPS exists before merging
    check_temp_table = session.sql("SHOW TABLES LIKE 'TEMP_NOAA_DAILY_TEMPS' IN SCHEMA HARMONIZED_NOAA").collect()
    if not check_temp_table:
        print("TEMP_NOAA_DAILY_TEMPS does not exist. Creating it.")
        session.sql("""
            CREATE TABLE HARMONIZED_NOAA.TEMP_NOAA_DAILY_TEMPS AS
            SELECT * FROM HARMONIZED_NOAA.NOAA_DAILY_TEMPS WHERE 1=0;
        """).collect()

    df_pivoted.write.mode("overwrite").saveAsTable("HARMONIZED_NOAA.TEMP_NOAA_DAILY_TEMPS")

    # Debugging: Check temp table before merging
    print("Checking TEMP_NOAA_DAILY_TEMPS data before merging...")
    df_temp_pivot = session.table("HARMONIZED_NOAA.TEMP_NOAA_DAILY_TEMPS")
    df_temp_pivot.show(10)

    # Merge Data
    session.sql("""
        MERGE INTO HARMONIZED_NOAA.NOAA_DAILY_TEMPS AS target
        USING (
            SELECT DISTINCT OBSERVATION_DATE, STATION_ID, TMAX, TMIN
            FROM HARMONIZED_NOAA.TEMP_NOAA_DAILY_TEMPS
        ) AS source
        ON target.OBSERVATION_DATE = source.OBSERVATION_DATE
           AND target.STATION_ID = source.STATION_ID
    WHEN MATCHED THEN
        UPDATE SET target.TMAX = COALESCE(source.TMAX, target.TMAX),
                   target.TMIN = COALESCE(source.TMIN, target.TMIN)
    WHEN NOT MATCHED THEN
        INSERT (OBSERVATION_DATE, STATION_ID, TMAX, TMIN)
        VALUES (source.OBSERVATION_DATE, source.STATION_ID, source.TMAX, source.TMIN);
    """).collect()

    session.sql("DROP TABLE HARMONIZED_NOAA.TEMP_NOAA_DAILY_TEMPS").collect()
    session.sql("COMMIT;").collect()
    print("Finished pivoting into NOAA_DAILY_TEMPS.")


# ----------------------------------------------------------------------
# Run Transformations
# ----------------------------------------------------------------------
def run_transformations(session):
    print("Ensuring correct role, database, and schema...")
    session.use_role("NOAA_LAB_ROLE")
    session.use_database("NOAA_LAB_DB")
    session.use_schema("RAW_NOAA")

    ensure_tables_and_streams(session)
    ensure_harmonized_tables_exist(session)

    transform_station_data(session)
    transform_observation_data(session)  
    pivot_observation_data(session)

    print("All NOAA transformations completed.")

# ----------------------------------------------------------------------
# Main Execution
# ----------------------------------------------------------------------
if __name__ == "__main__":
    with Session.builder.getOrCreate() as session:
        run_transformations(session)