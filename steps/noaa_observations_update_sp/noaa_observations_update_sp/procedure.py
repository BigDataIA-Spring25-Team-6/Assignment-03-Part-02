import time
from snowflake.snowpark import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


# ----------------------------------------------------------------------
# Check if table exists
# ----------------------------------------------------------------------
def table_exists(session, schema='', name=''):
    exists = session.sql(f"SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{name}') AS TABLE_EXISTS").collect()[0]['TABLE_EXISTS']
    return exists


# ----------------------------------------------------------------------
# Merge New Stream Data into NOAA_OBSERVATIONS
# ----------------------------------------------------------------------
import snowflake.snowpark.functions as F

def merge_noaa_observations(session):
    print("Running NOAA Observations Merge...")

    # Check new records in stream
    stream_count = session.sql("SELECT COUNT(*) FROM RAW_NOAA.RAW_NOAA_OBSERVATION_STREAM").collect()[0][0]
    print(f"{stream_count} new records in RAW_NOAA_OBSERVATION_STREAM")

    if stream_count == 0:
        print("No new data in the stream. Exiting merge process.")
        return

    # Read stream data and parse JSON
    df_stream = (
        session.table("RAW_NOAA.RAW_NOAA_OBSERVATION_STREAM")
        .select(
            F.col("weather_json")["date"].cast("TIMESTAMP_NTZ").alias("OBSERVATION_DATE"),
            F.col("weather_json")["datatype"].cast("STRING").alias("DATATYPE"),
            F.col("weather_json")["station"].cast("STRING").alias("STATION_ID"),
            F.col("weather_json")["attributes"].cast("STRING").alias("ATTRIBUTES"),
            F.col("weather_json")["value"].cast("FLOAT").alias("OBS_VALUE"),
        )
    )

    # Validate if stream has the correct structure
    print("Schema of parsed stream data:")
    df_stream.print_schema()

    # Read target table
    df_target = session.table("HARMONIZED_NOAA.NOAA_OBSERVATIONS")

    # Perform Merge
    df_target.merge(
        df_stream,
        (df_target["OBSERVATION_DATE"] == df_stream["OBSERVATION_DATE"]) &
        (df_target["STATION_ID"] == df_stream["STATION_ID"]) &
        (df_target["DATATYPE"] == df_stream["DATATYPE"]),
        [
            F.when_matched().update({
                "OBS_VALUE": df_stream["OBS_VALUE"],
                "ATTRIBUTES": df_stream["ATTRIBUTES"]
            }),
            F.when_not_matched().insert({
                "OBSERVATION_DATE": df_stream["OBSERVATION_DATE"],
                "DATATYPE": df_stream["DATATYPE"],
                "STATION_ID": df_stream["STATION_ID"],
                "ATTRIBUTES": df_stream["ATTRIBUTES"],
                "OBS_VALUE": df_stream["OBS_VALUE"]
            })
        ]
    )

    print("Merge completed successfully.")

# ----------------------------------------------------------------------
# Main Stored Procedure Logic
# ----------------------------------------------------------------------
def main(session: Session) -> str:
    # Ensure NOAA_OBSERVATIONS table exists before processing
    if not table_exists(session, schema="HARMONIZED_NOAA", name="NOAA_OBSERVATIONS"):
        return "ERROR: NOAA_OBSERVATIONS table does not exist. Run the transform script first."

    merge_noaa_observations(session)
    
    return "Successfully processed NOAA_OBSERVATIONS"

# ----------------------------------------------------------------------
# For Local Debugging
# ----------------------------------------------------------------------
if __name__ == "__main__":
    with Session.builder.getOrCreate() as session:
        print("Running NOAA Update Stored Procedure Locally...")
        result = main(session)
        print("Stored Procedure Result:", result)