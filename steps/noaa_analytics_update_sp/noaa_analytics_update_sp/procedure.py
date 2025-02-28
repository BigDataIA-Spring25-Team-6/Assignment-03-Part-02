import time
from snowflake.snowpark import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


def table_exists(session, schema='', name=''):
    """
    Checks if a table exists in the given schema.
    """
    exists = session.sql(
        f"SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{name}') AS TABLE_EXISTS"
    ).collect()[0]['TABLE_EXISTS']
    return exists


def create_noaa_analytics_table(session):
    """
    Creates NOAA_ANALYTICS table if it does not exist.
    This table stores **station-wise** daily temperature data with additional analytics.
    """
    ANALYTICS_COLUMNS = [
        T.StructField("OBSERVATION_DATE", T.DateType()),
        T.StructField("STATION_ID", T.StringType()),
        T.StructField("STATION_NAME", T.StringType()),
        T.StructField("AVG_TMAX", T.DoubleType()),  # Aggregated TMAX
        T.StructField("AVG_TMIN", T.DoubleType()),  # Aggregated TMIN
        T.StructField("AVG_TAVG", T.DoubleType()),  # Aggregated TAVG
        T.StructField("AVG_DAILY_TEMP_CHANGE", T.DoubleType()),  # Aggregated Temp Change
    ]
    ANALYTICS_SCHEMA = T.StructType(ANALYTICS_COLUMNS)

    df = session.create_dataframe([[None] * len(ANALYTICS_SCHEMA.names)], schema=ANALYTICS_SCHEMA).na.drop()
    df.write.mode('overwrite').save_as_table('ANALYTICS_NOAA.NOAA_ANALYTICS')


def merge_noaa_analytics(session):
    """
    Merges new station-wise **grouped** temperature data into NOAA_ANALYTICS.
    """
    _ = session.sql('ALTER WAREHOUSE NOAA_LAB_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE').collect()

    # Check new records in NOAA_DAILY_TEMPS
    daily_temps_count = session.sql("SELECT COUNT(*) FROM HARMONIZED_NOAA.NOAA_DAILY_TEMPS").collect()[0][0]
    if daily_temps_count == 0:
        print("No new records in NOAA_DAILY_TEMPS. Skipping analytics update.")
        return

    # Load daily temperature data
    daily_temps = session.table("HARMONIZED_NOAA.NOAA_DAILY_TEMPS")

    # Load station metadata (only selecting STATION_ID and NAME)
    stations = session.table("HARMONIZED_NOAA.NOAA_STATIONS").select("STATION_ID", "NAME")

    # Join temperature data with station metadata
    analytics_staging = daily_temps.join(stations, daily_temps["STATION_ID"] == stations["STATION_ID"]) \
        .select(
            daily_temps["OBSERVATION_DATE"],
            daily_temps["STATION_ID"],
            stations["NAME"].alias("STATION_NAME"),
            daily_temps["TMAX"],
            daily_temps["TMIN"],
            F.call_udf("ANALYTICS_NOAA.CALCULATE_DAILY_AVG_TEMP", daily_temps["TMAX"], daily_temps["TMIN"]).alias("TAVG"),
            F.call_udf("ANALYTICS_NOAA.DAILY_TEMPERATURE_CHANGES_UDF", daily_temps["TMAX"], daily_temps["TMIN"]).alias("DAILY_TEMP_CHANGE")
        )

    analytics_staging = analytics_staging.with_column_renamed('"l_0000_STATION_ID"', "STATION_ID")

    print("Columns in analytics_staging:", analytics_staging.columns)

    print("Columns in daily_temps:", daily_temps.columns)
    print("Columns in stations:", stations.columns)


    # **GROUP BY STATION_ID** to get **aggregated daily temperature stats**
    analytics_grouped = analytics_staging.group_by("OBSERVATION_DATE", "STATION_ID", "STATION_NAME") \
        .agg(
            F.avg("TMAX").alias("AVG_TMAX"),
            F.avg("TMIN").alias("AVG_TMIN"),
            F.avg("TAVG").alias("AVG_TAVG"),
            F.avg("DAILY_TEMP_CHANGE").alias("AVG_DAILY_TEMP_CHANGE"),
        ).order_by(F.col("OBSERVATION_DATE").desc(), F.col("STATION_ID").asc())


    # Debugging: Check if analytics_grouped has data before writing
    analytics_count = analytics_grouped.count()
    print(f"Rows in analytics_grouped before saving TEMP_NOAA_ANALYTICS: {analytics_count}")
    analytics_grouped.show(10)  # Print sample rows


    analytics_grouped.write.mode('overwrite').save_as_table("ANALYTICS_NOAA.TEMP_NOAA_ANALYTICS")

    # Merge new aggregated data into NOAA_ANALYTICS
    session.sql("""
        MERGE INTO ANALYTICS_NOAA.NOAA_ANALYTICS AS target
        USING (
            SELECT OBSERVATION_DATE, STATION_ID, STATION_NAME, AVG_TMAX, AVG_TMIN, AVG_TAVG, AVG_DAILY_TEMP_CHANGE
            FROM ANALYTICS_NOAA.TEMP_NOAA_ANALYTICS
        ) AS source
        ON target.OBSERVATION_DATE = source.OBSERVATION_DATE
           AND target.STATION_ID = source.STATION_ID
    WHEN MATCHED THEN
        UPDATE SET target.AVG_TMAX = source.AVG_TMAX,
                   target.AVG_TMIN = source.AVG_TMIN,
                   target.AVG_TAVG = source.AVG_TAVG,
                   target.AVG_DAILY_TEMP_CHANGE = source.AVG_DAILY_TEMP_CHANGE
    WHEN NOT MATCHED THEN
        INSERT (OBSERVATION_DATE, STATION_ID, STATION_NAME, AVG_TMAX, AVG_TMIN, AVG_TAVG, AVG_DAILY_TEMP_CHANGE)
        VALUES (source.OBSERVATION_DATE, source.STATION_ID, source.STATION_NAME, source.AVG_TMAX, source.AVG_TMIN, source.AVG_TAVG, source.AVG_DAILY_TEMP_CHANGE);
    """).collect()

    session.sql("DROP TABLE ANALYTICS_NOAA.TEMP_NOAA_ANALYTICS").collect()
    session.sql("COMMIT;").collect()

    print("Successfully updated NOAA_ANALYTICS table.")

    _ = session.sql('ALTER WAREHOUSE NOAA_LAB_WH SET WAREHOUSE_SIZE = XSMALL').collect()


def main(session: Session) -> str:
    """
    Main function to ensure table exists and merge analytics data.
    """
    if not table_exists(session, schema='ANALYTICS_NOAA', name='NOAA_ANALYTICS'):
        create_noaa_analytics_table(session)

    merge_noaa_analytics(session)

    return f"Successfully processed NOAA_ANALYTICS"


# For local debugging
if __name__ == '__main__':
    with Session.builder.getOrCreate() as session:
        print(main(session))