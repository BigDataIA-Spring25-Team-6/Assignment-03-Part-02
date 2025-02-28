USE ROLE NOAA_LAB_ROLE;
USE WAREHOUSE NOAA_LAB_WH;
USE SCHEMA RAW_NOAA;

-- ----------------------------------------------------------------------------
-- Task 1: Load raw data from S3 into Snowflake Staging Tables
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TASK LOAD_NOAA_RAW_TASK
WAREHOUSE = NOAA_LAB_WH
SCHEDULE = 'USING CRON 30 6 * * * UTC'
AS
CALL RAW_NOAA.NOAA_LOAD_RAW_SP();

-- ----------------------------------------------------------------------------
-- Task 2: Transform raw NOAA data into ha)rmonized tables
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TASK TRANSFORM_NOAA_TASK
WAREHOUSE = NOAA_LAB_WH
AFTER LOAD_NOAA_RAW_TASK
AS
CALL HARMONIZED_NOAA.NOAA_TRANSFORM_RAW_SP();

-- ----------------------------------------------------------------------------
-- Task 3: Update Observations Table Incrementally using Streams
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TASK UPDATE_NOAA_OBSERVATIONS_TASK
WAREHOUSE = NOAA_LAB_WH
AFTER TRANSFORM_NOAA_TASK
WHEN SYSTEM$STREAM_HAS_DATA('RAW_NOAA_OBSERVATION_STREAM')
AS
CALL HARMONIZED_NOAA.NOAA_OBSERVATIONS_UPDATE_SP();

-- ----------------------------------------------------------------------------
-- Task 4: Run Analytics Calculations
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TASK UPDATE_NOAA_ANALYTICS_TASK
WAREHOUSE = NOAA_LAB_WH
AFTER UPDATE_NOAA_OBSERVATIONS_TASK
AS
CALL ANALYTICS_NOAA.NOAA_ANALYTICS_SP();

-- ----------------------------------------------------------------------------
-- Step 3: Activate & Execute Tasks
-- ----------------------------------------------------------------------------

ALTER TASK TRANSFORM_NOAA_TASK RESUME;
ALTER TASK UPDATE_NOAA_OBSERVATIONS_TASK RESUME;
ALTER TASK UPDATE_NOAA_ANALYTICS_TASK RESUME;
ALTER TASK LOAD_NOAA_RAW_TASK RESUME;