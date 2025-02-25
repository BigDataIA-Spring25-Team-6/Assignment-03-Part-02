USE ROLE ACCOUNTADMIN;

-- 2.1: Create a custom role for this lab
SET MY_USER = CURRENT_USER();

CREATE OR REPLACE ROLE NOAA_LAB_ROLE;
-- Optionally grant to SYSADMIN, so you can switch easily
GRANT ROLE NOAA_LAB_ROLE TO ROLE SYSADMIN;
GRANT ROLE NOAA_LAB_ROLE TO USER IDENTIFIER($MY_USER);

-- 2.2: Privileges for the role
GRANT EXECUTE TASK ON ACCOUNT TO ROLE NOAA_LAB_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE NOAA_LAB_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE NOAA_LAB_ROLE;

-- 2.3: Create database for the lab
CREATE OR REPLACE DATABASE NOAA_LAB_DB;
GRANT OWNERSHIP ON DATABASE NOAA_LAB_DB TO ROLE NOAA_LAB_ROLE;

-- 2.4: Create a warehouse for the lab
CREATE OR REPLACE WAREHOUSE NOAA_LAB_WH 
  WAREHOUSE_SIZE = XSMALL 
  AUTO_SUSPEND = 300 
  AUTO_RESUME = TRUE;
GRANT OWNERSHIP ON WAREHOUSE NOAA_LAB_WH TO ROLE NOAA_LAB_ROLE;


/*-----------------------------------------------------------------------------
  STEP #3: Create database-level objects
  ----------------------------------------------------------------------------*/
USE ROLE NOAA_LAB_ROLE;
USE WAREHOUSE NOAA_LAB_WH;
USE DATABASE NOAA_LAB_DB;

-- 3.1: Create Schemas
CREATE OR REPLACE SCHEMA RAW_NOAA;         -- For raw staging of NOAA data
CREATE OR REPLACE SCHEMA HARMONIZED_NOAA;  -- For cleaned/harmonized data
CREATE OR REPLACE SCHEMA ANALYTICS_NOAA;   -- For final analytic tables

-- (Optional) Create an external schema for references to external data
CREATE OR REPLACE SCHEMA EXTERNAL_NOAA;

-- 3.2: Create external objects (e.g., stage, file format) if you have S3 or GCS
USE SCHEMA EXTERNAL_NOAA;

CREATE OR REPLACE FILE FORMAT JSON_FILE_FORMAT
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE   -- Adjust this based on how your JSON is structured
  COMPRESSION = AUTO
;

-- Example external stage pointing to an S3 bucket with NOAA data (adjust the URL).
-- Replace <S3_PATH> with your actual S3 or GCS path.
CREATE OR REPLACE STAGE NOAA_RAW_STAGE
  URL = 's3://team-6-a3-data-storage/'
  FILE_FORMAT = JSON_FILE_FORMAT
  COMMENT = 'Stage for raw NOAA Weather JSON files'
;

-- 3.3: (Optional) Create placeholders for any UDFs or stored procedures in Analytics schema
USE SCHEMA ANALYTICS_NOAA;

-- Example SQL UDF converting Fahrenheit to Celsius
-- Uncomment and adapt if needed:
-- CREATE OR REPLACE FUNCTION ANALYTICS_NOAA.FAHRENHEIT_TO_CELSIUS_UDF(TEMP_F NUMBER(35,4))
-- RETURNS NUMBER(35,4)
-- AS
-- $$
--   (TEMP_F - 32) * (5/9)
-- $$;

/*-----------------------------------------------------------------------------
  STEP #4: Confirmation
  ----------------------------------------------------------------------------
  At this point, you have:
  - A custom role (NOAA_LAB_ROLE)
  - A warehouse (NOAA_LAB_WH)
  - A database (NOAA_LAB_DB) with 3 schemas (RAW_NOAA, HARMONIZED_NOAA, ANALYTICS_NOAA)
  - An external schema (EXTERNAL_NOAA) with a JSON file format & stage
  - Example UDF(s) for future usage
-----------------------------------------------------------------------------*/

-- Optional: Show results
SHOW DATABASES LIKE 'NOAA_LAB_DB';
SHOW SCHEMAS IN DATABASE NOAA_LAB_DB;
SHOW ROLES LIKE 'NOAA_LAB_ROLE';
SHOW WAREHOUSES LIKE 'NOAA_LAB_WH';
SHOW STAGES IN SCHEMA EXTERNAL_NOAA;
SHOW FILE FORMATS IN SCHEMA EXTERNAL_NOAA;

/*
  STEP #4: Placeholders for Streams, Tasks, Python UDFs, & Stored Procedures
  -------------------------------------------------------------------------
  You typically create Streams and Tasks in separate scripts
  after your RAW tables exist. For example:

  USE SCHEMA RAW_NOAA;

  CREATE OR REPLACE STREAM RAW_NOAA_OBSERVATION_STREAM 
    ON TABLE RAW_NOAA_OBSERVATION_STAGING
    SHOW_INITIAL_ROWS = TRUE;
  
  -- Then a Task to call a stored procedure or run Snowpark transformations, e.g.:
  -- CREATE OR REPLACE TASK LOAD_NOAA_DATA_TASK
  --  WAREHOUSE = NOAA_LAB_WH
  --  SCHEDULE = 'USING CRON 0 6 * * * UTC'
  -- AS
  --   CALL HARMONIZED_NOAA.UPDATE_NOAA_SP();

  Similarly for Python UDFs and stored procedures:
  -- CREATE OR REPLACE PROCEDURE HARMONIZED_NOAA.UPDATE_NOAA_SP() ...
*/