-- ----------------------------------------------------------------------------
-- STEP #1: Create Role, Database, Warehouse
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- 1.1: Create a custom role for this lab
SET MY_USER = CURRENT_USER();

CREATE OR REPLACE ROLE NOAA_LAB_ROLE;
GRANT ROLE NOAA_LAB_ROLE TO ROLE SYSADMIN;
GRANT ROLE NOAA_LAB_ROLE TO USER IDENTIFIER($MY_USER);

-- 1.2: Grant necessary privileges
GRANT EXECUTE TASK ON ACCOUNT TO ROLE NOAA_LAB_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE NOAA_LAB_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE NOAA_LAB_ROLE;

-- 1.3: Create a database for NOAA data processing
CREATE OR REPLACE DATABASE NOAA_LAB_DB;
GRANT OWNERSHIP ON DATABASE NOAA_LAB_DB TO ROLE NOAA_LAB_ROLE;

-- 1.4: Create a Snowflake Warehouse
CREATE OR REPLACE WAREHOUSE NOAA_LAB_WH 
  WAREHOUSE_SIZE = XSMALL 
  AUTO_SUSPEND = 300 
  AUTO_RESUME = TRUE;
GRANT OWNERSHIP ON WAREHOUSE NOAA_LAB_WH TO ROLE NOAA_LAB_ROLE;

-- ----------------------------------------------------------------------------
-- STEP #2: Create Required Schemas
-- ----------------------------------------------------------------------------
USE ROLE NOAA_LAB_ROLE;
USE WAREHOUSE NOAA_LAB_WH;
USE DATABASE NOAA_LAB_DB;

-- 2.1: Create Schema for Raw Data
CREATE OR REPLACE SCHEMA RAW_NOAA;  -- Staging data from S3 before transformation

-- 2.2: Create Schema for Harmonized Data
CREATE OR REPLACE SCHEMA HARMONIZED_NOAA;  -- Cleaned & structured NOAA data

-- 2.3: Create Schema for Analytics & Reporting
CREATE OR REPLACE SCHEMA ANALYTICS_NOAA;  -- Final analytics tables for dashboards

-- ----------------------------------------------------------------------------
-- STEP #3: Create External Stage for S3 Storage
-- ----------------------------------------------------------------------------
USE SCHEMA RAW_NOAA;

CREATE OR REPLACE FILE FORMAT JSON_FILE_FORMAT
TYPE = 'JSON'
STRIP_OUTER_ARRAY = TRUE
COMPRESSION = AUTO;

CREATE OR REPLACE STAGE RAW_NOAA.NOAA_RAW_STAGE
URL = 's3://team-6-a3-data-storage/'
STORAGE_INTEGRATION = MY_S3_INTEGRATION
FILE_FORMAT = (TYPE = JSON);
-- ----------------------------------------------------------------------------
-- STEP #4: Confirm Setup is Complete
-- ----------------------------------------------------------------------------
SHOW DATABASES LIKE 'NOAA_LAB_DB';
SHOW SCHEMAS IN DATABASE NOAA_LAB_DB;
SHOW STAGES IN SCHEMA RAW_NOAA;
SHOW FILE FORMATS IN SCHEMA RAW_NOAA;

-- ----------------------------------------------------------------------------
-- STEP #5: SQL UDF TO CALCULATE THE AVERAGE TEMPERATURE
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ANALYTICS_NOAA.CALCULATE_DAILY_AVG_TEMP(TMAX FLOAT, TMIN FLOAT)
RETURNS FLOAT AS
$$
    (TMAX + TMIN) / 2
$$;