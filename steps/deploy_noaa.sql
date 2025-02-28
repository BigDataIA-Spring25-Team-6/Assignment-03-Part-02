--!jinja
/*-----------------------------------------------------------------------------
NOAA Lab Deployment - {{ env|upper }} Environment
-----------------------------------------------------------------------------*/

USE ROLE NOAA_LAB_ROLE;
USE DATABASE NOAA_LAB_DB;
USE SCHEMA {{ env }}_NOAA;

-- ----------------------------------------------------------------------------
-- Create Environment-Specific Schema
-- ----------------------------------------------------------------------------
CREATE OR REPLACE SCHEMA NOAA_LAB_DB.{{ env }}_NOAA;

-- ----------------------------------------------------------------------------
-- Deploy Stored Procedures
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE NOAA_LAB_DB.{{ env }}_NOAA.NOAA_LOAD_RAW_SP()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  HANDLER = 'procedure.main'
  PACKAGES = ('snowflake-snowpark-python')
  EXTERNAL_ACCESS_INTEGRATIONS = (MY_S3_INTEGRATION)
  IMPORTS = ('@NOAA_LAB_DB.{{ env }}_NOAA_STAGE/noaa_load_raw_sp.zip');

CREATE OR REPLACE PROCEDURE NOAA_LAB_DB.{{ env }}_NOAA.NOAA_TRANSFORM_RAW_SP()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  HANDLER = 'procedure.main'
  PACKAGES = ('snowflake-snowpark-python')
  IMPORTS = ('@NOAA_LAB_DB.{{ env }}_NOAA_STAGE/noaa_transform_raw_sp.zip');

CREATE OR REPLACE PROCEDURE NOAA_LAB_DB.{{ env }}_NOAA.NOAA_OBSERVATIONS_UPDATE_SP()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  HANDLER = 'procedure.main'
  PACKAGES = ('snowflake-snowpark-python')
  IMPORTS = ('@NOAA_LAB_DB.{{ env }}_NOAA_STAGE/noaa_observations_update_sp.zip');

CREATE OR REPLACE PROCEDURE NOAA_LAB_DB.{{ env }}_NOAA.NOAA_ANALYTICS_SP()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  HANDLER = 'procedure.main'
  PACKAGES = ('snowflake-snowpark-python')
  IMPORTS = ('@NOAA_LAB_DB.{{ env }}_NOAA_STAGE/noaa_analytics_sp.zip');

-- ----------------------------------------------------------------------------
-- Deploy Snowflake Tasks
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TASK NOAA_LAB_DB.{{ env }}_NOAA.LOAD_NOAA_RAW_TASK
WAREHOUSE = NOAA_LAB_WH
SCHEDULE = 'USING CRON 0 6 * * * UTC'
AS
CALL NOAA_LAB_DB.{{ env }}_NOAA.NOAA_LOAD_RAW_SP();

CREATE OR REPLACE TASK NOAA_LAB_DB.{{ env }}_NOAA.TRANSFORM_NOAA_TASK
WAREHOUSE = NOAA_LAB_WH
AFTER NOAA_LAB_DB.{{ env }}_NOAA.LOAD_NOAA_RAW_TASK
AS
CALL NOAA_LAB_DB.{{ env }}_NOAA.NOAA_TRANSFORM_RAW_SP();

CREATE OR REPLACE TASK NOAA_LAB_DB.{{ env }}_NOAA.UPDATE_NOAA_OBSERVATIONS_TASK
WAREHOUSE = NOAA_LAB_WH
AFTER NOAA_LAB_DB.{{ env }}_NOAA.TRANSFORM_NOAA_TASK
WHEN SYSTEM$STREAM_HAS_DATA('NOAA_LAB_DB.RAW_NOAA.RAW_NOAA_OBSERVATION_STREAM')
AS
CALL NOAA_LAB_DB.{{ env }}_NOAA.NOAA_OBSERVATIONS_UPDATE_SP();

CREATE OR REPLACE TASK NOAA_LAB_DB.{{ env }}_NOAA.UPDATE_NOAA_ANALYTICS_TASK
WAREHOUSE = NOAA_LAB_WH
AFTER NOAA_LAB_DB.{{ env }}_NOAA.UPDATE_NOAA_OBSERVATIONS_TASK
AS
CALL NOAA_LAB_DB.{{ env }}_NOAA.NOAA_ANALYTICS_SP();

-- ----------------------------------------------------------------------------
-- Activate & Execute Tasks
-- ----------------------------------------------------------------------------
ALTER TASK NOAA_LAB_DB.{{ env }}_NOAA.TRANSFORM_NOAA_TASK RESUME;
ALTER TASK NOAA_LAB_DB.{{ env }}_NOAA.UPDATE_NOAA_OBSERVATIONS_TASK RESUME;
ALTER TASK NOAA_LAB_DB.{{ env }}_NOAA.UPDATE_NOAA_ANALYTICS_TASK RESUME;
ALTER TASK NOAA_LAB_DB.{{ env }}_NOAA.LOAD_NOAA_RAW_TASK RESUME;