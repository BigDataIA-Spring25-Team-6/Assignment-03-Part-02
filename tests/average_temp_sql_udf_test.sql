-- ----------------------------------------------------------------------------
-- SQL UDF TO CALCULATE THE AVERAGE TEMPERATURE
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION ANALYTICS_NOAA.CALCULATE_DAILY_AVG_TEMP(TMAX FLOAT, TMIN FLOAT)
RETURNS FLOAT AS
$$
    (TMAX + TMIN) / 2
$$;


-- ----------------------------------------------------------------------------
-- Unit Test for the SQL UDF TO CALCULATE THE AVERAGE TEMPERATURE
-- ----------------------------------------------------------------------------
SELECT
    100 AS tmax_value,
    60 AS tmin_value,
    ANALYTICS_NOAA.CALCULATE_DAILY_AVG_TEMP(100, 60) AS avg_temp,
    CASE
        WHEN ANALYTICS_NOAA.CALCULATE_DAILY_AVG_TEMP(100, 60) = 80 THEN 'Pass'
        ELSE 'Fail'
    END AS test_status;