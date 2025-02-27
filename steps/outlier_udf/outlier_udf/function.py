#------------------------------------------------------------------------------
# Hands-On Lab: Data Engineering with Snowpark
# Script:       05_fahrenheit_to_celsius_udf/app.py
# Author:       Jeremiah Hansen, Caleb Baechtold
# Last Updated: 1/9/2023
#------------------------------------------------------------------------------

# SNOWFLAKE ADVANTAGE: Snowpark Python programmability
# SNOWFLAKE ADVANTAGE: Python UDFs (with third-party packages)
# SNOWFLAKE ADVANTAGE: SnowCLI (PuPr)

import sys

def main(TMAX: float, TMIN: float) -> str:
    """
    Detect outliers based on predefined ranges for tmax and tmin in Fahrenheit.

    Args:
    tmax (float): The maximum temperature for a given date in Fahrenheit.
    tmin (float): The minimum temperature for a given date in Fahrenheit.

    Returns:
    float: A combined score indicating outlier status (1 for outliers, 0 for normal).
    """
    
    # Predefined ranges in Fahrenheit
    TMAX_min, TMAX_max = 68, 104  # Normal range for tmax (68°F to 104°F)
    TMIN_min, TMIN_max = 14, 59  # Normal range for tmin (14°F to 59°F)
    
    # Check if tmax is outside the normal range (outlier)
    TMAX_outlier = not (TMAX_min <= TMAX <= TMAX_max)
    
    # Check if tmin is outside the normal range (outlier)
    TMIN_outlier = not (TMIN_min <= TMIN <= TMIN_max)
    
    # Print the outlier status
    if TMAX_outlier and TMIN_outlier:
        return "Both TMAX and TMIN are outliers"
    elif TMAX_outlier:
        return "TMAX is an outlier"
    elif TMIN_outlier:
        return "TMIN is an outlier"
    else:
        return "Neither TMAX nor TMIN is an outlier"


# For local debugging
if __name__ == '__main__':
    if len(sys.argv) > 1:
        # Ensure that we pass the arguments as floats for the main function
        print(main(float(sys.argv[1]), float(sys.argv[2])))  # type: ignore
    else:
        # Example test values for debugging
        print(main())  # Replace with your test values
