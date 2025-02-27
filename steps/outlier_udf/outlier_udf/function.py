
import sys

def main(TMAX: float, TMIN: float) -> str:
    """
    Detect outliers based on predefined ranges for tmax and tmin in Fahrenheit.

    Args:
    tmax (float): The maximum temperature for a given date in Fahrenheit.
    tmin (float): The minimum temperature for a given date in Fahrenheit.

    Returns:
    String: Mentions if TMIN or TMAX is an outlier.
    """
    
    # Predefined ranges in Fahrenheit
    TMAX_min, TMAX_max = 68, 104  
    TMIN_min, TMIN_max = 14, 59  
    
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
