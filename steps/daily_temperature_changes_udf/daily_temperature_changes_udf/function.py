import sys

def main(TMAX: float, TMIN: float) -> float:
    """
    Returns the daily change of temperature in a single day.
    If either TMAX or TMIN is None, returns None.
    """
    if TMAX is None or TMIN is None:
        return None

    return float(TMAX) - float(TMIN)

# For local debugging
if __name__ =='__main__':
    if len(sys.argv)>2:
        tmax = None if sys.argv[1] == "None" else float(sys.argv[1])
        tmin = None if sys.argv[2] == "None" else float(sys.argv[2])
        print(main(tmax, tmin))
    else:
        print(main())