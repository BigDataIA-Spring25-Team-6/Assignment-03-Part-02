import sys

def main(TMAX: float, TMIN: float) -> float:
    """
    returns the daily change of temperature in a single day
    """
    return float(TMAX - TMIN)

# For local debugging
if __name__ =='__main__':
    if len(sys.argv)>1:
        print(main(float(sys.argv[1]), float(sys.argv[2])))
    
    else:
        print(main())