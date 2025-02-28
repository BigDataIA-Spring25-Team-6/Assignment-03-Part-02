import unittest
from function import main  # Make sure to import the 'main' function from your UDF file

class TestOutlierUDF(unittest.TestCase):

    def test_both_outliers(self):
        """Test case where both TMAX and TMIN are outliers."""
        result = main(110.0, 10.0)  # TMAX = 110 is above the normal range; TMIN = 10 is below the normal range
        self.assertEqual(result, "Both TMAX and TMIN are outliers")
    
    def test_tmax_outlier(self):
        """Test case where only TMAX is an outlier."""
        result = main(110.0, 30.0)  # TMAX = 110 is above the normal range, but TMIN = 30 is within the normal range
        self.assertEqual(result, "TMAX is an outlier")
    
    def test_tmin_outlier(self):
        """Test case where only TMIN is an outlier."""
        result = main(70.0, 5.0)  # TMAX = 70 is within the normal range, but TMIN = 5 is below the normal range
        self.assertEqual(result, "TMIN is an outlier")
    
    def test_neither_outlier(self):
        """Test case where neither TMAX nor TMIN is an outlier."""
        result = main(75.0, 50.0)  # TMAX = 75 is within the normal range, and TMIN = 50 is within the normal range
        self.assertEqual(result, "Neither TMAX nor TMIN is an outlier")
    
    def test_tmax_lower_outlier(self):
        """Test case where TMAX is below the normal range."""
        result = main(60.0, 30.0)  # TMAX = 60 is below the normal range, but TMIN = 30 is within the normal range
        self.assertEqual(result, "TMAX is an outlier")
    
    def test_tmin_higher_outlier(self):
        """Test case where TMIN is above the normal range."""
        result = main(80.0, 65.0)  # TMAX = 80 is within the normal range, but TMIN = 65 is above the normal range
        self.assertEqual(result, "TMIN is an outlier")

if __name__ == '__main__':
    unittest.main()
