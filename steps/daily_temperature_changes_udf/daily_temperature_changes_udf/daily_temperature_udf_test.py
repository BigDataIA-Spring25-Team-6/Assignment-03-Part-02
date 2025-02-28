import unittest
from function import main

class TestTemperatureChange(unittest.TestCase):
    def test_normal_positive_difference(self):
        """Test with TMAX > TMIN; should return a positive difference."""
        self.assertEqual(main(30.0, 10.0), 20.0)

    def test_normal_zero_difference(self):
        """Test with TMAX equal to TMIN; should return zero."""
        self.assertEqual(main(15.0, 15.0), 0.0)

    def test_normal_negative_difference(self):
        """Test with TMAX < TMIN; should return a negative difference."""
        self.assertEqual(main(10.0, 15.0), -5.0)

    def test_tmax_none(self):
        """Test with TMAX as None; should return None."""
        self.assertIsNone(main(None, 10.0))

    def test_tmin_none(self):
        """Test with TMIN as None; should return None."""
        self.assertIsNone(main(10.0, None))

    def test_both_none(self):
        """Test with both TMAX and TMIN as None; should return None."""
        self.assertIsNone(main(None, None))

if __name__ == '__main__':
    unittest.main()