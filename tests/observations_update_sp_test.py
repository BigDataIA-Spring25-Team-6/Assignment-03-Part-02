import unittest
from unittest.mock import MagicMock
from snowflake.snowpark import Session
from steps.noaa_observations_update_sp.noaa_observations_update_sp.procedure import main, merge_noaa_observations, table_exists
import snowflake.snowpark.functions as F

class TestNoaaObservationsMerge(unittest.TestCase):

    def setUp(self):
        # Create a mock session for testing
        self.session = MagicMock(Session)

    def test_table_exists(self):
        # Mock SQL query result to simulate table exists
        self.session.sql.return_value.collect.return_value = [{'TABLE_EXISTS': True}]
        result = table_exists(self.session, schema="HARMONIZED_NOAA", name="NOAA_OBSERVATIONS")
        self.assertTrue(result)
        
        self.session.sql.return_value.collect.return_value = [{'TABLE_EXISTS': False}]
        result = table_exists(self.session, schema="HARMONIZED_NOAA", name="NOAA_OBSERVATIONS")
        self.assertFalse(result)


if __name__ == '__main__':
    # Create a test suite and run tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestNoaaObservationsMerge)
    
    # Run the tests and capture the results
    result = unittest.TextTestRunner(verbosity=2).run(suite)

    # Print the number of passed tests
    print(f"Tests run: {result.testsRun}")
    print(f"Passed: {len(result.successes)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
