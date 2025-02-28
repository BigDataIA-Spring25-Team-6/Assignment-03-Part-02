import unittest
from unittest.mock import MagicMock
from snowflake.snowpark import Session
from steps.noaa_analytics_update_sp.noaa_analytics_update_sp.procedure import table_exists


class TestNoaaAnalyticsUpdate(unittest.TestCase):
    
    def setUp(self):
        # Create a mock session
        self.mock_session = MagicMock(spec=Session)
        
    def test_table_exists_when_table_does_not_exist(self):
        # Mock the SQL query to return that the table does not exist
        self.mock_session.sql.return_value.collect.return_value = [{'TABLE_EXISTS': False}]
        
        # Run the table_exists function
        result = table_exists(self.mock_session, schema='ANALYTICS_NOAA', name='NOAA_ANALYTICS')
        
        # Assert the result is False (table does not exist)
        self.assertFalse(result)

    def test_table_exists_when_table_exists(self):
        # Mock the SQL query to return that the table exists
        self.mock_session.sql.return_value.collect.return_value = [{'TABLE_EXISTS': True}]
        
        # Run the table_exists function
        result = table_exists(self.mock_session, schema='ANALYTICS_NOAA', name='NOAA_ANALYTICS')
        
        # Assert the result is True (table exists)
        self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()