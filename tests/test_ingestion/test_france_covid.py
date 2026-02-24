"""
Unit tests for France COVID ingestion module.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

import pandas as pd

from src.ingestion.france_covid import FranceCovidIngestion


class TestFranceCovidIngestion(unittest.TestCase):
    """Test cases for FranceCovidIngestion class."""

    def setUp(self):
        """Set up test fixtures."""
        self.ingestion = FranceCovidIngestion()

    def test_init(self):
        """Test initialization."""
        self.assertIsNotNone(self.ingestion.logger)
        self.assertIsNotNone(self.ingestion.config)
        self.assertIsNotNone(self.ingestion.base_url)
        self.assertIsNotNone(self.ingestion.resource_id)

    def test_build_url(self):
        """Test URL building."""
        url = self.ingestion._build_url()
        self.assertIn("data.gouv.fr", url)
        self.assertIn(self.ingestion.resource_id, url)

    def test_validate_data_empty(self):
        """Test validation with empty DataFrame."""
        df_empty = pd.DataFrame()
        self.assertFalse(self.ingestion.validate_data(df_empty))

    def test_validate_data_missing_columns(self):
        """Test validation with missing columns."""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        self.assertFalse(self.ingestion.validate_data(df))

    def test_validate_data_valid(self):
        """Test validation with valid DataFrame."""
        df = pd.DataFrame({
            'dep': ['01', '02'],
            'sexe': ['0', '0'],
            'jour': ['2024-01-01', '2024-01-02'],
            'hosp': [100, 150],
            'rea': [10, 15],
            'dc': [5, 7],
            'rad': [50, 60]
        })
        self.assertTrue(self.ingestion.validate_data(df))

    def test_get_output_path(self):
        """Test output path generation."""
        date = "2024-11-25"
        path = self.ingestion._get_output_path(date)

        self.assertIn("france", str(path))
        self.assertIn(date, str(path))
        self.assertTrue(str(path).endswith('.csv'))

    @patch('src.ingestion.france_covid.requests.get')
    def test_fetch_data_success(self, mock_get):
        """Test successful data fetching."""
        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"dep;sexe;jour;hosp;rea;dc;rad\n01;0;2024-01-01;100;10;5;50"
        mock_get.return_value = mock_response

        df = self.ingestion.fetch_data()

        self.assertFalse(df.empty)
        self.assertIn('dep', df.columns)

    @patch('src.ingestion.france_covid.requests.get')
    def test_fetch_data_retry(self, mock_get):
        """Test retry mechanism on failure."""
        mock_get.side_effect = [
            Exception("Network error"),
            Exception("Network error"),
            Mock(status_code=200, content=b"dep;sexe;jour;hosp;rea;dc;rad\n01;0;2024-01-01;100;10;5;50")
        ]

        df = self.ingestion.fetch_data()

        self.assertEqual(mock_get.call_count, 3)


if __name__ == '__main__':
    unittest.main()
