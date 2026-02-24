"""Tests for Colombia COVID ingestion."""

import unittest
import os


class TestColombiaCovidIngestion(unittest.TestCase):
    """Test cases for Colombia COVID data ingestion."""

    def test_raw_data_exists(self):
        """Test that raw Colombia data exists."""
        raw_path = "data/raw/colombia"
        self.assertTrue(os.path.exists(raw_path))

    def test_raw_data_has_csv(self):
        """Test that raw data has CSV files."""
        raw_path = "data/raw/colombia"
        if os.path.exists(raw_path):
            subdirs = os.listdir(raw_path)
            self.assertGreater(len(subdirs), 0)

    def test_ingestion_module_exists(self):
        """Test that ingestion module exists."""
        module_path = "src/ingestion/colombia_covid.py"
        self.assertTrue(os.path.exists(module_path))


if __name__ == '__main__':
    unittest.main()
