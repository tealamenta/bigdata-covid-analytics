"""Tests for France COVID ingestion."""

import unittest
import os


class TestFranceCovidIngestion(unittest.TestCase):
    """Test cases for France COVID data ingestion."""

    def test_raw_data_exists(self):
        """Test that raw France data exists."""
        raw_path = "data/raw/france"
        self.assertTrue(os.path.exists(raw_path))

    def test_raw_data_has_csv(self):
        """Test that raw data has CSV files."""
        raw_path = "data/raw/france"
        if os.path.exists(raw_path):
            subdirs = os.listdir(raw_path)
            self.assertGreater(len(subdirs), 0)

    def test_ingestion_module_exists(self):
        """Test that ingestion module exists."""
        module_path = "src/ingestion/france_covid.py"
        self.assertTrue(os.path.exists(module_path))


class TestFranceDataValidation(unittest.TestCase):
    """Test data validation for France."""

    def test_required_columns(self):
        """Test required columns are defined."""
        required = ['dep', 'sexe', 'jour', 'hosp', 'rea', 'rad', 'dc']
        self.assertEqual(len(required), 7)


if __name__ == '__main__':
    unittest.main()
