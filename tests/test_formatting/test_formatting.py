"""Tests for data formatting."""

import unittest
import os


class TestFormattingFrance(unittest.TestCase):
    """Test cases for France data formatting."""

    def test_parquet_output_exists(self):
        """Test that parquet output directory exists."""
        parquet_path = "data/formatted/france/covid_hospitalizations.parquet"
        self.assertTrue(os.path.exists(parquet_path))

    def test_parquet_has_files(self):
        """Test that parquet directory has files."""
        parquet_path = "data/formatted/france/covid_hospitalizations.parquet"
        if os.path.exists(parquet_path):
            files = os.listdir(parquet_path)
            parquet_files = [f for f in files if f.endswith('.parquet')]
            self.assertGreater(len(parquet_files), 0)

    def test_formatting_module_exists(self):
        """Test that formatting module exists."""
        module_path = "src/formatting/format_france.py"
        self.assertTrue(os.path.exists(module_path))


class TestFormattingColombia(unittest.TestCase):
    """Test cases for Colombia data formatting."""

    def test_parquet_output_exists(self):
        """Test that parquet output directory exists."""
        parquet_path = "data/formatted/colombia/covid_cases.parquet"
        self.assertTrue(os.path.exists(parquet_path))

    def test_parquet_has_files(self):
        """Test that parquet directory has files."""
        parquet_path = "data/formatted/colombia/covid_cases.parquet"
        if os.path.exists(parquet_path):
            files = os.listdir(parquet_path)
            parquet_files = [f for f in files if f.endswith('.parquet')]
            self.assertGreater(len(parquet_files), 0)


if __name__ == '__main__':
    unittest.main()
