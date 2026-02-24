"""Tests for data combination."""

import unittest
import os


class TestDataCombination(unittest.TestCase):
    """Test cases for data combination."""

    def test_combined_output_exists(self):
        """Test that combined output exists."""
        output_path = "data/usage/covid_comparison/daily_comparison.parquet"
        self.assertTrue(os.path.exists(output_path))

    def test_summary_stats_exists(self):
        """Test that summary stats exists."""
        output_path = "data/usage/covid_comparison/summary_stats.parquet"
        self.assertTrue(os.path.exists(output_path))

    def test_csv_output_exists(self):
        """Test that CSV output exists."""
        csv_path = "data/usage/covid_comparison/summary_stats.csv"
        self.assertTrue(os.path.exists(csv_path))

    def test_combination_module_exists(self):
        """Test that combination module exists."""
        module_path = "src/combination/combine_data.py"
        self.assertTrue(os.path.exists(module_path))


if __name__ == '__main__':
    unittest.main()
