"""Tests for utility functions."""

import unittest
import os


class TestConfig(unittest.TestCase):
    """Test cases for configuration."""

    def test_config_module_exists(self):
        """Test that config module exists."""
        module_path = "src/utils/config.py"
        self.assertTrue(os.path.exists(module_path))


class TestLogger(unittest.TestCase):
    """Test cases for logger."""

    def test_logger_module_exists(self):
        """Test that logger module exists."""
        module_path = "src/utils/logger.py"
        self.assertTrue(os.path.exists(module_path))


if __name__ == '__main__':
    unittest.main()
