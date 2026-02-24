"""
Unit tests for Colombia COVID ingestion module.
"""

import unittest
from unittest.mock import Mock, patch
from pathlib import Path

import pandas as pd

from src.ingestion.colombia_covid import ColombiaCovidIngestion


class TestColombiaCovidIngestion(unittest.TestCase):
    """Test cases for ColombiaCovidIngestion class."""

    def setUp(self):
        """Set up test fixtures."""
        self.ingestion = ColombiaCovidIngestion()

    def test_init(self):
        """Test initialization."""
        self.assertIsNotNone(self.ingestion.logger)
        self.assertIsNotNone(self.ingestion.config)
        self.assertIsNotNone(self.ingestion.base_url)
        self.assertIsNotNone(self.ingestion.resource_id)

    def test_build_url(self):
        """Test URL building."""
        url = self.ingestion._build_url()
        self.assertIn("datos.gov.co", url)
        self.assertIn(self.ingestion.resource_id, url)
        self.assertIn("$limit", url)

    def test_validate_data_empty(self):
        """Test validation with empty DataFrame."""
        df_empty = pd.DataFrame()
        self.assertFalse(self.ingestion.validate_data(df_empty))

    def test_validate_data_missing_columns(self):
        """Test validation with missing essential columns."""
        df = pd.DataFrame({'col1': [1, 2, 3]})
        self.assertFalse(self.ingestion.validate_data(df))

    def test_validate_data_valid(self):
        """Test validation with valid DataFrame."""
        df = pd.DataFrame({
            'fecha_reporte_web': ['2024-01-01', '2024-01-02'],
            'departamento_nom': ['Bogotá', 'Antioquia'],
            'ciudad_municipio_nom': ['Bogotá', 'Medellín'],
            'edad': [45, 32],
            'sexo': ['F', 'M'],
            'estado': ['Recuperado', 'Fallecido']
        })
        self.assertTrue(self.ingestion.validate_data(df))

    def test_get_output_path(self):
        """Test output path generation."""
        date = "2024-11-25"
        path = self.ingestion._get_output_path(date)

        self.assertIn("colombia", str(path))
        self.assertIn(date, str(path))
        self.assertTrue(str(path).endswith('.csv'))

    @patch('src.ingestion.colombia_covid.requests.get')
    def test_fetch_data_success(self, mock_get):
        """Test successful data fetching."""
        # Mock response with Colombian data structure
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"fecha_reporte_web,departamento_nom,ciudad_municipio_nom,edad,sexo,estado\n2024-01-01,Bogota,Bogota,45,F,Recuperado"
        mock_get.return_value = mock_response

        df = self.ingestion.fetch_data()

        self.assertFalse(df.empty)
        self.assertIn('fecha_reporte_web', df.columns)
        self.assertIn('departamento_nom', df.columns)

    @patch('src.ingestion.colombia_covid.requests.get')
    def test_fetch_data_retry(self, mock_get):
        """Test retry mechanism on failure."""
        mock_get.side_effect = [
            Exception("Network error"),
            Exception("Network error"),
            Mock(
                status_code=200,
                content=b"fecha_reporte_web,departamento_nom\n2024-01-01,Bogota"
            )
        ]

        df = self.ingestion.fetch_data()

        self.assertEqual(mock_get.call_count, 3)


if __name__ == '__main__':
    unittest.main()
