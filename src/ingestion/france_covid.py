"""
COVID-19 Data Ingestion - France.

Fetches hospitalization data from data.gouv.fr API
and stores it in the Data Lake raw layer.
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

from src.utils.logger import get_logger, log_execution_time
from src.utils.config import load_config


class FranceCovidIngestion:
    """
    Ingestion class for France COVID-19 data.

    Fetches data from data.gouv.fr and stores in Data Lake.
    """

    def __init__(self, config_dir: str = "config/dev"):
        """
        Initialize France COVID ingestion.

        Args:
            config_dir: Configuration directory path
        """
        self.logger = get_logger("ingestion.france", "logs/ingestion")
        self.config = load_config(config_dir)

        # Get API configuration
        api_config = self.config.get_api_config('france', 'hospitalizations')
        self.base_url = self.config.get('apis.data_sources.france.base_url')
        self.resource_id = api_config['resource_id']
        self.encoding = api_config.get('encoding', 'utf-8')

        # Data Lake paths
        data_lake = self.config.get_data_lake_paths()
        self.raw_base_path = Path(data_lake['raw']['base_path'])

        # Request settings
        request_settings = self.config.get('apis.request_settings', {})
        self.timeout = request_settings.get('timeout', 60)
        self.max_retries = request_settings.get('max_retries', 3)
        self.user_agent = request_settings.get(
            'user_agent',
            'BigDataCovidAnalytics/1.0'
        )

        self.logger.info("France COVID Ingestion initialized")

    def _build_url(self) -> str:
        """
        Build the full API URL.

        Returns:
            Complete API URL
        """
        return f"{self.base_url}/{self.resource_id}"

    @log_execution_time(get_logger("ingestion.france", "logs/ingestion"))
    def fetch_data(self) -> pd.DataFrame:
        """
        Fetch data from data.gouv.fr API.

        Returns:
            DataFrame with COVID data

        Raises:
            requests.RequestException: If API request fails
        """
        url = self._build_url()
        self.logger.info(f"Fetching data from: {url}")

        headers = {'User-Agent': self.user_agent}

        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=self.timeout
                )
                response.raise_for_status()

                # Parse CSV
                df = pd.read_csv(
                    pd.io.common.BytesIO(response.content),
                    sep=';',
                    encoding=self.encoding
                )

                self.logger.info(
                    f"Successfully fetched {len(df)} rows, "
                    f"{len(df.columns)} columns"
                )
                return df

            except requests.RequestException as e:
                self.logger.warning(
                    f"Attempt {attempt}/{self.max_retries} failed: {e}"
                )
                if attempt == self.max_retries:
                    self.logger.error("Max retries reached. Aborting.")
                    raise

        return pd.DataFrame()  # Should not reach here

    def _get_output_path(self, date: Optional[str] = None) -> Path:
        """
        Get output path with date partitioning.

        Args:
            date: Date string (YYYY-MM-DD), defaults to today

        Returns:
            Path object for output file
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        output_dir = self.raw_base_path / "france" / date
        output_dir.mkdir(parents=True, exist_ok=True)

        return output_dir / "covid_hospitalizations.csv"

    @log_execution_time(get_logger("ingestion.france", "logs/ingestion"))
    def save_data(self, df: pd.DataFrame, date: Optional[str] = None):
        """
        Save data to Data Lake raw layer.

        Args:
            df: DataFrame to save
            date: Date string for partitioning
        """
        output_path = self._get_output_path(date)

        self.logger.info(f"Saving data to: {output_path}")

        df.to_csv(output_path, index=False, encoding='utf-8')

        self.logger.info(
            f"Data saved successfully: {output_path} "
            f"({output_path.stat().st_size / 1024:.2f} KB)"
        )

    def validate_data(self, df: pd.DataFrame) -> bool:
        """
        Validate fetched data.

        Args:
            df: DataFrame to validate

        Returns:
            True if valid, False otherwise
        """
        # Check if DataFrame is empty
        if df.empty:
            self.logger.error("DataFrame is empty")
            return False

        # Check required columns
        required_columns = ['dep', 'sexe', 'jour', 'hosp', 'rea', 'dc', 'rad']
        missing_columns = set(required_columns) - set(df.columns)

        if missing_columns:
            self.logger.error(
                f"Missing required columns: {missing_columns}"
            )
            return False

        self.logger.info("Data validation passed")
        return True

    @log_execution_time(get_logger("ingestion.france", "logs/ingestion"))
    def run(self, date: Optional[str] = None) -> bool:
        """
        Run the complete ingestion pipeline.

        Args:
            date: Date string for partitioning

        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("=" * 60)
            self.logger.info("Starting France COVID-19 data ingestion")
            self.logger.info("=" * 60)

            # Fetch data
            df = self.fetch_data()

            # Validate
            if not self.validate_data(df):
                return False

            # Save
            self.save_data(df, date)

            self.logger.info("=" * 60)
            self.logger.info("France COVID-19 ingestion completed successfully")
            self.logger.info("=" * 60)

            return True

        except Exception as e:
            self.logger.error(f"Ingestion failed: {e}", exc_info=True)
            return False


def main():
    """Main entry point for France COVID ingestion."""
    ingestion = FranceCovidIngestion()
    success = ingestion.run()

    if success:
        print("Ingestion successful!")
    else:
        print("Ingestion failed. Check logs for details.")


if __name__ == "__main__":
    main()
