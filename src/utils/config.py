"""
Configuration management module.

Loads and manages YAML configuration files for the project.
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from pydantic import BaseModel, Field


class APIConfig(BaseModel):
    """Configuration for API data sources."""

    name: str
    provider: str
    base_url: str
    resource_id: Optional[str] = None
    format: str = "csv"
    refresh_rate: str = "daily"
    encoding: str = "utf-8"


class DataLakeConfig(BaseModel):
    """Configuration for Data Lake paths."""

    base_path: str
    partition_format: Optional[str] = None
    format: Optional[str] = None
    compression: Optional[str] = None


class Config:
    """Main configuration manager."""

    def __init__(self, config_dir: str = "config/dev"):
        """
        Initialize configuration manager.

        Args:
            config_dir: Directory containing config files
        """
        self.config_dir = Path(config_dir)
        self._configs: Dict[str, Any] = {}

        # Load all configs
        self._load_configs()

    def _load_configs(self):
        """Load all YAML configuration files."""
        if not self.config_dir.exists():
            raise FileNotFoundError(
                f"Config directory not found: {self.config_dir}"
            )

        for config_file in self.config_dir.glob("*.yaml"):
            config_name = config_file.stem
            with open(config_file, 'r', encoding='utf-8') as f:
                self._configs[config_name] = yaml.safe_load(f)

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key.

        Args:
            key: Configuration key (e.g., 'apis.france.base_url')
            default: Default value if key not found

        Returns:
            Configuration value

        Example:
            >>> config = Config()
            >>> url = config.get('apis.data_sources.france.base_url')
        """
        keys = key.split('.')
        value = self._configs

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default

        return value

    def get_api_config(self, country: str, dataset: str = None) -> Dict:
        """
        Get API configuration for a specific country and dataset.

        Args:
            country: Country name (e.g., 'france', 'colombia')
            dataset: Dataset name (e.g., 'hospitalizations', 'cases')

        Returns:
            API configuration dict

        Example:
            >>> config = Config()
            >>> france_api = config.get_api_config('france', 'hospitalizations')
        """
        base_path = f"apis.data_sources.{country}"

        if dataset:
            config = self.get(f"{base_path}.{dataset}")
        else:
            config = self.get(base_path)

        if not config:
            raise ValueError(
                f"API config not found for {country}/{dataset}"
            )

        return config

    def get_spark_config(self) -> Dict:
        """Get Spark configuration."""
        return self.get('spark.spark', {})

    def get_elasticsearch_config(self) -> Dict:
        """Get Elasticsearch configuration."""
        return self.get('elasticsearch.elasticsearch', {})

    def get_data_lake_paths(self) -> Dict:
        """Get Data Lake paths configuration."""
        return self.get('apis.data_lake', {})


def load_config(config_dir: str = "config/dev") -> Config:
    """
    Load configuration from directory.

    Args:
        config_dir: Configuration directory path

    Returns:
        Config instance

    Example:
        >>> config = load_config()
        >>> api_url = config.get('apis.data_sources.france.base_url')
    """
    return Config(config_dir)


if __name__ == "__main__":
    # Test du config loader
    config = load_config()

    print("=== Testing Config Loader ===")
    print(f"France API: {config.get_api_config('france', 'hospitalizations')}")
    print(f"Spark config: {config.get_spark_config()}")
    print(f"Data Lake: {config.get_data_lake_paths()}")
