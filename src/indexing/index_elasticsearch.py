"""
COVID-19 Data Indexing - Elasticsearch.
Compatible with Elasticsearch Python client 8.x and 9.x.
"""

import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

from src.utils.logger import get_logger, log_execution_time
from src.utils.config import load_config


class CovidElasticsearchIndexer:
    """Indexer for COVID-19 data into Elasticsearch."""

    INDEX_NAME = "covid-analytics"

    INDEX_MAPPING = {
        "mappings": {
            "properties": {
                "date": {"type": "date"},
                "country": {"type": "keyword"},
                "total_cases": {"type": "integer"},
                "total_hospitalizations": {"type": "integer"},
                "total_icu": {"type": "integer"},
                "total_deaths": {"type": "integer"},
                "total_recovered": {"type": "integer"},
                "active_cases": {"type": "integer"},
                "regions_count": {"type": "integer"},
                "avg_age": {"type": "float"},
                "mortality_rate": {"type": "float"},
                "recovery_rate": {"type": "float"},
                "processed_at": {"type": "date"},
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }

    def __init__(self, config_dir: str = "config/dev"):
        """Initialize Elasticsearch indexer."""
        self.logger = get_logger("indexing.elasticsearch", "logs/indexing")
        self.config = load_config(config_dir)
        self.es = None
        self.data_path = Path("data/usage/covid_comparison/daily_comparison.csv")
        self.logger.info("COVID Elasticsearch Indexer initialized")

    def _connect(self) -> bool:
        """Connect to Elasticsearch."""
        try:
            from elasticsearch import Elasticsearch
        except ImportError:
            self.logger.warning("Elasticsearch package not installed.")
            return False

        try:
            self.logger.info("Connecting to Elasticsearch...")
            self.es = Elasticsearch(
                hosts=["http://localhost:9200"],
                request_timeout=30,
                max_retries=3,
                retry_on_timeout=True
            )
            info = self.es.info()
            self.logger.info(f"Connected to Elasticsearch {info['version']['number']}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect: {e}")
            return False

    def _create_index(self) -> bool:
        """Create or recreate the index."""
        try:
            if self.es.indices.exists(index=self.INDEX_NAME):
                self.logger.info(f"Deleting existing index: {self.INDEX_NAME}")
                self.es.indices.delete(index=self.INDEX_NAME)
            
            self.logger.info(f"Creating index: {self.INDEX_NAME}")
            self.es.indices.create(index=self.INDEX_NAME, body=self.INDEX_MAPPING)
            self.logger.info("Index created successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create index: {e}")
            return False

    def _read_data(self) -> Optional[pd.DataFrame]:
        """Read combined data from CSV."""
        if not self.data_path.exists():
            self.logger.error(f"Data file not found: {self.data_path}")
            return None
        df = pd.read_csv(self.data_path)
        self.logger.info(f"Read {len(df)} rows")
        return df

    def _safe_value(self, value, convert_type=None):
        """Convert value safely, handling NaN."""
        if pd.isna(value):
            return None
        if convert_type == int:
            try:
                return int(float(value))
            except:
                return None
        if convert_type == float:
            try:
                return float(value)
            except:
                return None
        return value

    def _prepare_documents(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Convert DataFrame to Elasticsearch documents."""
        documents = []
        for _, row in df.iterrows():
            doc = {
                "date": row.get("date"),
                "country": row.get("country"),
                "total_cases": self._safe_value(row.get("total_cases"), int),
                "total_hospitalizations": self._safe_value(row.get("total_hospitalizations"), int),
                "total_icu": self._safe_value(row.get("total_icu"), int),
                "total_deaths": self._safe_value(row.get("total_deaths"), int),
                "total_recovered": self._safe_value(row.get("total_recovered"), int),
                "active_cases": self._safe_value(row.get("active_cases"), int),
                "regions_count": self._safe_value(row.get("regions_count"), int),
                "avg_age": self._safe_value(row.get("avg_age"), float),
                "mortality_rate": self._safe_value(row.get("mortality_rate"), float),
                "recovery_rate": self._safe_value(row.get("recovery_rate"), float),
                "processed_at": row.get("processed_at"),
            }
            documents.append(doc)
        self.logger.info(f"Prepared {len(documents)} documents")
        return documents

    def _index_documents(self, documents: List[Dict[str, Any]]) -> bool:
        """Index documents into Elasticsearch."""
        try:
            from elasticsearch.helpers import bulk
            actions = [{"_index": self.INDEX_NAME, "_source": doc} for doc in documents]
            success, errors = bulk(self.es, actions, chunk_size=500, raise_on_error=False)
            self.logger.info(f"Indexed {success} documents")
            return True
        except Exception as e:
            self.logger.error(f"Failed to index: {e}")
            return False

    def _verify_index(self) -> bool:
        """Verify indexed data."""
        try:
            self.es.indices.refresh(index=self.INDEX_NAME)
            count = self.es.count(index=self.INDEX_NAME)["count"]
            self.logger.info(f"Total documents: {count}")
            return True
        except Exception as e:
            self.logger.error(f"Verify failed: {e}")
            return False

    @log_execution_time(get_logger("indexing.elasticsearch", "logs/indexing"))
    def run(self) -> bool:
        """Run the complete indexing pipeline."""
        try:
            self.logger.info("=" * 60)
            self.logger.info("Starting COVID-19 Elasticsearch indexing")
            self.logger.info("=" * 60)

            if not self._connect():
                return False
            if not self._create_index():
                return False
            df = self._read_data()
            if df is None:
                return False
            documents = self._prepare_documents(df)
            if not self._index_documents(documents):
                return False
            self._verify_index()

            self.logger.info("=" * 60)
            self.logger.info("Indexing completed!")
            self.logger.info(" Open Kibana: http://localhost:5601")
            return True
        except Exception as e:
            self.logger.error(f"Indexing failed: {e}")
            return False


def main():
    indexer = CovidElasticsearchIndexer()
    success = indexer.run()
    if success:
        print("Indexing successful!")
        print("\n Next steps:")
        print("  1. Open Kibana: http://localhost:5601")
        print("  2. Go to: Stack Management → Data Views")
        print("  3. Create data view: covid-analytics*")
        print("  4. Go to: Analytics → Discover")
    else:
        print(" Indexing failed.")


if __name__ == "__main__":
    main()
