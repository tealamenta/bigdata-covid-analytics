"""
COVID-19 Data Formatting - Colombia.

Transforms raw CSV data into formatted Parquet using Apache Spark.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src.utils.logger import get_logger, log_execution_time
from src.utils.config import load_config


class ColombiaCovidFormatter:
    """Formatter class for Colombia COVID-19 data."""

    COLUMN_MAPPING = {
        "fecha_reporte_web": "report_date",
        "id_de_caso": "case_id",
        "fecha_de_notificaci_n": "notification_date",
        "departamento": "department_code",
        "departamento_nom": "department_name",
        "ciudad_municipio": "city_code",
        "ciudad_municipio_nom": "city_name",
        "edad": "age",
        "sexo": "sex",
        "tipo_recuperacion": "recovery_type",
        "estado": "status",
        "fecha_muerte": "death_date",
        "fecha_recuperado": "recovery_date",
    }

    def __init__(self, config_dir: str = "config/dev"):
        """Initialize Colombia COVID formatter."""
        self.logger = get_logger("formatting.colombia", "logs/formatting")
        self.config = load_config(config_dir)

        data_lake = self.config.get_data_lake_paths()
        self.raw_base_path = Path(data_lake['raw']['base_path'])
        self.formatted_base_path = Path(data_lake['formatted']['base_path'])
        self.spark: Optional[SparkSession] = None

        self.logger.info("Colombia COVID Formatter initialized")

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with legacy date parsing."""
        self.logger.info("Creating Spark session...")

        spark = SparkSession.builder \
            .appName("COVID-Colombia-Formatting") \
            .master("local[*]") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.sql.shuffle.partitions", "10") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        self.logger.info("Spark session created successfully")
        return spark

    def _get_input_path(self, date: Optional[str] = None) -> Path:
        """Get input path for raw data."""
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")
        return self.raw_base_path / "colombia" / date / "covid_cases.csv"

    def _get_output_path(self) -> Path:
        """Get output path for formatted data."""
        output_dir = self.formatted_base_path / "colombia"
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir / "covid_cases.parquet"

    @log_execution_time(get_logger("formatting.colombia", "logs/formatting"))
    def read_raw_data(self, input_path: Path) -> DataFrame:
        """Read raw CSV data."""
        self.logger.info(f"Reading raw data from: {input_path}")

        df = self.spark.read \
            .option("header", "true") \
            .option("sep", ",") \
            .option("encoding", "UTF-8") \
            .csv(str(input_path))

        row_count = df.count()
        self.logger.info(f"Read {row_count} rows, {len(df.columns)} columns")
        self.logger.info(f"Columns: {df.columns}")

        return df

    @log_execution_time(get_logger("formatting.colombia", "logs/formatting"))
    def transform_data(self, df: DataFrame) -> DataFrame:
        """Apply transformations to raw data."""
        self.logger.info("Applying transformations...")
        self.logger.info(f"Input columns: {df.columns}")

        # 1. Rename columns
        for old_name, new_name in self.COLUMN_MAPPING.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
                self.logger.info(f"Renamed: {old_name} -> {new_name}")

        self.logger.info(f"Columns after renaming: {df.columns}")

        # 2. Convert date columns using split to handle various formats
        # Formats: "2021-01-05 00:00:00" or "2021-01-05T00:00:00.000"
        date_columns = ["report_date", "notification_date", "death_date", "recovery_date"]
        
        for col_name in date_columns:
            if col_name in df.columns:
                # Split by space or T, take first part, then parse as date
                df = df.withColumn(
                    col_name,
                    F.to_date(
                        F.split(F.col(col_name), "[T ]").getItem(0),
                        "yyyy-MM-dd"
                    )
                )
                self.logger.info(f"Converted {col_name} to DateType")

        # 3. Convert age to Integer
        if "age" in df.columns:
            df = df.withColumn("age", F.col("age").cast(IntegerType()))
            self.logger.info("Converted age to Integer")

        # 4. Normalize sex values (uppercase)
        if "sex" in df.columns:
            df = df.withColumn("sex", F.upper(F.trim(F.col("sex"))))
            self.logger.info("Normalized sex values")

        # 5. Normalize status values (Spanish -> English)
        if "status" in df.columns:
            df = df.withColumn("status",
                F.when(F.col("status") == "Recuperado", "RECOVERED")
                .when(F.col("status") == "Fallecido", "DECEASED")
                .when(F.col("status") == "Activo", "ACTIVE")
                .when(F.col("status") == "N/A", "UNKNOWN")
                .otherwise(F.col("status")))
            self.logger.info("Normalized status values")

        # 6. Add metadata
        df = df.withColumn("country", F.lit("COLOMBIA"))
        df = df.withColumn("ingestion_date", F.current_date())
        self.logger.info("Added metadata columns")

        # 7. Filter rows with null report_date
        if "report_date" in df.columns:
            initial = df.count()
            df = df.filter(F.col("report_date").isNotNull())
            final = df.count()
            self.logger.info(f"Filtered null dates: {initial} -> {final} rows")

        return df

    def validate_data(self, df: DataFrame) -> bool:
        """Validate transformed data."""
        count = df.count()
        if count == 0:
            self.logger.error("DataFrame is empty")
            return False

        self.logger.info(f"Validation passed. {count} rows")
        return True

    @log_execution_time(get_logger("formatting.colombia", "logs/formatting"))
    def write_parquet(self, df: DataFrame, output_path: Path):
        """Write to Parquet."""
        self.logger.info(f"Writing Parquet to: {output_path}")
        df.write.mode("overwrite").parquet(str(output_path))
        self.logger.info("Parquet written successfully")

    def show_sample(self, df: DataFrame, n: int = 5):
        """Display sample rows."""
        self.logger.info(f"Sample of {n} rows:")
        
        display_cols = ["report_date", "department_name", "city_name",
                       "age", "sex", "status", "country"]
        existing_cols = [c for c in display_cols if c in df.columns]
        
        df.select(existing_cols).show(n, truncate=False)
        df.printSchema()

    @log_execution_time(get_logger("formatting.colombia", "logs/formatting"))
    def run(self, input_date: Optional[str] = None) -> bool:
        """Run the complete formatting pipeline."""
        try:
            self.logger.info("=" * 60)
            self.logger.info("Starting Colombia COVID-19 data formatting")
            self.logger.info("=" * 60)

            self.spark = self._create_spark_session()

            input_path = self._get_input_path(input_date)
            output_path = self._get_output_path()

            if not input_path.exists():
                self.logger.error(f"Input not found: {input_path}")
                return False

            df_raw = self.read_raw_data(input_path)
            df_formatted = self.transform_data(df_raw)

            if not self.validate_data(df_formatted):
                return False

            self.show_sample(df_formatted)
            self.write_parquet(df_formatted, output_path)

            self.logger.info("=" * 60)
            self.logger.info("Colombia formatting completed successfully!")
            self.logger.info("=" * 60)

            return True

        except Exception as e:
            self.logger.error(f"Formatting failed: {e}", exc_info=True)
            return False

        finally:
            if self.spark:
                self.spark.stop()
                self.logger.info("Spark session stopped")


def main():
    """Main entry point."""
    formatter = ColombiaCovidFormatter()
    success = formatter.run()

    if success:
        print("Formatting successful!")
    else:
        print("Formatting failed. Check logs for details.")


if __name__ == "__main__":
    main()
