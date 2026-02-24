"""
COVID-19 Data Formatting - France.

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


class FranceCovidFormatter:
    """Formatter class for France COVID-19 data."""

    COLUMN_MAPPING = {
        "dep": "department_code",
        "sexe": "sex",
        "jour": "date",
        "hosp": "hospitalizations",
        "rea": "icu_patients",
        "HospConv": "conventional_hosp",
        "SSR_USLD": "ssr_usld",
        "autres": "other",
        "rad": "returned_home",
        "dc": "deaths",
    }

    def __init__(self, config_dir: str = "config/dev"):
        """Initialize France COVID formatter."""
        self.logger = get_logger("formatting.france", "logs/formatting")
        self.config = load_config(config_dir)

        data_lake = self.config.get_data_lake_paths()
        self.raw_base_path = Path(data_lake['raw']['base_path'])
        self.formatted_base_path = Path(data_lake['formatted']['base_path'])
        self.spark: Optional[SparkSession] = None

        self.logger.info("France COVID Formatter initialized")

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session."""
        self.logger.info("Creating Spark session...")

        spark = SparkSession.builder \
            .appName("COVID-France-Formatting") \
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
        return self.raw_base_path / "france" / date / "covid_hospitalizations.csv"

    def _get_output_path(self) -> Path:
        """Get output path for formatted data."""
        output_dir = self.formatted_base_path / "france"
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir / "covid_hospitalizations.parquet"

    def _detect_separator(self, input_path: Path) -> str:
        """
        Detect CSV separator by reading first line.
        
        Args:
            input_path: Path to CSV file
            
        Returns:
            Detected separator (';' or ',')
        """
        with open(input_path, 'r', encoding='utf-8') as f:
            first_line = f.readline()
        
        # Count occurrences of potential separators
        semicolon_count = first_line.count(';')
        comma_count = first_line.count(',')
        tab_count = first_line.count('\t')
        
        self.logger.info(f"Separator detection: ; = {semicolon_count}, , = {comma_count}, tab = {tab_count}")
        
        if semicolon_count > comma_count and semicolon_count > tab_count:
            return ';'
        elif tab_count > comma_count:
            return '\t'
        else:
            return ','

    @log_execution_time(get_logger("formatting.france", "logs/formatting"))
    def read_raw_data(self, input_path: Path) -> DataFrame:
        """Read raw CSV data with auto-detected separator."""
        self.logger.info(f"Reading raw data from: {input_path}")

        # Detect separator
        separator = self._detect_separator(input_path)
        self.logger.info(f"Using separator: '{separator}'")

        df = self.spark.read \
            .option("header", "true") \
            .option("sep", separator) \
            .option("encoding", "UTF-8") \
            .option("inferSchema", "false") \
            .csv(str(input_path))

        row_count = df.count()
        col_count = len(df.columns)
        self.logger.info(f"Read {row_count} rows, {col_count} columns")
        self.logger.info(f"Columns: {df.columns}")

        # Verify we got multiple columns
        if col_count == 1:
            self.logger.warning("Only 1 column detected! Trying alternative separators...")
            # Try other separators
            for alt_sep in [',', ';', '\t']:
                if alt_sep != separator:
                    df_alt = self.spark.read \
                        .option("header", "true") \
                        .option("sep", alt_sep) \
                        .option("encoding", "UTF-8") \
                        .csv(str(input_path))
                    if len(df_alt.columns) > 1:
                        self.logger.info(f"Found working separator: '{alt_sep}'")
                        return df_alt
        
        return df

    @log_execution_time(get_logger("formatting.france", "logs/formatting"))
    def transform_data(self, df: DataFrame) -> DataFrame:
        """Apply transformations to raw data."""
        self.logger.info("Applying transformations...")
        self.logger.info(f"Input columns: {df.columns}")

        # 1. Rename columns that exist
        renamed_count = 0
        for old_name, new_name in self.COLUMN_MAPPING.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
                renamed_count += 1
                self.logger.info(f"Renamed: {old_name} -> {new_name}")

        self.logger.info(f"Renamed {renamed_count} columns")
        self.logger.info(f"Columns after renaming: {df.columns}")

        # 2. Convert date column
        if "date" in df.columns:
            df = df.withColumn("date", F.to_date(F.col("date"), "yyyy-MM-dd"))
            self.logger.info("Converted 'date' to DateType")

        # 3. Convert numeric columns
        numeric_cols = ["hospitalizations", "icu_patients", "conventional_hosp",
                       "ssr_usld", "other", "returned_home", "deaths"]
        for col_name in numeric_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))

        self.logger.info("Converted numeric columns")

        # 4. Map sex values
        if "sex" in df.columns:
            df = df.withColumn("sex",
                F.when(F.col("sex") == "0", "ALL")
                .when(F.col("sex") == "1", "M")
                .when(F.col("sex") == "2", "F")
                .otherwise(F.col("sex")))
            self.logger.info("Mapped sex values")

        # 5. Add metadata
        df = df.withColumn("country", F.lit("FRANCE"))
        df = df.withColumn("ingestion_date", F.current_date())
        self.logger.info("Added metadata columns")

        # 6. Filter nulls (only if columns exist)
        current_cols = df.columns
        filter_cols = ["hospitalizations", "icu_patients", "deaths"]
        existing_filter_cols = [c for c in filter_cols if c in current_cols]
        
        if existing_filter_cols:
            conditions = [F.col(c).isNotNull() for c in existing_filter_cols]
            combined = conditions[0]
            for c in conditions[1:]:
                combined = combined | c
            
            initial = df.count()
            df = df.filter(combined)
            final = df.count()
            self.logger.info(f"Filtered: {initial} -> {final} rows")
        else:
            self.logger.warning(f"No filter columns found. Available: {current_cols}")

        return df

    def validate_data(self, df: DataFrame) -> bool:
        """Validate transformed data."""
        count = df.count()
        if count == 0:
            self.logger.error("DataFrame is empty")
            return False

        # Check for at least some expected columns
        expected = ["department_code", "date", "country"]
        found = [c for c in expected if c in df.columns]
        
        if len(found) < 2:
            self.logger.warning(f"Missing expected columns. Found: {found}, Available: {df.columns}")

        self.logger.info(f"Validation passed. {count} rows")
        return True

    @log_execution_time(get_logger("formatting.france", "logs/formatting"))
    def write_parquet(self, df: DataFrame, output_path: Path):
        """Write to Parquet."""
        self.logger.info(f"Writing Parquet to: {output_path}")
        df.write.mode("overwrite").parquet(str(output_path))
        self.logger.info("Parquet written successfully")

    def show_sample(self, df: DataFrame, n: int = 5):
        """Display sample rows."""
        self.logger.info(f"Sample of {n} rows:")
        df.show(n, truncate=False)
        df.printSchema()

    @log_execution_time(get_logger("formatting.france", "logs/formatting"))
    def run(self, input_date: Optional[str] = None) -> bool:
        """Run the complete formatting pipeline."""
        try:
            self.logger.info("=" * 60)
            self.logger.info("Starting France COVID-19 data formatting")
            self.logger.info("=" * 60)

            self.spark = self._create_spark_session()

            input_path = self._get_input_path(input_date)
            output_path = self._get_output_path()

            if not input_path.exists():
                self.logger.error(f"Input not found: {input_path}")
                return False

            df_raw = self.read_raw_data(input_path)
            
            # Debug: show raw data structure
            self.logger.info(f"Raw data columns: {df_raw.columns}")
            self.logger.info("Raw data sample:")
            df_raw.show(3, truncate=False)
            
            df_formatted = self.transform_data(df_raw)

            if not self.validate_data(df_formatted):
                return False

            self.show_sample(df_formatted)
            self.write_parquet(df_formatted, output_path)

            self.logger.info("=" * 60)
            self.logger.info("France formatting completed successfully!")
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
    formatter = FranceCovidFormatter()
    success = formatter.run()

    if success:
        print("Formatting successful!")
    else:
        print("Formatting failed. Check logs for details.")


if __name__ == "__main__":
    main()
