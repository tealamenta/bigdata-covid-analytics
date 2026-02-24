"""
COVID-19 Data Combination - France vs Colombia.

Combines formatted data from both countries to create
comparative analytics and aggregated metrics.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType

from src.utils.logger import get_logger, log_execution_time
from src.utils.config import load_config


class CovidDataCombiner:
    """
    Combiner class for COVID-19 data.

    Combines France and Colombia data to create comparative analytics.
    """

    def __init__(self, config_dir: str = "config/dev"):
        """
        Initialize COVID data combiner.

        Args:
            config_dir: Configuration directory path
        """
        self.logger = get_logger("combination.covid", "logs/combination")
        self.config = load_config(config_dir)

        # Data Lake paths
        data_lake = self.config.get_data_lake_paths()
        self.formatted_base_path = Path(data_lake['formatted']['base_path'])
        self.usage_base_path = Path(data_lake.get('usage', {}).get('base_path', 'data/usage'))

        # Spark session
        self.spark: Optional[SparkSession] = None

        self.logger.info("COVID Data Combiner initialized")

    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        self.logger.info("Creating Spark session...")

        spark = SparkSession.builder \
            .appName("COVID-Combination") \
            .master("local[*]") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.sql.shuffle.partitions", "10") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        self.logger.info("Spark session created successfully")
        return spark

    def _get_france_path(self) -> Path:
        """Get path to France formatted data."""
        return self.formatted_base_path / "france" / "covid_hospitalizations.parquet"

    def _get_colombia_path(self) -> Path:
        """Get path to Colombia formatted data."""
        return self.formatted_base_path / "colombia" / "covid_cases.parquet"

    def _get_output_path(self) -> Path:
        """Get output path for combined data."""
        output_dir = Path(self.usage_base_path) / "covid_comparison"
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    @log_execution_time(get_logger("combination.covid", "logs/combination"))
    def read_france_data(self) -> DataFrame:
        """Read France formatted data."""
        path = self._get_france_path()
        self.logger.info(f"Reading France data from: {path}")

        df = self.spark.read.parquet(str(path))
        self.logger.info(f"France data: {df.count()} rows")
        return df

    @log_execution_time(get_logger("combination.covid", "logs/combination"))
    def read_colombia_data(self) -> DataFrame:
        """Read Colombia formatted data."""
        path = self._get_colombia_path()
        self.logger.info(f"Reading Colombia data from: {path}")

        df = self.spark.read.parquet(str(path))
        self.logger.info(f"Colombia data: {df.count()} rows")
        return df

    @log_execution_time(get_logger("combination.covid", "logs/combination"))
    def aggregate_france(self, df: DataFrame) -> DataFrame:
        """
        Aggregate France data by date.

        Creates daily summary with total hospitalizations, ICU, deaths.

        Args:
            df: France DataFrame

        Returns:
            Aggregated DataFrame
        """
        self.logger.info("Aggregating France data by date...")

        # Filter to get total (sex = ALL or 0)
        df_total = df.filter(
            (F.col("sex") == "ALL") | (F.col("sex") == "0")
        )

        # Aggregate by date
        df_agg = df_total.groupBy("date", "country").agg(
            F.sum("hospitalizations").alias("total_hospitalizations"),
            F.sum("icu_patients").alias("total_icu"),
            F.sum("deaths").alias("total_deaths"),
            F.sum("returned_home").alias("total_recovered"),
            F.countDistinct("department_code").alias("regions_count")
        )

        self.logger.info(f"France aggregated: {df_agg.count()} daily records")
        return df_agg

    @log_execution_time(get_logger("combination.covid", "logs/combination"))
    def aggregate_colombia(self, df: DataFrame) -> DataFrame:
        """
        Aggregate Colombia data by date.

        Creates daily summary with total cases, deaths, recoveries.

        Args:
            df: Colombia DataFrame

        Returns:
            Aggregated DataFrame
        """
        self.logger.info("Aggregating Colombia data by date...")

        # Aggregate by report_date
        df_agg = df.groupBy(
            F.col("report_date").alias("date"),
            "country"
        ).agg(
            F.count("*").alias("total_cases"),
            F.sum(F.when(F.col("status") == "DECEASED", 1).otherwise(0)).alias("total_deaths"),
            F.sum(F.when(F.col("status") == "RECOVERED", 1).otherwise(0)).alias("total_recovered"),
            F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("active_cases"),
            F.countDistinct("department_name").alias("regions_count"),
            F.avg("age").alias("avg_age")
        )

        # Colombia doesn't have hospitalization data, set to null
        df_agg = df_agg.withColumn("total_hospitalizations", F.lit(None).cast(IntegerType()))
        df_agg = df_agg.withColumn("total_icu", F.lit(None).cast(IntegerType()))

        self.logger.info(f"Colombia aggregated: {df_agg.count()} daily records")
        return df_agg

    @log_execution_time(get_logger("combination.covid", "logs/combination"))
    def combine_data(self, df_france: DataFrame, df_colombia: DataFrame) -> DataFrame:
        """
        Combine France and Colombia aggregated data.

        Args:
            df_france: Aggregated France DataFrame
            df_colombia: Aggregated Colombia DataFrame

        Returns:
            Combined DataFrame
        """
        self.logger.info("Combining France and Colombia data...")

        # Standardize columns for France
        df_france_std = df_france.select(
            "date",
            "country",
            F.lit(None).cast(IntegerType()).alias("total_cases"),
            "total_hospitalizations",
            "total_icu",
            "total_deaths",
            "total_recovered",
            F.lit(None).cast(IntegerType()).alias("active_cases"),
            "regions_count",
            F.lit(None).cast(FloatType()).alias("avg_age")
        )

        # Standardize columns for Colombia
        df_colombia_std = df_colombia.select(
            "date",
            "country",
            "total_cases",
            "total_hospitalizations",
            "total_icu",
            "total_deaths",
            "total_recovered",
            "active_cases",
            "regions_count",
            "avg_age"
        )

        # Union both datasets
        df_combined = df_france_std.union(df_colombia_std)

        # Add calculated metrics
        df_combined = df_combined.withColumn(
            "mortality_rate",
            F.when(
                F.col("total_cases").isNotNull() & (F.col("total_cases") > 0),
                F.round(F.col("total_deaths") / F.col("total_cases") * 100, 2)
            ).otherwise(None)
        )

        df_combined = df_combined.withColumn(
            "recovery_rate",
            F.when(
                F.col("total_cases").isNotNull() & (F.col("total_cases") > 0),
                F.round(F.col("total_recovered") / F.col("total_cases") * 100, 2)
            ).otherwise(None)
        )

        # Add processing timestamp
        df_combined = df_combined.withColumn(
            "processed_at",
            F.current_timestamp()
        )

        # Sort by date and country
        df_combined = df_combined.orderBy("date", "country")

        self.logger.info(f"Combined data: {df_combined.count()} total records")
        return df_combined

    @log_execution_time(get_logger("combination.covid", "logs/combination"))
    def create_summary_stats(self, df: DataFrame) -> DataFrame:
        """
        Create summary statistics by country.

        Args:
            df: Combined DataFrame

        Returns:
            Summary DataFrame
        """
        self.logger.info("Creating summary statistics...")

        df_summary = df.groupBy("country").agg(
            F.min("date").alias("first_date"),
            F.max("date").alias("last_date"),
            F.count("*").alias("total_days"),
            F.sum("total_cases").alias("cumulative_cases"),
            F.sum("total_deaths").alias("cumulative_deaths"),
            F.sum("total_recovered").alias("cumulative_recovered"),
            F.max("total_hospitalizations").alias("peak_hospitalizations"),
            F.max("total_icu").alias("peak_icu"),
            F.avg("mortality_rate").alias("avg_mortality_rate"),
            F.avg("recovery_rate").alias("avg_recovery_rate")
        )

        self.logger.info("Summary statistics created")
        return df_summary

    @log_execution_time(get_logger("combination.covid", "logs/combination"))
    def write_outputs(self, df_combined: DataFrame, df_summary: DataFrame):
        """
        Write combined and summary data to Parquet.

        Args:
            df_combined: Combined daily data
            df_summary: Summary statistics
        """
        output_path = self._get_output_path()

        # Write combined daily data
        combined_path = output_path / "daily_comparison.parquet"
        self.logger.info(f"Writing combined data to: {combined_path}")
        df_combined.write.mode("overwrite").parquet(str(combined_path))

        # Write summary statistics
        summary_path = output_path / "summary_stats.parquet"
        self.logger.info(f"Writing summary stats to: {summary_path}")
        df_summary.write.mode("overwrite").parquet(str(summary_path))

        # Also write as CSV for easy viewing
        csv_path = output_path / "daily_comparison.csv"
        self.logger.info(f"Writing CSV to: {csv_path}")
        df_combined.write.mode("overwrite").option("header", "true").csv(str(csv_path))

        summary_csv_path = output_path / "summary_stats.csv"
        df_summary.toPandas().to_csv(str(summary_csv_path).replace(".csv", "_new.csv"), index=False)

        self.logger.info("All outputs written successfully")

    def show_results(self, df_combined: DataFrame, df_summary: DataFrame):
        """Display sample results."""
        self.logger.info("=" * 60)
        self.logger.info("COMBINED DAILY DATA (Sample)")
        self.logger.info("=" * 60)
        df_combined.show(10, truncate=False)

        self.logger.info("=" * 60)
        self.logger.info("SUMMARY STATISTICS BY COUNTRY")
        self.logger.info("=" * 60)
        df_summary.show(truncate=False)

    @log_execution_time(get_logger("combination.covid", "logs/combination"))
    def run(self) -> bool:
        """
        Run the complete combination pipeline.

        Returns:
            True if successful, False otherwise
        """
        try:
            self.logger.info("=" * 60)
            self.logger.info("Starting COVID-19 data combination")
            self.logger.info("=" * 60)

            # Initialize Spark
            self.spark = self._create_spark_session()

            # Check inputs exist
            france_path = self._get_france_path()
            colombia_path = self._get_colombia_path()

            if not france_path.exists():
                self.logger.error(f"France data not found: {france_path}")
                self.logger.error("Run format_france.py first!")
                return False

            if not colombia_path.exists():
                self.logger.error(f"Colombia data not found: {colombia_path}")
                self.logger.error("Run format_colombia.py first!")
                return False

            # Read formatted data
            df_france = self.read_france_data()
            df_colombia = self.read_colombia_data()

            # Aggregate each country
            df_france_agg = self.aggregate_france(df_france)
            df_colombia_agg = self.aggregate_colombia(df_colombia)

            # Combine
            df_combined = self.combine_data(df_france_agg, df_colombia_agg)

            # Create summary
            df_summary = self.create_summary_stats(df_combined)

            # Show results
            self.show_results(df_combined, df_summary)

            # Write outputs
            self.write_outputs(df_combined, df_summary)

            self.logger.info("=" * 60)
            self.logger.info("COVID-19 data combination completed successfully!")
            self.logger.info("=" * 60)

            return True

        except Exception as e:
            self.logger.error(f"Combination failed: {e}", exc_info=True)
            return False

        finally:
            if self.spark:
                self.spark.stop()
                self.logger.info("Spark session stopped")


def main():
    """Main entry point for COVID data combination."""
    combiner = CovidDataCombiner()
    success = combiner.run()

    if success:
        print(" Combination successful!")
        print("\n Output files:")
        print("  - data/usage/covid_comparison/daily_comparison.parquet")
        print("  - data/usage/covid_comparison/daily_comparison.csv")
        print("  - data/usage/covid_comparison/summary_stats.parquet")
        print("  - data/usage/covid_comparison/summary_stats.csv")
    else:
        print(" Combination failed. Check logs for details.")


if __name__ == "__main__":
    main()
