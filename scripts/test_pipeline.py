"""Test pipeline simple."""
import sys
sys.path.insert(0, '.')

print("=== TEST PIPELINE ===")

# 1. Ingestion
print("\n[1/5] Ingestion France...")
from src.ingestion.france_covid import FranceCovidIngestion
FranceCovidIngestion().run()
print("OK")

print("\n[2/5] Ingestion Colombia...")
from src.ingestion.colombia_covid import ColombiaCovidIngestion
ColombiaCovidIngestion().run()
print("OK")

# 2. Formatting
print("\n[3/5] Formatting France...")
from src.formatting.format_france import FranceCovidFormatter
FranceCovidFormatter().run()
print("OK")

print("\n[4/5] Formatting Colombia...")
from src.formatting.format_colombia import ColombiaCovidFormatter
ColombiaCovidFormatter().run()
print("OK")

# 3. Combination (sans CSV export)
print("\n[5/5] Combination...")
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Test").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df_france = spark.read.parquet("data/formatted/france/covid_hospitalizations.parquet")
df_colombia = spark.read.parquet("data/formatted/colombia/covid_cases.parquet")

print(f"France: {df_france.count()} rows")
print(f"Colombia: {df_colombia.count()} rows")

# Ecrire parquet seulement (pas de CSV)
from pyspark.sql import functions as F
df_france_agg = df_france.filter(F.col("sex") == "ALL").groupBy("date").agg(
    F.sum("hospitalizations").alias("total_hospitalizations"),
    F.sum("icu_patients").alias("total_icu"),
    F.sum("deaths").alias("total_deaths")
).withColumn("country", F.lit("FRANCE"))

df_france_agg.write.mode("overwrite").parquet("data/usage/covid_comparison/daily_france.parquet")
print("Parquet written")

spark.stop()
print("\n=== SUCCESS ===")
