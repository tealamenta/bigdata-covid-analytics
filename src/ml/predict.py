"""COVID-19 Machine Learning - Predictions with Spark MLlib."""

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

def main():
    print(" Starting COVID-19 ML Pipeline...")
    
    # Create Spark
    spark = SparkSession.builder \
        .appName("COVID-ML") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load data
    df = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("data/usage/covid_comparison/daily_comparison.csv")
    
    print(f" Loaded {df.count()} rows")
    
    # Filter France & prepare
    df_france = df.filter(F.col("country") == "FRANCE")
    df_france = df_france.withColumn("date_parsed", F.to_date(F.col("date")))
    min_date = df_france.agg(F.min("date_parsed")).collect()[0][0]
    df_france = df_france.withColumn("day_number", F.datediff(F.col("date_parsed"), F.lit(min_date)))
    
    df_ml = df_france.filter(F.col("total_hospitalizations").isNotNull()) \
        .select("date", "day_number", "total_hospitalizations").fillna(0)
    
    print(f" Training data: {df_ml.count()} rows")
    
    # Create features
    assembler = VectorAssembler(inputCols=["day_number"], outputCol="features")
    df_features = assembler.transform(df_ml)
    df_features = df_features.withColumn("label", F.col("total_hospitalizations").cast("double"))
    
    # Split
    train, test = df_features.randomSplit([0.8, 0.2], seed=42)
    
    # Train
    print(" Training Linear Regression model...")
    lr = LinearRegression(featuresCol="features", labelCol="label", maxIter=100)
    model = lr.fit(train)
    
    print(f"   Coefficients: {model.coefficients}")
    print(f"   Intercept: {model.intercept:.2f}")
    
    # Evaluate
    predictions = model.transform(test)
    evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
    r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
    
    print(f"\n Model Performance:")
    print(f"   RMSE: {rmse:.2f}")
    print(f"   R² Score: {r2:.4f}")
    
    # Predict future (30 days)
    print(f"\n Future Predictions (next 30 days):")
    max_day = df_ml.agg(F.max("day_number")).collect()[0][0]
    future = spark.createDataFrame([(max_day + i,) for i in range(1, 31)], ["day_number"])
    future = assembler.transform(future)
    future_pred = model.transform(future)
    
    future_pred.select("day_number", F.round("prediction", 0).alias("predicted_hosp")).show(10)
    
    # Save results
    Path("data/ml_results").mkdir(parents=True, exist_ok=True)
    with open("data/ml_results/model_metrics.txt", "w") as f:
        f.write(f"COVID-19 ML Model Results\n")
        f.write(f"RMSE: {rmse:.2f}\n")
        f.write(f"R²: {r2:.4f}\n")
    
    print("\n ML Pipeline completed!")
    print(" Results saved to: data/ml_results/")
    
    spark.stop()

if __name__ == "__main__":
    main()
