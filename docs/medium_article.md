# Building a COVID-19 Data Lake: France vs Colombia Analysis

## Introduction

In this project, I built a complete Big Data architecture to analyze and compare COVID-19 data between France and Colombia. The goal was to implement a full data pipeline from ingestion to visualization using modern technologies.

## Architecture Overview

The project follows a Data Lake architecture with three main layers:

- **Raw Layer**: Raw data from REST APIs (CSV format)
- **Formatted Layer**: Transformed data using Apache Spark (Parquet format)
- **Usage Layer**: Combined and aggregated data for analysis

## Technologies Used

- **Python 3.9+**: Main programming language
- **Apache Spark 3.2.1**: Data processing and transformation
- **Apache Airflow 2.7.3**: Pipeline orchestration
- **Elasticsearch 8.11.0**: Data indexing
- **Kibana 8.11.0**: Data visualization
- **LocalStack (S3)**: Distributed file storage
- **Docker**: Containerization

## Data Sources

### France (data.gouv.fr)
- Daily hospitalizations by department
- Fields: hospitalizations, ICU, deaths, recoveries
- Period: March 2020 - March 2023
- Records: 338,245

### Colombia (datos.gov.co)
- Individual COVID cases with status
- Fields: date, department, age, sex, status
- Period: May 2020 - January 2022
- Records: 100,000

## Pipeline Implementation

### 1. Ingestion
Python scripts fetch data from REST APIs and store them in the Raw layer partitioned by date.

### 2. Formatting (Spark)
Apache Spark transforms CSV files to optimized Parquet format with:
- Data type conversion
- Column normalization
- Value mapping
- Snappy compression

### 3. Combination
Spark joins and aggregates data from both countries into a unified dataset for comparison.

### 4. Indexation
Combined data is indexed into Elasticsearch for fast querying and visualization.

### 5. Orchestration
Apache Airflow DAG orchestrates all pipeline steps with dependency management.

## Dashboard

The Kibana dashboard includes 10 visualizations:
- Hospitalizations over time (Heat Map)
- Deaths comparison (Heat Map)
- ICU evolution (Heat Map)
- Recoveries (Heat Map)
- Data distribution by country (Pie Chart)
- Key metrics (Metric widgets)
- Statistics tables

## Machine Learning

A Linear Regression model using Spark MLlib predicts future hospitalizations:
- **RMSE**: 7,545
- **R-squared**: -0.004 (expected for non-linear COVID waves)
- **Prediction**: ~18,000 hospitalizations stable

## Key Results

| Metric | France | Colombia |
|--------|--------|----------|
| Data Period | Mar 2020 - Mar 2023 | May 2020 - Jan 2022 |
| Days of Data | 1,109 | 171 |
| Peak Hospitalizations | 33,466 | N/A |
| Peak ICU | 7,019 | N/A |

## Challenges

The main challenge was the heterogeneity of data sources. France provides detailed hospital indicators while Colombia provides individual case data. This required careful schema normalization during the combination phase.

## Deployment

- **API**: Deployed on Render (https://covid-analytics-api.onrender.com)
- **S3**: LocalStack for distributed storage
- **Dashboard**: Kibana running locally with Docker

## Conclusion

This project demonstrates a complete Big Data architecture from ingestion to visualization. The use of modern technologies (Spark, Airflow, Elasticsearch, Kibana) enables efficient processing of large datasets and meaningful analysis.

## Links

- **GitHub**: https://github.com/tealamenta/bigdata-covid-analytics
- **API**: https://covid-analytics-api.onrender.com

---

*Author: tealamenta - AI/ML Engineer*
