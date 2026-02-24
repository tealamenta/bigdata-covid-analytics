# COVID-19 Analytics: France vs Colombia

Analyse comparative Big Data de la pandemie COVID-19 entre la France et la Colombie.

## Description

Ce projet implemente une architecture Data Lake complete pour analyser et comparer les donnees COVID-19 entre la France et la Colombie. Il utilise des technologies Big Data modernes : Apache Spark, Elasticsearch, Kibana, Airflow et S3 (LocalStack).

## Architecture
```
Sources de donnees (API REST)
        |
        v
   INGESTION (Python)
        |
        v
   RAW LAYER (CSV) --> S3
        |
        v
   FORMATTING (Apache Spark)
        |
        v
   FORMATTED LAYER (Parquet) --> S3
        |
        v
   COMBINATION (Spark)
        |
        v
   USAGE LAYER (Parquet/CSV) --> S3
        |
        v
   INDEXATION (Elasticsearch)
        |
        v
   VISUALISATION (Kibana)
```

## Technologies

| Composant | Technologie | Version |
|-----------|-------------|---------|
| Langage | Python | 3.9+ |
| Processing | Apache Spark | 3.2.1 |
| Orchestration | Apache Airflow | 2.7.3 |
| Indexation | Elasticsearch | 8.11.0 |
| Visualisation | Kibana | 8.11.0 |
| Storage | S3 (LocalStack) | - |
| Containerisation | Docker | 24.0+ |
| Machine Learning | Spark MLlib | 3.2.1 |

## Sources de Donnees

### France (data.gouv.fr)
- URL: https://www.data.gouv.fr/fr/datasets/r/63352e38-d353-4b54-bfd1-f1b3ee1cabd7
- Donnees: Hospitalisations, reanimation, deces, guerisons par departement
- Periode: Mars 2020 - Mars 2023
- Enregistrements: 338,245

### Colombie (datos.gov.co)
- URL: https://www.datos.gov.co/resource/gt2j-8ykr.csv
- Donnees: Cas individuels avec statut (recupere, decede, actif)
- Periode: Mai 2020 - Janvier 2022
- Enregistrements: 100,000

## Structure du Data Lake
```
data/
|-- raw/                              [Layer 1: Donnees brutes]
|   |-- france/
|   |   |-- 2026-02-24/
|   |       |-- covid_hospitalizations.csv
|   |-- colombia/
|       |-- 2026-02-24/
|           |-- covid_cases.csv
|-- formatted/                        [Layer 2: Donnees formatees]
|   |-- france/
|   |   |-- covid_hospitalizations.parquet/
|   |-- colombia/
|       |-- covid_cases.parquet/
|-- usage/                            [Layer 3: Donnees combinees]
|   |-- covid_comparison/
|       |-- daily_comparison.parquet/
|       |-- summary_stats.parquet/
|-- ml_results/                       [Machine Learning]
    |-- model_metrics.txt
    |-- future_predictions.csv
```

## Installation

### Prerequis
- Python 3.9+
- Docker et Docker Compose
- Apache Spark 3.2.1

### Installation
```bash
# Cloner le repo
git clone https://github.com/tealamenta/bigdata-covid-analytics.git
cd bigdata-covid-analytics

# Installer les dependances
pip install -r requirements.txt

# Lancer les services Docker
docker-compose up -d

# Lancer LocalStack (S3)
docker run -d --name localstack -p 4566:4566 localstack/localstack
```

## Utilisation

### Executer le pipeline complet
```bash
PYTHONPATH=. python scripts/run_pipeline.py
```

### Resultat attendu
```
[OK] ingestion_france
[OK] ingestion_colombia
[OK] formatting_france
[OK] formatting_colombia
[OK] combination

SUCCESS - PIPELINE COMPLETED
```

### Indexer dans Elasticsearch
```bash
python scripts/index_simple.py
```

### Acceder au Dashboard Kibana
```
http://localhost:5601
```

### Synchroniser avec S3
```bash
aws --endpoint-url=http://localhost:4566 s3 sync data/ s3://covid-datalake/data/
```

## Dashboard Kibana

Le dashboard comprend 10 visualisations :

| Visualisation | Type | Description |
|---------------|------|-------------|
| Hospitalisations par mois | Heat Map | Evolution temporelle France vs Colombie |
| Deces par mois | Heat Map | Comparaison des deces |
| Evolution ICU | Heat Map | Patients en reanimation |
| Guerisons cumulees | Heat Map | Evolution des guerisons |
| Repartition par pays | Pie Chart | 86.54% France / 13.46% Colombie |
| Pic hospitalisations | Metric | Max: 33,466 / Median: 17,531 |
| Total documents | Metric | 1,270 enregistrements |
| Statistiques deces | Table | Par pays |
| Statistiques hospitalisations | Table | Par pays |
| Statistiques guerisons | Table | Par pays |

## Machine Learning

### Modele
- Algorithme: Linear Regression (Spark MLlib)
- Feature: day_number (jours depuis le debut)
- Target: total_hospitalizations
- Split: 80% train / 20% test

### Resultats

| Metrique | Valeur | Interpretation |
|----------|--------|----------------|
| RMSE | 7,545 | Erreur moyenne |
| R-squared | -0.004 | Donnees non-lineaires |
| Prediction J+30 | ~18,000 | Hospitalisations stables |

Note: Le R-squared negatif est attendu car les donnees COVID suivent des vagues non-lineaires.

## API REST

Une API Flask est disponible pour acceder aux statistiques :
```bash
python app.py
```

Endpoints :
- GET / : Informations du projet
- GET /stats : Statistiques globales
- GET /france : Donnees France
- GET /colombia : Donnees Colombie

## Resultats Cles

| Metrique | France | Colombie |
|----------|--------|----------|
| Periode | Mars 2020 - Mars 2023 | Mai 2020 - Jan 2022 |
| Jours de donnees | 1,109 | 171 |
| Pic hospitalisations | 33,466 | N/A |
| Pic reanimation | 7,019 | N/A |
| Mediane deces | 89,751 | 8 |

## Structure du Projet
```
bigdata-covid-analytics/
|-- src/
|   |-- ingestion/          # Scripts d'ingestion
|   |-- formatting/         # Transformation Spark
|   |-- combination/        # Combinaison des donnees
|   |-- ml/                 # Machine Learning
|   |-- utils/              # Logger, config
|-- scripts/
|   |-- run_pipeline.py     # Pipeline complet
|   |-- index_simple.py     # Indexation ES
|-- airflow/
|   |-- dags/               # DAG Airflow
|-- data/                   # Data Lake
|-- docker/                 # Dockerfiles
|-- tests/                  # Tests unitaires
|-- app.py                  # API Flask
|-- docker-compose.yml
|-- requirements.txt
```

## Auteur

Mohamed H - DATA705 Big Data - Fevrier 2026

## Licence

Ce projet est realise dans le cadre du module DATA705 Big Data.
