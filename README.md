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

# Configurer AWS CLI pour LocalStack
aws configure set aws_access_key_id test
aws configure set aws_secret_access_key test
aws configure set region us-east-1

# Creer le bucket S3
aws --endpoint-url=http://localhost:4566 s3 mb s3://covid-datalake
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

## Elasticsearch

### Creer l'index avec le mapping
```bash
curl -X PUT "http://localhost:9200/covid-analytics" -H "Content-Type: application/json" -d '{
  "mappings": {
    "properties": {
      "date": { "type": "date" },
      "country": { "type": "keyword" },
      "total_cases": { "type": "float" },
      "total_hospitalizations": { "type": "float" },
      "total_icu": { "type": "float" },
      "total_deaths": { "type": "float" },
      "total_recovered": { "type": "float" },
      "active_cases": { "type": "float" },
      "regions_count": { "type": "integer" },
      "avg_age": { "type": "float" },
      "mortality_rate": { "type": "float" },
      "recovery_rate": { "type": "float" }
    }
  }
}'
```

### Indexer les donnees
```bash
python scripts/index_simple.py
```

### Verifier l'indexation
```bash
curl http://localhost:9200/covid-analytics/_count
```

## S3 - Stockage Distribue

### Synchroniser les donnees vers S3
```bash
aws --endpoint-url=http://localhost:4566 s3 sync data/ s3://covid-datalake/data/
```

### Verifier le contenu S3
```bash
aws --endpoint-url=http://localhost:4566 s3 ls s3://covid-datalake/data/
```

## Airflow - Orchestration

### Lancer Airflow en local
```bash
# Demarrer les services Docker (Elasticsearch, Kibana, Airflow)
docker-compose up -d

# Acceder a l'interface Airflow
# URL: http://localhost:8080
# Username: airflow
# Password: airflow
```

### DAG Pipeline

Le DAG `covid_analytics_pipeline` orchestre les etapes suivantes :
```
start
  |
  +---> ingest_france ---> format_france ---+
  |                                         |
  +---> ingest_colombia ---> format_colombia ---+---> combine_data ---> index_elasticsearch ---> end
```

### Execution manuelle du pipeline
```bash
# Sans Airflow (recommande pour les tests)
PYTHONPATH=. python scripts/run_pipeline.py
```

## Kibana - Dashboard

### Acceder a Kibana
```
http://localhost:5601
```

### Creer le Data View

1. Aller dans Management > Data Views
2. Create data view
3. Name: covid-analytics
4. Index pattern: covid-analytics*
5. Timestamp field: date
6. Save

### Visualisations du Dashboard

| Visualisation | Type | Description |
|---------------|------|-------------|
| Hospitalisations par mois | Heat Map | Evolution temporelle France vs Colombie |
| Deces par mois | Heat Map | Comparaison des deces |
| Evolution ICU | Heat Map | Patients en reanimation |
| Guerisons cumulees | Heat Map | Evolution des guerisons |
| Repartition par pays | Pie Chart | 86.54% France / 13.46% Colombie |
| Pic hospitalisations | Metric | Max: 33,466 / Median: 17,531 |
| Total documents | Metric | 1,280 enregistrements |

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

### Voir les resultats
```bash
cat data/ml_results/model_metrics.txt
```

## API REST

Une API Flask est deployee sur Render :

- URL: https://covid-analytics-api.onrender.com

### Endpoints

| Endpoint | Description |
|----------|-------------|
| GET / | Informations du projet |
| GET /stats | Statistiques globales |
| GET /france | Donnees France |
| GET /colombia | Donnees Colombie |

## Resultats Cles

| Metrique | France | Colombie |
|----------|--------|----------|
| Periode | Mars 2020 - Mars 2023 | Mai 2020 - Jan 2022 |
| Jours de donnees | 1,109 | 171 |
| Pic hospitalisations | 33,466 | N/A |
| Pic reanimation | 7,019 | N/A |

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
|-- tests/                  # Tests unitaires
|-- app.py                  # API Flask
|-- docker-compose.yml
|-- requirements.txt
```

## Tests

### Lancer les tests
```bash
python -m unittest discover tests/ -v
```

## Liens

- GitHub: https://github.com/tealamenta/bigdata-covid-analytics
- API: https://covid-analytics-api.onrender.com
- Medium: https://medium.com/@suntzu_80548/building-a-covid-19-data-lake-france-vs-colombia-analysis-b365263bb724

## Auteur

tealamenta - AI/ML Engineer

## Licence

MIT
