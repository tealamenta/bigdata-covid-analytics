# COVID-19 Analytics: France vs Colombia

Analyse comparative Big Data de la pandemie COVID-19 entre la France et la Colombie.

## Architecture
```
Sources API REST --> Ingestion --> Raw Layer (CSV)
                         |
                    Formatting (Spark)
                         |
                  Formatted Layer (Parquet)
                         |
                   Combination (Spark)
                         |
                    Usage Layer
                         |
              Indexation (Elasticsearch)
                         |
                 Visualisation (Kibana)
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
| Machine Learning | Spark MLlib | 3.2.1 |
| Containerisation | Docker | 24.0+ |

## Installation

### Prerequis

- Python 3.9+
- Docker et Docker Compose
- Apache Spark 3.2.1

### Cloner le repo
```bash
git clone https://github.com/tealamenta/bigdata-covid-analytics.git
cd bigdata-covid-analytics
```

### Installer les dependances
```bash
pip install -r requirements.txt
```

## Lancer les Services Docker

### Demarrer tous les services
```bash
docker-compose up -d
```

### Verifier que les services tournent
```bash
docker ps
```

Services actifs :
- covid-elasticsearch (port 9200)
- covid-kibana (port 5601)
- covid-airflow-webserver (port 8081)
- covid-airflow-scheduler
- covid-postgres (port 5432)

### Arreter les services
```bash
docker-compose down
```

## Lancer le Pipeline

### Option 1 : Via Script Python
```bash
PYTHONPATH=. python scripts/run_pipeline.py
```

Resultat attendu :
```
[OK] ingestion_france
[OK] ingestion_colombia
[OK] formatting_france
[OK] formatting_colombia
[OK] combination

SUCCESS - PIPELINE COMPLETED
```

### Option 2 : Via Airflow

1. Demarrer Docker :
```bash
docker-compose up -d
```

2. Ouvrir Airflow UI :
```
http://localhost:8081
Username: admin
Password: admin
```

3. Activer le DAG `covid_analytics_pipeline`

4. Cliquer sur Play pour lancer le DAG

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

## Kibana Dashboard

### Acceder a Kibana
```
http://localhost:5601
```

### Creer le Data View

1. Management > Data Views > Create data view
2. Name: `covid-analytics`
3. Index pattern: `covid-analytics*`
4. Timestamp field: `date`
5. Save

## Airflow - Orchestration

### Acceder a Airflow
```
http://localhost:8081
Username: admin
Password: admin
```

### DAG : covid_analytics_pipeline

- Schedule : `0 6 * * *` (quotidien a 6h00)
- Tasks : 8 (2 EmptyOperators + 6 PythonOperators)

Flux du DAG :
```
start --> [ingest_france, ingest_colombia] --> [format_france, format_colombia] --> combine_data --> index_elasticsearch --> end
```

## S3 - Stockage Distribue

### Lancer LocalStack
```bash
docker run -d --name localstack -p 4566:4566 localstack/localstack
```

### Creer le bucket
```bash
aws --endpoint-url=http://localhost:4566 s3 mb s3://covid-datalake
```

### Synchroniser les donnees
```bash
aws --endpoint-url=http://localhost:4566 s3 sync data/ s3://covid-datalake/data/
```

## Machine Learning

| Metrique | Valeur |
|----------|--------|
| Algorithme | Linear Regression (Spark MLlib) |
| RMSE | 7,545 |
| R-squared | -0.004 |
```bash
cat data/ml_results/model_metrics.txt
```

## API REST

### URL Production
```
https://covid-analytics-api.onrender.com
```

### Endpoints

| Endpoint | Description |
|----------|-------------|
| GET / | Informations du projet |
| GET /stats | Statistiques globales |
| GET /france | Donnees France |
| GET /colombia | Donnees Colombie |

## Tests
```bash
python -m unittest discover tests/ -v
```

## Resultats

| Metrique | France | Colombie |
|----------|--------|----------|
| Periode | Mars 2020 - Mars 2023 | Mai 2020 - Jan 2022 |
| Jours de donnees | 1,109 | 171 |
| Pourcentage | 86.54% | 13.46% |
| Max hospitalisations | 33,466 | N/A |

## Liens

- GitHub : https://github.com/tealamenta/bigdata-covid-analytics
- API : https://covid-analytics-api.onrender.com
- Medium : https://medium.com/@suntzu_80548/building-a-covid-19-data-lake-france-vs-colombia-analysis-b365263bb724

## Auteur

tealamenta

## Licence

MIT
