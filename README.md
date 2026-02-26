# COVID-19 Analytics: France vs Colombia

Architecture Data Lake complete pour analyser et comparer les donnees COVID-19 entre la France et la Colombie.

## Architecture
```
Sources API REST --> Ingestion --> Raw Layer (CSV)
                         |
                    Formatting (Pandas/Parquet)
                         |
                  Formatted Layer (Parquet)
                         |
                   Combination (Pandas)
                         |
                    Usage Layer (CSV)
                         |
              Indexation (Elasticsearch)
                         |
                 Visualisation (Kibana)
```

## Technologies

| Composant | Technologie | Version |
|-----------|-------------|---------|
| Langage | Python | 3.9+ |
| Processing | Pandas / PySpark | 2.0+ / 3.2.1 |
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
- Apache Spark 3.2.1 (pour ML local)

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

### Option 1 : Via Airflow (RECOMMANDE - Production)

1. Demarrer Docker :
```bash
docker-compose up -d
```

2. Attendre 1-2 minutes puis ouvrir Airflow UI :
```
http://localhost:8081
Username: admin
Password: admin
```

3. Activer le DAG `covid_analytics_pipeline` (toggle ON)

4. Cliquer sur Play pour lancer le DAG

5. Toutes les taches passent au VERT :
   - start
   - ingest_france, ingest_colombia (en parallele)
   - format_france, format_colombia (en parallele)
   - combine_data
   - index_elasticsearch
   - end

### Option 2 : Via Script Python (pour tests locaux)
```bash
PYTHONPATH=. python scripts/run_pipeline.py
```

## Airflow - Orchestration

### Acceder a Airflow
```
http://localhost:8081
Username: admin
Password: admin
```

### DAG : covid_analytics_pipeline

- **Schedule** : `0 6 * * *` (quotidien a 6h00)
- **Tasks** : 8 (2 EmptyOperators + 6 PythonOperators)
- **Owner** : tealamenta

### Flux du DAG
```
start
  |
  +--> ingest_france --> format_france --+
  |                                      |
  +--> ingest_colombia --> format_colombia --> combine_data --> index_elasticsearch --> end
```

### Fonctionnalites du DAG

- **Ingestion** : Telecharge les donnees depuis les API REST (France + Colombie)
- **Formatting** : Transforme les CSV en Parquet avec Pandas
- **Combination** : Agregation et jointure des donnees
- **Indexation** : Indexe automatiquement dans Elasticsearch

## Elasticsearch

### Creer l'index manuellement (si besoin)
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
      "regions_count": { "type": "integer" },
      "avg_age": { "type": "float" }
    }
  }
}'
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

### Visualisations disponibles

| Visualisation | Type | Champ |
|---------------|------|-------|
| Hospitalisations par mois | Bar/Heat Map | total_hospitalizations |
| Deces par mois | Bar/Heat Map | total_deaths |
| Repartition par pays | Pie | country |
| Total documents | Metric | count |
| Pic hospitalisations | Metric | max(total_hospitalizations) |

## S3 - Stockage Distribue

### Lancer LocalStack
```bash
docker run -d --name localstack -p 4566:4566 localstack/localstack
```

### Creer le bucket et synchroniser
```bash
aws --endpoint-url=http://localhost:4566 s3 mb s3://covid-datalake
aws --endpoint-url=http://localhost:4566 s3 sync data/ s3://covid-datalake/data/
```

### Verifier
```bash
aws --endpoint-url=http://localhost:4566 s3 ls s3://covid-datalake/data/
```

## Machine Learning
```bash
PYTHONPATH=. python src/ml/train_model.py
cat data/ml_results/model_metrics.txt
```

| Metrique | Valeur |
|----------|--------|
| Algorithme | Linear Regression (Spark MLlib) |
| RMSE | 7,545 |
| R-squared | -0.004 |

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

Resultat : 18 tests OK

## Structure du Projet
```
bigdata-covid-analytics/
├── airflow/
│   └── dags/
│       └── covid_pipeline.py      # DAG Airflow complet
├── data/
│   ├── raw/                       # Donnees brutes CSV
│   ├── formatted/                 # Donnees Parquet
│   ├── usage/                     # Donnees combinees
│   └── ml_results/                # Resultats ML
├── src/
│   ├── ingestion/                 # Scripts d'ingestion
│   ├── formatting/                # Scripts de formatting
│   ├── combination/               # Scripts de combination
│   └── ml/                        # Machine Learning
├── scripts/
│   ├── run_pipeline.py            # Pipeline local
│   └── index_simple.py            # Indexation ES
├── tests/                         # Tests unitaires
├── docker-compose.yml             # Services Docker
├── requirements.txt               # Dependances Python
└── README.md
```

## Resultats

| Metrique | France | Colombie |
|----------|--------|----------|
| Periode | Mars 2020 - Mars 2023 | Mai 2020 - Jan 2022 |
| Jours de donnees | 1,109 | 171 |
| Pourcentage | 86.54% | 13.46% |
| Max hospitalisations | 33,466 | N/A |

## Liens

- **GitHub** : https://github.com/tealamenta/bigdata-covid-analytics
- **API** : https://covid-analytics-api.onrender.com
- **Medium** : https://medium.com/@suntzu_80548/building-a-covid-19-data-lake-france-vs-colombia-analysis-b365263bb724

## Auteur

tealamenta - AI/ML Engineer

## Licence

MIT
