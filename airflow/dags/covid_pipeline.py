"""
COVID-19 Analytics Pipeline DAG
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import os
import sys

sys.path.insert(0, '/opt/airflow')

default_args = {
    'owner': 'tealamenta',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'covid_analytics_pipeline',
    default_args=default_args,
    description='Pipeline Big Data COVID-19 France vs Colombia',
    schedule_interval='0 6 * * *',
    catchup=False,
)

start = EmptyOperator(task_id='start', dag=dag)

def ingest_france():
    import requests
    from datetime import datetime
    
    url = "https://www.data.gouv.fr/fr/datasets/r/63352e38-d353-4b54-bfd1-f1b3ee1cabd7"
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_dir = f"/opt/airflow/data/raw/france/{date_str}"
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Downloading France data from {url}")
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    
    output_file = f"{output_dir}/covid_hospitalizations.csv"
    with open(output_file, 'wb') as f:
        f.write(response.content)
    
    print(f"France data saved to {output_file}")
    return True

def ingest_colombia():
    import requests
    from datetime import datetime
    
    url = "https://www.datos.gov.co/resource/gt2j-8ykr.csv?$limit=100000"
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_dir = f"/opt/airflow/data/raw/colombia/{date_str}"
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Downloading Colombia data from {url}")
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    
    output_file = f"{output_dir}/covid_cases.csv"
    with open(output_file, 'wb') as f:
        f.write(response.content)
    
    print(f"Colombia data saved to {output_file}")
    return True

def format_france():
    import pandas as pd
    from datetime import datetime
    import glob
    import shutil
    
    print("Starting France formatting...")
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    input_path = f"/opt/airflow/data/raw/france/{date_str}/covid_hospitalizations.csv"
    
    if not os.path.exists(input_path):
        pattern = "/opt/airflow/data/raw/france/*/covid_hospitalizations.csv"
        files = glob.glob(pattern)
        if files:
            input_path = sorted(files)[-1]
        else:
            raise FileNotFoundError("No France CSV file found")
    
    print(f"Reading: {input_path}")
    df = pd.read_csv(input_path, sep=';')
    print(f"Loaded {len(df)} records")
    
    df_formatted = df.rename(columns={
        'dep': 'department_code',
        'sexe': 'sex',
        'jour': 'date',
        'hosp': 'hospitalizations',
        'rea': 'icu_patients',
        'rad': 'returned_home',
        'dc': 'deaths'
    })
    
    df_formatted['country'] = 'FRANCE'
    df_formatted['date'] = pd.to_datetime(df_formatted['date'])
    
    output_dir = "/opt/airflow/data/formatted/france"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/france_hospitalizations.parquet"
    if os.path.isdir(output_path):
        shutil.rmtree(output_path)
    
    df_formatted.to_parquet(output_path, index=False)
    print(f"Saved {len(df_formatted)} records to {output_path}")
    return True

def format_colombia():
    import pandas as pd
    from datetime import datetime
    import glob
    import shutil
    
    print("Starting Colombia formatting...")
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    input_path = f"/opt/airflow/data/raw/colombia/{date_str}/covid_cases.csv"
    
    if not os.path.exists(input_path):
        pattern = "/opt/airflow/data/raw/colombia/*/covid_cases.csv"
        files = glob.glob(pattern)
        if files:
            input_path = sorted(files)[-1]
        else:
            raise FileNotFoundError("No Colombia CSV file found")
    
    print(f"Reading: {input_path}")
    df = pd.read_csv(input_path)
    print(f"Loaded {len(df)} records")
    
    df['country'] = 'COLOMBIA'
    
    output_dir = "/opt/airflow/data/formatted/colombia"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/colombia_cases.parquet"
    if os.path.isdir(output_path):
        shutil.rmtree(output_path)
    
    df.to_parquet(output_path, index=False)
    print(f"Saved {len(df)} records to {output_path}")
    return True

def combine_data():
    import pandas as pd
    
    print("Starting data combination...")
    
    # France
    france_path = "/opt/airflow/data/formatted/france/france_hospitalizations.parquet"
    france_df = pd.read_parquet(france_path)
    print(f"Loaded France: {len(france_df)} records")
    
    france_daily = france_df.groupby('date').agg({
        'hospitalizations': 'sum',
        'icu_patients': 'sum',
        'deaths': 'sum',
        'returned_home': 'sum',
        'department_code': 'nunique'
    }).reset_index()
    
    france_daily.columns = ['date', 'total_hospitalizations', 'total_icu', 'total_deaths', 'total_recovered', 'regions_count']
    france_daily['country'] = 'FRANCE'
    france_daily['total_cases'] = 0
    france_daily['avg_age'] = 0
    
    # Colombia
    colombia_path = "/opt/airflow/data/formatted/colombia/colombia_cases.parquet"
    colombia_df = pd.read_parquet(colombia_path)
    print(f"Loaded Colombia: {len(colombia_df)} records")
    
    # Colonnes correctes basees sur les logs
    date_col = 'fecha_reporte_web'
    dept_col = 'departamento_nom'  # CORRIGE!
    age_col = 'edad'
    status_col = 'estado'
    
    colombia_df[date_col] = pd.to_datetime(colombia_df[date_col])
    
    # Agregation simple
    colombia_agg = colombia_df.groupby(date_col).size().reset_index(name='total_cases')
    colombia_agg.columns = ['date', 'total_cases']
    
    # Regions count
    regions = colombia_df.groupby(date_col)[dept_col].nunique().reset_index()
    regions.columns = ['date', 'regions_count']
    colombia_agg = colombia_agg.merge(regions, on='date')
    
    # Age moyen
    age_avg = colombia_df.groupby(date_col)[age_col].mean().reset_index()
    age_avg.columns = ['date', 'avg_age']
    colombia_agg = colombia_agg.merge(age_avg, on='date')
    
    # Deaths
    deaths = colombia_df[colombia_df[status_col] == 'Fallecido'].groupby(date_col).size().reset_index(name='total_deaths')
    deaths.columns = ['date', 'total_deaths']
    colombia_agg = colombia_agg.merge(deaths, on='date', how='left')
    colombia_agg['total_deaths'] = colombia_agg['total_deaths'].fillna(0)
    
    # Recovered
    recovered = colombia_df[colombia_df[status_col] == 'Recuperado'].groupby(date_col).size().reset_index(name='total_recovered')
    recovered.columns = ['date', 'total_recovered']
    colombia_agg = colombia_agg.merge(recovered, on='date', how='left')
    colombia_agg['total_recovered'] = colombia_agg['total_recovered'].fillna(0)
    
    colombia_agg['country'] = 'COLOMBIA'
    colombia_agg['total_hospitalizations'] = 0
    colombia_agg['total_icu'] = 0
    
    # Colonnes finales
    columns = ['date', 'country', 'total_cases', 'total_hospitalizations', 'total_icu', 'total_deaths', 'total_recovered', 'regions_count', 'avg_age']
    
    france_final = france_daily[columns]
    colombia_final = colombia_agg[columns]
    
    combined = pd.concat([france_final, colombia_final], ignore_index=True)
    combined['date'] = pd.to_datetime(combined['date']).dt.strftime('%Y-%m-%d')
    
    print(f"Combined: {len(combined)} records")
    
    output_dir = "/opt/airflow/data/usage/covid_comparison"
    os.makedirs(output_dir, exist_ok=True)
    combined.to_csv(f"{output_dir}/daily_comparison.csv", index=False)
    print(f"Saved to {output_dir}/daily_comparison.csv")
    
    return True

def index_elasticsearch():
    import pandas as pd
    from elasticsearch import Elasticsearch
    import math
    
    print("Starting Elasticsearch indexation...")
    
    es = Elasticsearch(["http://elasticsearch:9200"])
    
    if not es.ping():
        raise ConnectionError("Cannot connect to Elasticsearch")
    
    print("Connected to Elasticsearch")
    
    if es.indices.exists(index="covid-analytics"):
        es.indices.delete(index="covid-analytics")
    
    mapping = {
        "mappings": {
            "properties": {
                "date": {"type": "date"},
                "country": {"type": "keyword"},
                "total_cases": {"type": "float"},
                "total_hospitalizations": {"type": "float"},
                "total_icu": {"type": "float"},
                "total_deaths": {"type": "float"},
                "total_recovered": {"type": "float"},
                "regions_count": {"type": "integer"},
                "avg_age": {"type": "float"}
            }
        }
    }
    es.indices.create(index="covid-analytics", body=mapping)
    print("Created index")
    
    csv_path = "/opt/airflow/data/usage/covid_comparison/daily_comparison.csv"
    df = pd.read_csv(csv_path)
    df = df.fillna(0)
    
    print(f"Indexing {len(df)} documents...")
    
    for _, row in df.iterrows():
        doc = {k: (0 if pd.isna(v) or (isinstance(v, float) and math.isnan(v)) else v) for k, v in row.to_dict().items()}
        es.index(index="covid-analytics", document=doc)
    
    print(f"Indexed {len(df)} documents!")
    return True

# Tasks
ingest_france_task = PythonOperator(task_id='ingest_france', python_callable=ingest_france, dag=dag)
ingest_colombia_task = PythonOperator(task_id='ingest_colombia', python_callable=ingest_colombia, dag=dag)
format_france_task = PythonOperator(task_id='format_france', python_callable=format_france, dag=dag)
format_colombia_task = PythonOperator(task_id='format_colombia', python_callable=format_colombia, dag=dag)
combine_task = PythonOperator(task_id='combine_data', python_callable=combine_data, dag=dag)
index_task = PythonOperator(task_id='index_elasticsearch', python_callable=index_elasticsearch, dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

start >> [ingest_france_task, ingest_colombia_task]
ingest_france_task >> format_france_task
ingest_colombia_task >> format_colombia_task
[format_france_task, format_colombia_task] >> combine_task
combine_task >> index_task
index_task >> end
