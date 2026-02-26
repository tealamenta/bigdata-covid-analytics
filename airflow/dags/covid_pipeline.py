"""
COVID-19 Analytics Pipeline DAG
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import os
import sys

# Ajouter le chemin pour les imports
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
    import os
    from datetime import datetime
    
    url = "https://www.data.gouv.fr/fr/datasets/r/63352e38-d353-4b54-bfd1-f1b3ee1cabd7"
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_dir = f"/opt/airflow/data/raw/france/{date_str}"
    os.makedirs(output_dir, exist_ok=True)
    
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    
    output_file = f"{output_dir}/covid_hospitalizations.csv"
    with open(output_file, 'wb') as f:
        f.write(response.content)
    
    print(f"France data saved to {output_file}")
    return True

def ingest_colombia():
    import requests
    import os
    from datetime import datetime
    
    url = "https://www.datos.gov.co/resource/gt2j-8ykr.csv?$limit=100000"
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_dir = f"/opt/airflow/data/raw/colombia/{date_str}"
    os.makedirs(output_dir, exist_ok=True)
    
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    
    output_file = f"{output_dir}/covid_cases.csv"
    with open(output_file, 'wb') as f:
        f.write(response.content)
    
    print(f"Colombia data saved to {output_file}")
    return True

def format_france():
    print("Formatting France data...")
    # Simplified for Docker - just check file exists
    import os
    from datetime import datetime
    date_str = datetime.now().strftime("%Y-%m-%d")
    input_file = f"/opt/airflow/data/raw/france/{date_str}/covid_hospitalizations.csv"
    if os.path.exists(input_file):
        print(f"France file exists: {input_file}")
        return True
    raise FileNotFoundError(f"France file not found: {input_file}")

def format_colombia():
    print("Formatting Colombia data...")
    import os
    from datetime import datetime
    date_str = datetime.now().strftime("%Y-%m-%d")
    input_file = f"/opt/airflow/data/raw/colombia/{date_str}/covid_cases.csv"
    if os.path.exists(input_file):
        print(f"Colombia file exists: {input_file}")
        return True
    raise FileNotFoundError(f"Colombia file not found: {input_file}")

def combine_data():
    print("Combining data...")
    return True

def index_elasticsearch():
    print("Indexing to Elasticsearch...")
    return True

ingest_france_task = PythonOperator(
    task_id='ingest_france',
    python_callable=ingest_france,
    dag=dag,
)

ingest_colombia_task = PythonOperator(
    task_id='ingest_colombia',
    python_callable=ingest_colombia,
    dag=dag,
)

format_france_task = PythonOperator(
    task_id='format_france',
    python_callable=format_france,
    dag=dag,
)

format_colombia_task = PythonOperator(
    task_id='format_colombia',
    python_callable=format_colombia,
    dag=dag,
)

combine_task = PythonOperator(
    task_id='combine_data',
    python_callable=combine_data,
    dag=dag,
)

index_task = PythonOperator(
    task_id='index_elasticsearch',
    python_callable=index_elasticsearch,
    dag=dag,
)

end = EmptyOperator(task_id='end', dag=dag)

# Dependencies
start >> [ingest_france_task, ingest_colombia_task]
ingest_france_task >> format_france_task
ingest_colombia_task >> format_colombia_task
[format_france_task, format_colombia_task] >> combine_task
combine_task >> index_task
index_task >> end
