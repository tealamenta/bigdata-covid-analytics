"""
COVID-19 Analytics Pipeline DAG
Orchestration du pipeline de donnees France vs Colombia
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

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
    schedule_interval='0 6 * * *',  # Daily at 6:00 AM
    catchup=False,
)

# Tasks
start = DummyOperator(task_id='start', dag=dag)

def ingest_france():
    from src.ingestion.france_covid import FranceCovidIngestion
    ingestion = FranceCovidIngestion()
    return ingestion.run()

def ingest_colombia():
    from src.ingestion.colombia_covid import ColombiaCovidIngestion
    ingestion = ColombiaCovidIngestion()
    return ingestion.run()

def format_france():
    from src.formatting.format_france import FranceCovidFormatter
    formatter = FranceCovidFormatter()
    return formatter.run()

def format_colombia():
    from src.formatting.format_colombia import ColombiaCovidFormatter
    formatter = ColombiaCovidFormatter()
    return formatter.run()

def combine_data():
    from src.combination.combine_data import CovidDataCombiner
    combiner = CovidDataCombiner()
    return combiner.run()

def index_elasticsearch():
    import subprocess
    subprocess.run(['python', 'scripts/index_simple.py'], check=True)

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

end = DummyOperator(task_id='end', dag=dag)

# Dependencies
start >> [ingest_france_task, ingest_colombia_task]
ingest_france_task >> format_france_task
ingest_colombia_task >> format_colombia_task
[format_france_task, format_colombia_task] >> combine_task
combine_task >> index_task
index_task >> end
