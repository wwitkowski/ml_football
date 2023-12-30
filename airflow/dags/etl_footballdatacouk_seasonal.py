from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'max_active_runs': 1,
    'retries': 0,
    'catchup': False
}

dag = DAG(
    'footballdata_co_uk_seasonal_download',
    default_args=default_args,
    schedule_interval='0 10 * * SUN,MON,TUE'
)

task = DockerOperator(
    task_id='etl',
    image='ml_football-football_data_co_uk:latest',
    dag=dag,
    network_mode="bridge",
    api_version='auto',
    auto_remove=True,
    mounts=['./data:/app/data'],
    command='python -m app/footballdata_co_uk/football_data_co_uk_seasonal'    
)