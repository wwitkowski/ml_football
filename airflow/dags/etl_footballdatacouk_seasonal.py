from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 28),
    'retries': 0
}

dag = DAG(
    'footballdata_co_uk_seasonal_download',
    default_args=default_args,
    schedule_interval='0 10 * * SUN,MON,TUE',
    catchup=False,
    max_active_runs=1
)

task = DockerOperator(
    task_id='etl',
    image='ml_football-football_data_co_uk:latest',
    dag=dag,
    network_mode='bridge',
    api_version='auto',
    auto_remove=True,
    mounts=[
        Mount(
            source='/opt/airflow/data', 
            target='/app/data', 
            type='bind'
        )
    ],
    command='python -m app/footballdata_co_uk/football_data_co_uk_seasonal'    
)
