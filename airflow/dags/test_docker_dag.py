from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 29),
    'max_active_runs': 1,
    'retries': 0,
    'catchup': False
}

with DAG('docker_operator_demo', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    t3 = DockerOperator(
        task_id='docker_command_world',
        image='ml_football-football_data_co_uk:latest',
        container_name='task___command_world',
        api_version='auto',
        auto_remove=True,
        command="echo world",
        network_mode="bridge"
    )
