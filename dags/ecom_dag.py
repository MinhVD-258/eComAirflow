# Airflow imports
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.base import PokeReturnValue

# Other imports
from datetime import datetime, timedelta

# Tasks imports
from include.ecom.tasks import _get_logical_date, _store_data_in_minio

@dag(
    start_date=datetime(2025,4,1),
    schedule=None,
    catchup=False,
    description='This is a test for various feature.',
    dagrun_timeout=timedelta(minutes=5),
)

def ecom_dag():
    # Get logical date to check for .csv files later.
    get_logical_date = PythonOperator(
        task_id='get_logical_date',
        python_callable=_get_logical_date
    )

    # Check if the file at the logical date exsits in the specified location.
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def check_file_exists(ti=None) -> PokeReturnValue:
        import os.path
        logical_date = ti.xcom_pull(
            key='logical_date',
            task_ids='get_logical_date'
        )
        file_path = f'/usr/local/airflow/sensor/{logical_date}.csv'
        does_file_exist = os.path.exists(file_path)
        if does_file_exist == True:
            ti.xcom_push(key='file_path', value=file_path)
        return PokeReturnValue(is_done=does_file_exist)

    # Store data on MinIO.
    store_data_in_minio = PythonOperator(
        task_id='store_data_in_minio',
        python_callable=_store_data_in_minio
    )

    format_data = DockerOperator(
        task_id='format_data',
        image='airflow/format-app',
        container_name='format_csv',
        api_version='auto',
        auto_remove='success',
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{ ti.xcom_pull(task_ids="store_data_in_minio") }}'
        }
    )

    get_logical_date >> check_file_exists() >> store_data_in_minio >> format_data

ecom_dag()
