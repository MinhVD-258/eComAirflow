from airflow.hooks.base import BaseHook
from io import BytesIO
from minio import Minio
import pendulum

BUCKET_NAME = 'ecommerce'

def _get_logical_date(ti=None,**kwargs):
    logical_date = kwargs['logical_date']
    logical_date = logical_date.to_date_string()
    ti.xcom_push(key='logical_date', value=logical_date)

def _get_minio_client():
    minio = BaseHook.get_connection('minio')
    client=Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client

def _store_data_in_minio(ti=None):
    import os.path
    client=_get_minio_client()

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    file_path = ti.xcom_pull(
        key='file_path',
        task_ids='check_file_exists'
    )
    logical_date = ti.xcom_pull(
        key='logical_date',
        task_ids='get_logical_date'
    )
    with open(file_path, 'r') as file:
        eComData = file.read().encode('utf-8')
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f'{logical_date}/raw.csv',
        data=BytesIO(eComData),
        length=len(eComData)
    )
    return f'{objw.bucket_name}/{logical_date}'
