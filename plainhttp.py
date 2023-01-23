

import os

import requests
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from decors import get_connection, remove, setup
from image_transfer import http2ssh

default_args = {
    'owner': 'airflow',
}


@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['wp4', 'http', 'ssh'])
def plainhttp2ssh():

    @task
    def stream_upload(connection_id, **kwargs):
        params = kwargs['params']
        force = params.get('force', True)
        target = params.get('target', '/tmp/')
        url = params.get('url', '')
        if not url:
            print('Provide a valid url')
            return -1

        print(f"Putting {url} --> {target}")
        ssh_hook = get_connection(conn_id=connection_id, **kwargs)

        with ssh_hook.get_conn() as ssh_client:
            return http2ssh(url=url, ssh_client=ssh_client, remote_name=target, force=force)
        
    setup_task = PythonOperator(python_callable=setup, task_id='setup_connection')
    a_id = setup_task.output['return_value']
    cleanup_task = PythonOperator(python_callable=remove, op_kwargs={'conn_id': a_id}, task_id='cleanup')

    setup_task >> stream_upload(connection_id=a_id) >> cleanup_task


dag = plainhttp2ssh()
