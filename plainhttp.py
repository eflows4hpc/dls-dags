

import os

import requests
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from decors import get_connection, remove, setup
from image_transfer import file_exist

default_args = {
    'owner': 'airflow',
}

def http2ssh(url: str, ssh_client, remote_name: str, force=True):
    sftp_client = ssh_client.open_sftp()
    size = file_exist(sftp=sftp_client, name=remote_name)
    if size>0:
        print(f"File {remote_name} exists and has {size} bytes")
        if force is not True:
            return 0
        print("Forcing overwrite")

    dirname = os.path.dirname(remote_name)
    ssh_client.exec_command(command=f"mkdir -p {dirname}")
    ssh_client.exec_command(command=f"touch {remote_name}")

    with requests.get(url, stream=True, verify=False, timeout=(2,3)) as r:
        with sftp_client.open(remote_name, 'w') as f:
            f.set_pipelined(pipelined=True)
            for chunk in r.iter_content(chunk_size=1024*1000):
                content_to_write = memoryview(chunk)
                f.write(content_to_write)

        return 0

@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['wp4', 'http', 'ssh'])
def plainhttp2ssh():

    @task
    def stream_upload(connection_id, **kwargs):
        params = kwargs['params']
        force = params.get('force', True)
        target = params.get('target', '/tmp/')
        url = params.get('url', '')
        if not url:
            print('Provide valid url')
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
