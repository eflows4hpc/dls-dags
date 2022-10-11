import os
import requests

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from uploadflow import copy_streams

from decors import setup, get_connection, remove

default_args = {
    'owner': 'airflow',
}

def file_exist(sftp, name):
    try:
        r = sftp.stat(name)  
        return r.st_size
    except:
        return -1

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
        written = 0
        with sftp_client.open(remote_name, 'w') as f:
            f.set_pipelined(pipelined=True)
            while True:
                chunk=r.raw.read(1024 * 1000)
                if not chunk:
                    break
                content_to_write = memoryview(chunk)
                f.write(content_to_write)
                written+=len(chunk)
        cl = r.headers.get('Content-Length', 0)
        print(f"Written: {written} Content-Length: {cl}")
        if cl!=written:
            print('Content length mismatch')
            return -1
        return 0


@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['example'])
def transfer_image():
    
    @task
    def stream_upload(connection_id, **kwargs):
        params = kwargs['params']
        force = params.get('force', True)
        target = params.get('target', '/tmp/')
        image_id = params.get('image_id', 'wordcount_skylake.sif')
        target = os.path.join(target, image_id)
        url = f"https://bscgrid20.bsc.es/image_creation/images/download/{image_id}"

        print(f"Putting {url} --> {target}")
        ssh_hook = get_connection(conn_id=connection_id, **kwargs)

        with ssh_hook.get_conn() as ssh_client:
            return http2ssh(url=url, ssh_client=ssh_client, remote_name=target, force=force)
                    
    setup_task = PythonOperator(python_callable=setup, task_id='setup_connection')
    a_id = setup_task.output['return_value']
    cleanup_task = PythonOperator(python_callable=remove, op_kwargs={'conn_id': a_id}, task_id='cleanup')

    setup_task >> stream_upload(connection_id=a_id) >> cleanup_task


dag = transfer_image()
