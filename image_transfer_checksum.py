import os
from httpx import delete
import requests

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from decors import setup, get_connection, remove

"""The pipeline is doing an HTTP get request and writes the file to a remote repository via SFTP.
Added a comparison for content-length vs file size after the file transfer to insure a completeness. 
Additionally a checksum check is performed. The checksum is based on the sha256 algorithm.
At the moment the logic assumes that the user has provided the initial checksum, currently in the form of an additional file in the same location, 
that has the exact same name as the original with an added extension ".sha". 
If such a file with the initial checksum does not exist, the checksum logic altogether gets ignored.
"""
        
default_args = {
    'owner': 'airflow',
}

def file_exist(sftp, name):
    try:
        r = sftp.stat(name)  
        return r.st_size
    except:
        return -1

@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['example'])
def test_checksum():

    
    @task
    def stream_upload(connection_id, **kwargs):
        params = kwargs['params']
        target = params.get('target', '/tmp/')
        # image_id = params.get('image_id', 'wordcount_skylake.sif')
        image_id = params.get('image_id', 'Screenshot_20220217_134904.png')
        # url = f"https://bscgrid20.bsc.es/image_creation/images/download/{image_id}"
        url = f"https://b2share-testing.fz-juelich.de/api/files/d9d05cfe-e8f0-4703-843c-0bfe5c22f6e3/{image_id}"
        url_sha = url + ".sha"

        print(f"Putting {url} --> {target} connection")
        print(f"Connection id is:{connection_id}")
        print(f"Keywords are: {kwargs.keys()}")
        
        response = requests.get(url_sha)
        checksum_before = 0;
        # if the sha file exists, expected is sha256 
        if response.status_code == 200: 
            checksum_before = response.content
        
        ssh_hook = get_connection(conn_id=connection_id, **kwargs)

        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            remote_name = os.path.join(target, image_id)
            size = file_exist(sftp=sftp_client, name=remote_name)
            if size>0:
                print(f"File {remote_name} exists and has {size} bytes")
                force = params.get('force', True)
                if force is not True:
                    return 0
                print("Forcing overwrite")

            ssh_client.exec_command(command=f"mkdir -p {target}")
            ssh_client.exec_command(command=f"touch {remote_name}")


            with requests.get(url, stream=True, verify=False, timeout=None) as r:
                with sftp_client.open(remote_name, 'w+') as f:
                    f.set_pipelined(pipelined=True)
                    content_length = r.headers['Content-length']
                    print(f"The content length of the remote file is: {content_length} bytes")
                    while True:
                        chunk=r.raw.read(1024 * 1000)
                        if not chunk:
                            break
                        content_to_write = memoryview(chunk)
                        f.write(content_to_write)
                        # break
                    file_size = f.stat().st_size
                    if content_length and (int(file_size) != int(content_length)):
                        raise ValueError(f'The file transfer was incomplete. Transferred only {file_size} bytes. Expected were {content_length} bytes')
                if checksum_before:
                    stdin, stdout, stderr = ssh_client.exec_command(f'sha256sum {remote_name}')
                    checksum_after = stdout.read()
                    checksum_after = checksum_after.split()[0]
                    
                    print(f"The calculated checksum after the transfer is: {checksum_after}")
                    if checksum_before.strip() != checksum_after.strip():
                        raise ValueError(f'The file transfer was incomplete. Checksum mismatch')
            

                    
    setup_task = PythonOperator(
        python_callable=setup, task_id='setup_connection')
    a_id = setup_task.output['return_value']
    
    cleanup_task = PythonOperator(python_callable=remove, op_kwargs={
                                  'conn_id': a_id}, task_id='cleanup')

    setup_task >> stream_upload(connection_id=a_id) >> cleanup_task


dag = test_checksum()
