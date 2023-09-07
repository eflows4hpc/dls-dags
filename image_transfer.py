import os
from urllib.parse import urljoin

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
import pendulum
from decors import get_connection, remove, setup, get_creds_from_vault_path
import requests
from utils import file_exist, clean_up_vaultid

def http2ssh(url: str, ssh_client, remote_name: str, force=True, auth=None):
    sftp_client = ssh_client.open_sftp()
    size = file_exist(sftp=sftp_client, name=remote_name)
    if size > 0:
        print(f"File {remote_name} exists and has {size} bytes")
        if force is not True:
            return 0
        print("Forcing overwrite")

    dirname = os.path.dirname(remote_name)
    ssh_client.exec_command(command=f"mkdir -p {dirname}")
    ssh_client.exec_command(command=f"touch {remote_name}")

    written = 0
    required = 1
    retries = 0
    max_retries = 10
    headers = None

    with sftp_client.open(remote_name, "wb") as f:
        f.set_pipelined(pipelined=True)
        while (written < required and retries < max_retries):
            with requests.get(url, stream=True, verify=False, auth=auth, headers=headers) as r:
                if retries==0: # this is the first try
                    required = int(r.headers.get('Content-Length', 0))
                    print(f"File size is {required}")

                for chunk in r.iter_content(chunk_size=1024 * 1000):
                    written += len(chunk)
                    content_to_write = memoryview(chunk)
                    f.write(content_to_write)

            print(f"Written {written} bytes. Content-lenght {required}")
            
            if required>0 and written<required:
                print(f"This was {retries}/{max_retries} try. Size mismatch detected: {written} < {required}. Preparing range query")
                headers = {'Range': f"bytes={written}-"}
                retries+=1

        return 0

@dag(
    default_args={
    "owner": "airflow",
    },
    on_success_callback=clean_up_vaultid,
    schedule=None,
    start_date=pendulum.yesterday('UTC'),
    tags=["example"],
    params={
        "force": Param(True, type="boolean"),
        "target": Param("/tmp/", type="string"),
        "image_id": Param("wordcount_skylake.sif", type="string"),
        "url": Param("https://bscgrid20.bsc.es/image_creation/images/download/", type="string"),
        "vault_path": Param("", type="string")
        }
)
def transfer_image():
    
    @task
    def stream_upload(connection_id, **kwargs):
        params = kwargs["params"]
        force = params.get("force")
        target = params.get("target")
        image_id = params.get("image_id")
        target = os.path.join(target, image_id)
        url = params.get("url")
        url = urljoin(url, image_id)
        vault_path = params.get("vault_path", "")
        user, passsword = get_creds_from_vault_path(path=vault_path)

        print(f"Putting {url} --> {target}")
        ssh_hook = get_connection(conn_id=connection_id, **kwargs)

        with ssh_hook.get_conn() as ssh_client:
            return http2ssh(
                url=url,
                ssh_client=ssh_client,
                remote_name=target,
                force=force,
                auth=(user, passsword),
            )

    setup_task = PythonOperator(python_callable=setup, task_id="setup_connection")
    a_id = setup_task.output["return_value"]
    cleanup_task = PythonOperator(
        python_callable=remove, op_kwargs={"conn_id": a_id}, task_id="cleanup"
    )

    setup_task >> stream_upload(connection_id=a_id) >> cleanup_task


dag = transfer_image()
