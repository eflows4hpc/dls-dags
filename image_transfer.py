import os

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from decors import get_connection, remove, setup
from utils import http2ssh

default_args = {
    "owner": "airflow",
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["example"],
)
def transfer_image():
    @task
    def stream_upload(connection_id, **kwargs):
        params = kwargs["params"]
        force = params.get("force", True)
        target = params.get("target", "/tmp/")
        image_id = params.get("image_id", "wordcount_skylake.sif")
        target = os.path.join(target, image_id)
        url = f"https://bscgrid20.bsc.es/image_creation/images/download/{image_id}"

        print(f"Putting {url} --> {target}")
        ssh_hook = get_connection(conn_id=connection_id, **kwargs)

        with ssh_hook.get_conn() as ssh_client:
            return http2ssh(
                url=url, ssh_client=ssh_client, remote_name=target, force=force
            )

    setup_task = PythonOperator(python_callable=setup, task_id="setup_connection")
    a_id = setup_task.output["return_value"]
    cleanup_task = PythonOperator(
        python_callable=remove, op_kwargs={"conn_id": a_id}, task_id="cleanup"
    )

    setup_task >> stream_upload(connection_id=a_id) >> cleanup_task


dag = transfer_image()
