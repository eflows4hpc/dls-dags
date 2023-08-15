from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
import pendulum
from airflow.models.param import Param
from decors import get_connection, remove, setup
from utils import http2ssh, clean_up_vaultid

default_args = {
    "owner": "airflow",
}


@dag(
    default_args=default_args,
    schedule=None,
    start_date=pendulum.today("UTC"),
    tags=["wp4", "http", "ssh"],
    on_success_callback=clean_up_vaultid,
    params={
        "vault_id": Param(default="", type="string"),
        "host": Param(default="", type="string"),
        "port": Param(default=22, type="integer"),
        "login": Param(default="", type="string"),
        "force": Param(True, type="boolean"),
        "target": Param("/tmp/", type="string"),
        "url": Param("", type="string")
        }

)
def plainhttp2ssh():
    @task
    def stream_upload(connection_id, **kwargs):
        params = kwargs["params"]
        force = params.get("force", True)
        target = params.get("target", "/tmp/")
        url = params.get("url", "")
        if not url:
            print("Provide a valid url")
            return -1

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


dag = plainhttp2ssh()
