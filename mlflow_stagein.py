import os
import tempfile
import pendulum

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator


from decors import get_connection, remove, setup
from utils import file_exist, copy_streams


@dag(
    schedule=None,
    start_date=pendulum.today("UTC"),
    tags=["example", "model repo"],
    params={
        "location": Param("/tmp/", type="string", description="target location to copy the model into"),
        "vault_id": Param(default="", type="string"),
        "host": Param(default="", type="string"),
        "port": Param(type="integer", default=22),
        "login": Param(default="", type="string"),
        "mlflow_runid": Param(default="", type="string", description="run from which model should be staged-in"),
        "mlflow_modelpath": Param(type="string", default="model/model.pkl")
    },
)
def mlflow_download():
    @task()
    def copy_model(connection_id, **context):
        from utils import get_mlflow_client
        import shutil

        parms = context["params"]
        location = parms["location"]
        run_id = parms["mlflow_runid"]
        model_path = parms["mlflow_modelpath"]

        target = Variable.get("working_dir", default_var="/tmp/")
        temp_dir = tempfile.mkdtemp(dir=target)

        client = get_mlflow_client()
        ret = client.download_artifacts(run_id=run_id, path=model_path, dst_path=temp_dir) 
        if ret:
            print("Model dowloaded: ", ret)
            

        ssh_hook = get_connection(conn_id=connection_id, **context)
        sftp_client = ssh_hook.get_conn().open_sftp()

        with open(ret, "rb") as sr:
            target_name = os.path.join(location, ret[len(temp_dir):])
            print(f"Uploading: {ret}-->{target_name}")
            if file_exist(sftp=sftp_client, name=target_name):
                print(target_name," exists. Overwritting.")

            with sftp_client.open(target_name, "wb") as tr:
                tr.set_pipelined(pipelined=True)
                copy_streams(inp=sr, outp=tr)
        

        shutil.rmtree(path=temp_dir)
     
        return target_name

    setup_task = PythonOperator(python_callable=setup, task_id="setup_connection")
    a_id = setup_task.output["return_value"]

    cpy = copy_model(connection_id=a_id)

    cleanup_task = PythonOperator(
        python_callable=remove, op_kwargs={"conn_id": a_id}, task_id="cleanup"
    )

    setup_task >> cpy >> cleanup_task


dag = mlflow_download()
