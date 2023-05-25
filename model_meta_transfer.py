import json
import os
import shutil
import tempfile

from airflow.decorators import dag, task
from airflow.exceptions import AirflowNotFoundException
from airflow.models import Variable
from airflow.models.connection import Connection
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
import pendulum

from decors import get_connection, remove, setup
from utils import file_exist, ssh_download


@dag(
    schedule=None,
    start_date=pendulum.today("UTC"),
    tags=["example", "model repo"],
    params={"location": Param("/tmp/", type="string"),},
)
def mlflow_upload_model():
    @task(multiple_outputs=True)
    def download_artifacts(connection_id, **context):
        parms = context["params"]
        location = parms["location"]

        target = Variable.get("working_dir", default_var="/tmp/")
        temp_dir = tempfile.mkdtemp(dir=target)

        ssh_hook = get_connection(conn_id=connection_id, **content)
        sftp_client = ssh_hook.get_conn().open_sftp()

        content = {}

        if file_exist(sftp=sftp_client, name=os.path.join(location, "attrs.json")):
            print(f"Downloading attributes ({location}/attrs.json)")
            ssh_download(
                sftp_client=sftp_client,
                remote=os.path.join(location, "attrs.json"),
                local=os.path.join(temp_dir, "attrs.json"),
            )
            # why not parse it
            with open(os.path.join(temp_dir, "attrs.json"), "r") as f:
                content = json.load(f)
                print("Got json content: ", content)

        else:
            print(f"No model attributes found {location}/attrs.json")

        if file_exist(sftp=sftp_client, name=os.path.join(location, "model.pkl")):
            print(f"Downloading model ({location}/model.pkl)")
            ssh_download(
                sftp_client=sftp_client,
                remote=os.path.join(location, "model.pkl"),
                local=os.path.join(temp_dir, "model.pkl"),
            )
            content["model"] = os.path.join(temp_dir, "model.pkl")
        else:
            print(f"No serialized model found in {location}")

        content["temp_dir"] = temp_dir
        return content

    @task.virtualenv(requirements=["mlflow==2.3.2"])
    def upl(attrs):
        from utils import get_mlflow_client, upload_metrics
        import shutil
        import tempfile

        client = get_mlflow_client()
        if "metrics" or "model" in attrs:
            name = attrs.get(
                "name", f"experiment_{next(tempfile._get_candidate_names())}"
            )
            experiment = client.get_experiment_by_name(name)
            if experiment:
                experiment_id = experiment.experiment_id
            else:
                print("Experiment with given name was not found, creating new")
                experiment_id = client.create_experiment(name)

            run = client.create_run(experiment_id)
            print(f"Uploading to experiment {name}/{experiment_id}/{run.info.run_id}")

        if "metrics" or "params" in attrs:
            print("Uploading metrics client")
            upload_metrics(mlflow_client=client, metadata=attrs, runid=run.info.run_id)

        if "model" in attrs:
            print("Uploading model")
            client.log_artifact(
                run_id=run.info.run_id,
                local_path=attrs["model"],
                artifact_path="model/model.pkl",
            )

        client.log_text(
            run_id=run.info.run_id,
            text="This experiment was created by DLS",
            artifact_file="model/meta.txt",
        )
        shutil.rmtree(path=attrs["temp_dir"])

    setup_task = PythonOperator(python_callable=setup, task_id="setup_connection")
    a_id = setup_task.output["return_value"]

    attrs = download_artifacts(connection_id=a_id)

    cleanup_task = PythonOperator(
        python_callable=remove, op_kwargs={"conn_id": a_id}, task_id="cleanup"
    )

    setup_task >> attrs >> upl(attrs) >> cleanup_task


dag = mlflow_upload_model()
