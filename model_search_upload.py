import json
import os
import tempfile
import pendulum
import pandas as pd

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from decors import get_connection, remove, setup
from utils import file_exist, ssh_download


@dag(
    schedule=None,
    start_date=pendulum.today("UTC"),
    tags=["example", "model repo"],
    params={
        "location": Param("/tmp/", type="string", description="location of model directory with pd.csv and model.dat"),
        "experiment_name": Param("model_search", type="string", description="name of the experiment in the model repository"),
        "vault_id": Param(default="", type="string"),
        "host": Param(default="", type="string"),
        "port": Param(type="integer", default=22),
        "login": Param(default="", type="string"),
    },
)
def model_search_upload():
    @task
    def download_artifacts(connection_id, **context):
        parms = context["params"]
        location = parms["location"]

        target = Variable.get("working_dir", default_var="/tmp/")
        temp_dir = tempfile.mkdtemp(dir=target)

        ssh_hook = get_connection(conn_id=connection_id, **context)
        sftp_client = ssh_hook.get_conn().open_sftp()

        for fl in ['pd.csv', 'model.dat']:
            if file_exist(sftp=sftp_client, name=os.path.join(location,fl)):
                print(f"Downloading model search results ({location}/{fl})")
                ssh_download(
                    sftp_client=sftp_client,
                    remote=os.path.join(location, fl),
                    local=os.path.join(temp_dir, fl),
                )
            else:
                print(f"No model search result found {location}/{fl}")

        return temp_dir

    #@task.virtualenv(requirements=["mlflow==2.3.2"])
    @task
    def uploat_to_mlflow(temp_dir, **context):
        from utils import get_mlflow_client
        import shutil


        client = get_mlflow_client()
        parms = context["params"]
        experiment_name = parms["experiment_name"]
      
        experiment = client.get_experiment_by_name(experiment_name)
        if experiment:
            experiment_id = experiment.experiment_id
        else:
            print(f"Experiment {experiment_name} was not found, creating new")
            experiment_id = client.create_experiment(experiment_name)

        run = client.create_run(experiment_id)
        print(f"Uploading to experiment {experiment_name}/{experiment_id}/{run.info.run_id}")

        print("Uploading model")
        client.log_artifact(
            run_id=run.info.run_id,
            local_path=os.path.join(temp_dir, 'model.dat'),
            artifact_path="model",
        )

        print("Uploading model search results")
        df = pd.read_csv(os.path.join(temp_dir, 'pd.csv'), index_col=0)
        dct = df.to_dict()
        metrics=['mean_test_score', 'mean_fit_time']

        for i, p in enumerate(dct['params'].values()):
            with client.start_run():
                p = json.loads(p.replace('\'', '"'))
                for parname, parvalue in p.items():
                    client.log_param(parname, value=parvalue)

                for m in metrics:
                    print(f"Logging metric {m} {dct[m][i]}")
                    client.log_metric(m, dct[m][i])

        #clean up
        if 'temp_dir' in attrs:
            shutil.rmtree(path=attrs["temp_dir"])

    setup_task = PythonOperator(python_callable=setup, task_id="setup_connection")
    a_id = setup_task.output["return_value"]

    attrs = download_artifacts(connection_id=a_id)

    cleanup_task = PythonOperator(
        python_callable=remove, op_kwargs={"conn_id": a_id}, task_id="cleanup"
    )

    setup_task >> attrs >> uploat_to_mlflow(attrs) >> cleanup_task


dag = model_search_upload()
