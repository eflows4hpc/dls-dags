import os

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import tempfile

from decors import setup, get_connection, remove
#from uploadflow import ssh2local_copy
from mlflow.client import MlflowClient

default_args = {
    'owner': 'airflow',
}

def transfer_model(local_client, remote_client):

    for experiment in local_client.search_experiments():
        print('Processing experiment ', experiment.experiment_id, experiment.name)
        
        #check if it already exists:
        lst = remote_client.search_experiments(filter_string=f"name = '{experiment.name}'")
        if not lst:
            print("Creating experiment")
            remote_id = remote_client.create_experiment(experiment.name)#, artifact_location='http://localhost:5000/')
            
        else:
            print("Experiment exists")
            remote_id = lst[0].experiment_id
            
        print("Remote experiment id", remote_id)

        runs = local_client.search_runs(experiment_ids=[experiment.experiment_id])

        for run in runs:
            remote_run = remote_client.create_run(experiment_id=remote_id, start_time=run.info.start_time)

            remote_run_id = remote_run.info.run_id
            print('Processing run:', run.info.run_id, '->', remote_run_id)

            for metric in run.data.metrics:
                metric_history = local_client.get_metric_history(run_id=run.info.run_id, key=metric)
                print(f"Got metric history for {metric} length={len(metric_history)}")
                remote_client.log_batch(run_id=remote_run_id, metrics=metric_history)

            
            print('Params:', run.data.params)
            for n,v in run.data.params.items():
                remote_client.log_param(run_id=remote_run_id, key=n, value=v)
            
            #tags if any?

            artifacts = local_client.list_artifacts(run_id=run.info.run_id)
            #mlflow.set_tracking_uri('http://localhost:5000')
            with tempfile.TemporaryDirectory() as tmpdirname:
                print('created temporary directory', tmpdirname)
                for art in artifacts:
                    local_client.download_artifacts(run_id=run.info.run_id, path=art.path, dst_path=tmpdirname)
                    remote_client.log_artifact(run_id=remote_run_id, local_path=os.path.join(tmpdirname, art.path))


            remote_client.set_terminated(remote_run_id)
            print('-'*10)

@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['example', 'model repo'])
def mlflow_model_transfer():
    
    @task()
    def load(connection_id, **kwargs):
        params = kwargs['params']
        target = Variable.get("working_dir", default_var='/tmp/')
        source = params.get('source', '/tmp/')
        ssh_hook = get_connection(conn_id=connection_id, **kwargs)

        #mappings = ssh2local_copy(ssh_hook=ssh_hook, source=source, target=target)
        return {}

    @task()
    def register_local2remote(files, **kwargs):
        local_client = MlflowClient()
        remote_client = MlflowClient(tracking_uri='http://localhost:5000', registry_uri='http://localhost:5000')
        transfer_model(local_client=local_client, remote_client=remote_client)
                    
    setup_task = PythonOperator(python_callable=setup, task_id='setup_connection')
    a_id = setup_task.output['return_value']
    cleanup_task = PythonOperator(python_callable=remove, op_kwargs={'conn_id': a_id}, task_id='cleanup')

    files = load(connection_id=a_id)
    register_local2remote(files=files)

    setup_task >> files >> cleanup_task


dag = mlflow_model_transfer()
