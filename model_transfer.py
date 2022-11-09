import os
import tempfile

import yaml
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from mlflow.client import MlflowClient

from decors import get_connection, remove, setup
from uploadflow import ssh_download
from webdav_stagein import LFSC, RFSC, walk_dir

default_args = {
    'owner': 'airflow',
}


def transfer_model(local_client, remote_client):

    for experiment in local_client.search_experiments():
        print('Processing experiment ', experiment.experiment_id, experiment.name)

        # check if it already exists:
        lst = remote_client.search_experiments(
            filter_string=f"name = '{experiment.name}'")
        if not lst:
            print("Creating experiment")
            # , artifact_location='http://localhost:5000/')
            remote_id = remote_client.create_experiment(experiment.name)

        else:
            print("Experiment exists")
            remote_id = lst[0].experiment_id

        print("Remote experiment id", remote_id)

        runs = local_client.search_runs(
            experiment_ids=[experiment.experiment_id])

        for run in runs:
            remote_run = remote_client.create_run(
                experiment_id=remote_id, start_time=run.info.start_time)

            remote_run_id = remote_run.info.run_id
            print('Processing run:', run.info.run_id, '->', remote_run_id)

            for metric in run.data.metrics:
                metric_history = local_client.get_metric_history(
                    run_id=run.info.run_id, key=metric)
                print(
                    f"Got metric history for {metric} length={len(metric_history)}")
                remote_client.log_batch(
                    run_id=remote_run_id, metrics=metric_history)

            print('Params:', run.data.params)
            for n, v in run.data.params.items():
                remote_client.log_param(run_id=remote_run_id, key=n, value=v)

            # tags if any?

            artifacts = local_client.list_artifacts(run_id=run.info.run_id)
            # mlflow.set_tracking_uri('http://localhost:5000')
            with tempfile.TemporaryDirectory() as tmpdirname:
                print('created temporary directory', tmpdirname)
                for art in artifacts:
                    local_client.download_artifacts(
                        run_id=run.info.run_id, path=art.path, dst_path=tmpdirname)
                    remote_client.log_artifact(
                        run_id=remote_run_id, local_path=os.path.join(tmpdirname, art.path))

            remote_client.set_terminated(remote_run_id)
            print('-'*10)


@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['example', 'model repo'])
def mlflow_model_transfer():

    @task()
    def load(connection_id, **kwargs):
        params = kwargs['params']
        target = os.path.join(Variable.get(
            "working_dir", default_var='/tmp/'), 'mlruns')
        source = os.path.join(params.get('source', '/tmp/'), 'mlruns')

        ssh_hook = get_connection(conn_id=connection_id, **kwargs)
        sftp_client = ssh_hook.get_conn().open_sftp()
        sclient = RFSC(sftp_client)

        mappings = list(walk_dir(client=sclient, path=source, prefix=''))
        for fname in mappings:
            localname = fname.replace(source, target)
            print('Processing', fname, '-->', localname)

            di = os.path.dirname(localname)
            os.makedirs(di, exist_ok=True)

            #sftp_client.get(remotepath=fname, localpath=localname)
            ssh_download(sftp_client=sftp_client, remote=source, local=target)
        return target

    @task
    def convert_artifact_locations(location, **kwargs):

        # BashOperator with something like that would do probably as well:
        # find . -name "*.yaml" -exec sed -i 's/source/notebooks/\/tmp\/myro/g' {} \;
        # we are a little bit more flexible and error prone by not verifing the initial location
        fscllient = LFSC()

        metas = [m for m in walk_dir(
            client=fscllient, prefix='', path=location) if m.endswith('meta.yaml')]
        for meta in metas:
            print('Converting', meta)
            with open(meta) as f:
                ct = yaml.safe_load(f)
            if not ct:
                continue

            for key, vals in ct.items():
                if isinstance(vals, str) and vals.startswith("file://"):
                    bname = os.path.basename(vals)
                    vals = f"file://{os.path.join(location, bname)}"
                    ct[key] = vals

            with open(meta, 'w') as f:
                yaml.dump(ct, f)

        return location

    @task()
    def register_local2remote(location, **kwargs):
        local_client = MlflowClient(
            tracking_uri=location, registry_uri=location)
        remote_client = MlflowClient(
            tracking_uri='http://localhost:5000', registry_uri='http://localhost:5000')
        transfer_model(local_client=local_client, remote_client=remote_client)

    setup_task = PythonOperator(
        python_callable=setup, task_id='setup_connection')
    a_id = setup_task.output['return_value']
    cleanup_task = PythonOperator(python_callable=remove, op_kwargs={
                                  'conn_id': a_id}, task_id='cleanup')

    location = load(connection_id=a_id)

    converted = convert_artifact_locations(location=location)
    register_local2remote(location=converted)

    setup_task >> location >> cleanup_task


dag = mlflow_model_transfer()
