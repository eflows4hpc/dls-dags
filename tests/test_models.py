
import unittest
from unittest.mock import MagicMock, Mock, patch, create_autospec
from model_transfer import transfer_model
from mlflow.client import MlflowClient
from collections import namedtuple

Experiment = namedtuple('Experiment', ['artifact_location', 'creation_time', 'experiment_id', 'name'])
RunInfo = namedtuple('RunInfo', [ 'run_id', 'run_name', 'start_time'])
RunData  = namedtuple('RunData', ['metrics', 'tags', 'params'])
Run = namedtuple('Run', ['metrics', 'params', 'tags', 'info', 'data' ])
Artifact = namedtuple('Artifact', ['path'])

class TestModels(unittest.TestCase):

    #@patch('image_transfer.file_exist')
    def test_transfer(self):
        local_client = create_autospec(MlflowClient)
        local_client.search_experiments.return_value = [Experiment(artifact_location='bla', creation_time='o', experiment_id='0', name='rem')]
        local_client.search_runs.return_value=[Run(metrics={}, tags={}, params={}, 
            info=RunInfo(run_id=2, run_name='fss', start_time=8281), 
            data=RunData(metrics={}, tags={}, params={'key': 'value'}))]

        local_client.list_artifacts.return_value = [Artifact(path='foo'), Artifact(path='bar')]

        remote_client = create_autospec(MlflowClient)
        remote_client.search_experiments.return_value = [Experiment(artifact_location='bla', creation_time='o', experiment_id='r0', name='rem')]
        remote_client.create_run.return_value = (Run(metrics={}, tags={}, params={}, 
            info=RunInfo(run_id=88, run_name='remo', start_time=1211),
            data=RunData(metrics={}, tags={}, params={})))

        transfer_model(local_client, remote_client)

        local_client.search_experiments.assert_called()
        remote_client.create_run.assert_called()
        remote_client.log_param.assert_called()

        local_client.download_artifacts.assert_called()
        remote_client.log_artifact.assert_called()

        remote_client.set_terminated.assert_called()