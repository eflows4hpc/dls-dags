import unittest
from collections import namedtuple
from unittest.mock import MagicMock, create_autospec, patch
from utils import upload_metrics
from airflow import settings
from airflow.models import DagBag
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType
import json


try:
    from mlflow.client import MlflowClient
except ImportError:
    print("Unable to import mlflow")


from model_transfer import transfer_model

Experiment = namedtuple(
    "Experiment", ["artifact_location", "creation_time", "experiment_id", "name"]
)
RunInfo = namedtuple("RunInfo", ["run_id", "run_name", "start_time"])
RunData = namedtuple("RunData", ["metrics", "tags", "params"])
Run = namedtuple("Run", ["metrics", "params", "tags", "info", "data"])
Artifact = namedtuple("Artifact", ["path"])

RUN_ID = "mlflowRunID"


class TestModels(unittest.TestCase):

    def tearDown(self) -> None:
        session = settings.Session()
        _ = session.execute(f"delete from dag_run where run_id = '{RUN_ID}'")
        session.commit()


    def test_conv(self):
        md = {
            "params": {"C": 20.1, "layers": 11, "function": "relu",},
            "metrics": {"MSE": 11.1, "G": 18},
        }
        client = MagicMock()
        client.log_metric = MagicMock()
        client.log_param = MagicMock()
        upload_metrics(runid="run", mlflow_client=client, metadata=md)
        client.log_metric.assert_called()
        client.log_param.assert_called()

    # @patch('image_transfer.file_exist')
    def test_transfer(self):
        if "MlflowClient" not in locals():
            return 0

        local_client = create_autospec(MlflowClient)
        local_client.search_experiments.return_value = [
            Experiment(
                artifact_location="bla",
                creation_time="o",
                experiment_id="0",
                name="rem",
            )
        ]
        local_client.search_runs.return_value = [
            Run(
                metrics={},
                tags={},
                params={},
                info=RunInfo(run_id=2, run_name="fss", start_time=8281),
                data=RunData(metrics={}, tags={}, params={"key": "value"}),
            )
        ]

        local_client.list_artifacts.return_value = [
            Artifact(path="foo"),
            Artifact(path="bar"),
        ]

        remote_client = create_autospec(MlflowClient)
        remote_client.search_experiments.return_value = [
            Experiment(
                artifact_location="bla",
                creation_time="o",
                experiment_id="r0",
                name="rem",
            )
        ]
        remote_client.create_run.return_value = Run(
            metrics={},
            tags={},
            params={},
            info=RunInfo(run_id=88, run_name="remo", start_time=1211),
            data=RunData(metrics={}, tags={}, params={}),
        )

        transfer_model(local_client, remote_client)

        local_client.search_experiments.assert_called()
        remote_client.create_run.assert_called()
        remote_client.log_param.assert_called()

        local_client.download_artifacts.assert_called()
        remote_client.log_artifact.assert_called()

        remote_client.set_terminated.assert_called()


    @patch('decors.get_connection')
    @patch('utils.ssh_download')
    def test_model_transfer_arts(self, ssh_down, get_conn):
        sft_client = MagicMock()
        get_conn.get_conn().__enter__().open_sftp().return_value = sft_client

        def my_down(sftp_client, remote, local):
            print("Creating local file", local)
            content = {
                "params": {"C": 20.1, "layers": 11, "function": "relu",},
                "metrics": {"MSE": 11.1, "G": 18},
                "artifacts": ["/tmp/a", "/tmp/b"]

            }
            with open(local, 'w+') as f:
                json.dump(content, f)
            
        ssh_down.side_effect = my_down
        dagbag = DagBag(".", include_examples=False)
        dag = dagbag.get_dag(dag_id="mlflow_upload_model")
        self.assertIsNotNone(dag)

        dagrun = dag.create_dagrun(
            state=DagRunState.RUNNING,
            run_id=RUN_ID,
            run_type=DagRunType.MANUAL,
            conf={
                "vault_id": "foo",
                "host": "foo_host",
                "login": "foo",
                "location": "/zmpt/",
            },
        )

        ti = dagrun.get_task_instance(task_id="download_artifacts")
        ti.task = dag.get_task(task_id="download_artifacts")

        ti.run(ignore_all_deps=True, ignore_ti_state=True, test_mode=True)
        self.assertEqual(ti.state, TaskInstanceState.SUCCESS)
        ret = ti.xcom_pull(key="return_value")

        self.assertEqual(len(ret['params']), 3)
        self.assertTrue(len(ret['metrics']), 3)
        self.assertEqual(len(ret['artifacts']), 2)

