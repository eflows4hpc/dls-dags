import unittest
from unittest.mock import MagicMock, patch
from airflow import settings
from airflow.models import DagBag
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from utils import get_unicore_client

RUN_ID = "unicoReRun"


class TestUnicores(unittest.TestCase):
    def tearDown(self) -> None:
        session = settings.Session()
        _ = session.execute(f"delete from dag_run where run_id = '{RUN_ID}'")
        session.commit()

    def test_get_uc_client(self):
        cfg = {"user": "foo", "password": "bar", "site_url": "http://foo.bar"}

        with self.assertRaises(Exception) as context:
            _ = get_unicore_client(**cfg)

    @patch("utils.setup_webdav")
    @patch("utils.get_unicore_client")
    @patch("utils.walk_dir")
    def test_run(self, walker, uci, davs):

        walker.return_value = ["/foo/bar", "/foo/bar2"]
        webdav_client = MagicMock()
        davs.return_value = (webdav_client, "foo/", "prefix")

        dagbag = DagBag(".", include_examples=False)
        dag = dagbag.get_dag(dag_id="webdav2unicore")
        self.assertIsNotNone(dag)

        uclient = MagicMock()
        uclient.upload = MagicMock()
        uclient.mkdir = MagicMock()

        uci.return_value = uclient

        dagrun = dag.create_dagrun(
            state=DagRunState.RUNNING,
            run_id=RUN_ID,
            run_type=DagRunType.MANUAL,
            conf={
                "target": "/tmp",
                "oid": "8891",
                "working_dir": "/target/",
                "site_url": "https://localhost:8080/DEMO-SITE/rest/core",
                "user": "demouser",
                "password": "test123",
            },
        )

        ti = dagrun.get_task_instance(task_id="load")
        ti.task = dag.get_task(task_id="load")

        ti.run(ignore_all_deps=True, ignore_ti_state=True, test_mode=True)
        self.assertEqual(ti.state, TaskInstanceState.SUCCESS)

        uclient.mkdir.assert_called_with("/tmp/foo")
        uclient.upload.assert_called()
