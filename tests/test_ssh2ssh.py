import unittest
from unittest.mock import MagicMock, patch
from airflow.models import DagBag
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from ssh2ssh import get_prefixed_params
from airflow import settings

RUN_ID = "sshRUn"


class TestSSH2SSH(unittest.TestCase):
    def tearDown(self) -> None:
        session = settings.Session()
        _ = session.execute(f"delete from dag_run where run_id = '{RUN_ID}'")
        session.commit()

    def test_prefixed(self):
        params = {
            "source_vault_id": "foo_id",
            "source_host": "foo_host",
            "source_port": 44,
            "source_login": "foo_user",
            "source_path": "/foo/path",
            "target_vault_id": "bar_id",
            "target_host": "bar_host",
            "target_port": 33,
            "target_login": "bar_user",
            "target_path": "/bar/path",
        }
        cparams = get_prefixed_params(prefix="source_", params=params)
        #self.assertEqual("vault_foo_id", conn_id)
        self.assertEqual(cparams["host"], "foo_host")
        self.assertEqual(cparams["port"], 44)

        cparams = get_prefixed_params(prefix="target_", params=params)
        #self.assertEqual("vault_bar_id", conn_id)
        self.assertEqual(cparams["host"], "bar_host")
        self.assertEqual(cparams["port"], 33)

    @patch("decors.get_connection")
    @patch("utils.walk_dir")
    @patch("utils.copy_streams")
    def test_run_cpy(self, cpy, w_dir, get_conn):
        source_ssh = MagicMock()
        source_client = MagicMock()
        source_ssh.get_conn().__enter__().open_sftp = source_client

        target_ssh = MagicMock()
        target_client = MagicMock()
        target_ssh.get_conn().__enter__().open_sftp = target_client

        get_conn.side_effects = [source_ssh, target_ssh]

        w_dir.return_value = [
            "/home/foo/a.txt",
            "/home/foo/b.txt",
            "/home/foo/sub/foo.bar",
        ]

        dagbag = DagBag(".", include_examples=False)
        dag = dagbag.get_dag(dag_id="ssh2ssh")
        self.assertIsNotNone(dag)

        dagrun = dag.create_dagrun(
            state=DagRunState.RUNNING,
            run_id=RUN_ID,
            run_type=DagRunType.MANUAL,
            conf={
                "source_vault_id": "foo_id",
                "source_host": "foo_host",
                "source_port": 44,
                "source_login": "foo_user",
                "source_path": "/home/foo/",
                "target_vault_id": "bar_id",
                "target_host": "bar_host",
                "target_port": 33,
                "target_login": "bar_user",
                "target_path": "/home/bar/",
            },
        )

        ti = dagrun.get_task_instance(task_id="copy")
        ti.task = dag.get_task(task_id="copy")

        ti.run(ignore_all_deps=True, ignore_ti_state=True, test_mode=True)
        self.assertEqual(ti.state, TaskInstanceState.SUCCESS)
        ret = ti.xcom_pull(key="return_value")
        print("Returned", ret)
        self.assertDictEqual(
            ret,
            {
                "/home/foo/a.txt": "/home/bar/a.txt",
                "/home/foo/b.txt": "/home/bar/b.txt",
                "/home/foo/sub/foo.bar": "/home/bar/sub/foo.bar",
            },
        )
