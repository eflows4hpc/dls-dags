import unittest
from unittest.mock import MagicMock, patch

from airflow import settings
from airflow.models import DagBag
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from gitlab2ssh import get_gitlab_client, get_project, GitFSC
from utils import walk_dir

URL = "https://gitlab.jsc.fz-juelich.de/"

RUN_ID = "gitRun"


class TestGits(unittest.TestCase):
    def tearDown(self) -> None:
        session = settings.Session()
        _ = session.execute(f"delete from dag_run where run_id = '{RUN_ID}'")
        session.commit()

    def test_get_client(self):
        client = get_gitlab_client(url=URL)
        self.assertIsNotNone(client)

    def test_get_project(self):
        client = get_gitlab_client(url=URL)
        project = get_project(client=client, name="ModelRepository")
        self.assertIsNotNone(project)

        print("Got project", project.name)

    def test_gitF(self):
        client = get_gitlab_client(url=URL)
        project = get_project(client=client, name="ModelRepository")

        gitf = GitFSC(client=project)
        files = gitf.list(path="deployment", get_info=False)
        self.assertIsNotNone(files)
        self.assertTrue(len(files) > 0)
        print("Got files: ", files)

        self.assertTrue("docker-compose.yml" in files)

    def test_walkdir(self):
        client = get_gitlab_client(url=URL)
        project = get_project(client=client, name="ModelRepository")

        gitf = GitFSC(client=project)
        files = list(walk_dir(client=gitf, path="", prefix=""))
        self.assertIsNotNone(files)
        self.assertTrue(len(files) > 0)

        self.assertTrue("deployment/Dockerfile" in files)

    @patch("decors.get_connection")
    def test_rundag(self, get_conn):
        target_ssh = MagicMock()
        target_client = MagicMock()
        target_ssh.get_conn().__enter__().open_sftp = target_client

        get_conn.return_value = target_ssh

        dagbag = DagBag(".", include_examples=False)
        dag = dagbag.get_dag(dag_id="git2ssh")
        self.assertIsNotNone(dag)

        dagrun = dag.create_dagrun(
            state=DagRunState.RUNNING,
            run_id=RUN_ID,
            run_type=DagRunType.MANUAL,
            conf={
                "connection_id": "foo",
                "target": "/target/",
                "gitlab_url": "https://gitlab.jsc.fz-juelich.de",
                "gitlab_repo": "DataCatalog",
            },
        )

        ti = dagrun.get_task_instance(task_id="stream_vupload")
        ti.task = dag.get_task(task_id="stream_vupload")

        ti.run(ignore_all_deps=True, ignore_ti_state=True, test_mode=True)
        self.assertEqual(ti.state, TaskInstanceState.SUCCESS)

        target_ssh.get_conn().__enter__().exec_command.assert_called()
        ret = ti.xcom_pull(key="return_value")
        print("Returned", ret)
        self.assertTrue(ret > 0)
