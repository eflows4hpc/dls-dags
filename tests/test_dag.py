import unittest

from unittest.mock import MagicMock, patch

from airflow.models import DagBag
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from airflow.models import Connection

from airflow import settings

RUN_ID = "foobarrun"


class TestADag(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(".")

    def tearDown(self) -> None:
        session = settings.Session()
        res = session.execute(f"delete from dag_run where run_id = '{RUN_ID}'")
        session.commit()

    @unittest.skip("db changed")
    def test_dag_loaded(self):
        dag = self.dagbag.get_dag(dag_id="firsto")
        print(self.dagbag.import_errors)
        self.assertDictEqual(self.dagbag.import_errors, {}, "not equal")
        assert dag is not None
        self.assertEqual(len(dag.tasks), 2, f"Actually: {len(dag.tasks)}")

    def test_tf_loaded(self):
        dag = self.dagbag.get_dag(dag_id="taskflow_example")
        assert self.dagbag.import_errors == {}
        assert dag is not None
        self.assertEqual(len(dag.tasks), 5, f"Actually: {len(dag.tasks)}")

    def test_loaded(self):
        dag = self.dagbag.get_dag(dag_id="testdag")
        self.assertEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)

    def test_plainhttp(self):
        dag = self.dagbag.get_dag(dag_id="plainhttp2ssh")
        self.assertIsNotNone(dag)
        print(dag.task_ids)
        self.assertEqual(len(dag.tasks), 3)

        dagrun = dag.create_dagrun(
            state=DagRunState.RUNNING, run_id=RUN_ID, run_type=DagRunType.MANUAL,
        )

        ti = dagrun.get_task_instance(task_id="stream_upload")
        ti.task = dag.get_task(task_id="stream_upload")

        conn = Connection(conn_type="gcpssh", login="cat", host="conn-host",)
        conn_uri = conn.get_uri()
        with patch.dict("os.environ", AIRFLOW_CONN_TEMP_CONN=conn_uri):
            ti.run(ignore_all_deps=True, ignore_ti_state=True, test_mode=True)
            # won't fail under test

    @patch("decors.get_connection")
    @patch("utils.http2ssh")
    def test_mocking(self, utl, g):
        utl.return_value = 17

        a = MagicMock()
        a.return_value = ["a", "c"]
        stat = MagicMock(side_effect=["elo", "elo"])
        cpy = MagicMock(return_value=False)
        g.get_conn().__enter__().open_sftp().listdir = a
        g.get_conn().__enter__().open_sftp().stat = stat
        g.get_conn().__enter__().open_sftp().open().__enter__().read = cpy

        dagbag = DagBag(".")

        dag = dagbag.get_dag(dag_id="plainhttp2ssh")
        self.assertIsNotNone(dag)
        print(dag.task_ids)
        self.assertEqual(len(dag.tasks), 3)

        dagrun = dag.create_dagrun(
            state=DagRunState.RUNNING,
            run_id=RUN_ID,
            run_type=DagRunType.MANUAL,
            conf={"url": "http://www.google.foo",},
        )

        ti = dagrun.get_task_instance(task_id="stream_upload")
        ti.task = dag.get_task(task_id="stream_upload")

        ti.run(ignore_all_deps=True, ignore_ti_state=True, test_mode=True)
        assert ti.state == TaskInstanceState.SUCCESS

    def test_webdavst(self):
        dag = self.dagbag.get_dag(dag_id="webdav_stagein")
        self.assertIsNotNone(dag)
        print(dag.task_ids)
        self.assertEqual(len(dag.tasks), 3)

    def test_testdag(self):
        dag = self.dagbag.get_dag(dag_id="testdag")
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 2)

        # dagrun = dag.run(local=True)

    @patch("decors.get_connection")
    @patch("utils.ssh2local_copy")
    def test_uploadflow_load(self, utl, g):

        utl.return_value = 17

        a = MagicMock()
        a.return_value = ["a", "c"]
        stat = MagicMock(side_effect=["elo", "elo"])
        cpy = MagicMock(return_value=False)
        g.get_conn().__enter__().open_sftp().listdir = a
        g.get_conn().__enter__().open_sftp().stat = stat
        g.get_conn().__enter__().open_sftp().open().__enter__().read = cpy

        dagbag = DagBag(".")
        dag = dagbag.get_dag(dag_id="upload_example")

        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 5)

        dagrun = dag.create_dagrun(
            state=DagRunState.RUNNING, run_id=RUN_ID, run_type=DagRunType.MANUAL,
        )

        ti = dagrun.get_task_instance(task_id="load")
        ti.task = dag.get_task(task_id="load")
        ti.run(ignore_all_deps=True, ignore_ti_state=True, test_mode=True)
        assert ti.state == TaskInstanceState.SUCCESS

        ti = dagrun.get_task_instance(task_id="register")
        ti.task = dag.get_task(task_id="register")
        ti.run(ignore_all_deps=True, ignore_ti_state=True, test_mode=True)
        assert ti.state == TaskInstanceState.SUCCESS
        # registration skipped:
        self.assertEqual(0, ti.xcom_pull(key="return_value"))

    @patch("datacat_integration.hooks.DataCatalogHook")
    def test_uploadflow(self, cat):
        mm = MagicMock()
        mm.create_entry.return_value = "4044"
        mm.base_url = "http://bar.foo"

        cat.return_value = mm

        dagbag = DagBag(".")
        dag = dagbag.get_dag(dag_id="upload_example")

        # make it fixture
        dagrun = dag.create_dagrun(
            state=DagRunState.RUNNING,
            run_id=RUN_ID,
            run_type=DagRunType.MANUAL,
            conf={"register": True},
        )

        ti = dagrun.get_task_instance(task_id="register")
        ti.task = dag.get_task(task_id="register")
        ti.run(ignore_all_deps=True, ignore_ti_state=True, test_mode=True)
        assert ti.state == TaskInstanceState.SUCCESS
        # registration skipped:
        self.assertEqual(
            mm.base_url + "/dataset/4044", ti.xcom_pull(key="return_value")
        )

    @patch("b2shareoperator.download_file")
    def test_taskflow_example_transform(self, dw):
        dagbag = DagBag(".")
        dag = dagbag.get_dag(dag_id="taskflow_example")

        dagrun = dag.create_dagrun(
            state=DagRunState.RUNNING, run_id=RUN_ID, run_type=DagRunType.MANUAL
        )

        ti = dagrun.get_task_instance(task_id="transform")
        ti.task = dag.get_task(task_id="transform")
        ti.task.op_kwargs = {"flist": {"bla": "/bla", "foo": "/bar/"}}

        dw.side_effect = ["a", "b"]

        ti.run(ignore_all_deps=True, ignore_ti_state=True, test_mode=True)
        assert ti.state == TaskInstanceState.SUCCESS
        print(ti.xcom_pull(key="return_value"))
        self.assertDictEqual({"bla": "a", "foo": "b"}, ti.xcom_pull(key="return_value"))

    @patch("utils.get_mlflow_client")
    @patch("decors.get_connection")
    @patch("utils.file_exist")
    def test_meta_transfer(self, fe, get, mlclient):
        fe.return_value = False
       
        get.get_conn().__enter__().open_sftp().return_value = None

        dagbag = DagBag(".")
        dag = dagbag.get_dag(dag_id="mlflow_upload_model")
        dagrun = dag.create_dagrun(
            state=DagRunState.RUNNING, run_id=RUN_ID, run_type=DagRunType.MANUAL, conf={"location": ".tmp",}
        )
        ti = dagrun.get_task_instance(task_id="download_artifacts")
        ti.task = dag.get_task(task_id="download_artifacts")
        #ti.task.op_kwargs = {"connection_id": 2929}

        ti.run(ignore_all_deps=True, ignore_ti_state=True, test_mode=True)
        ret = ti.xcom_pull(key="return_value")
        self.assertIn('temp_dir', ret)

        mlclient.return_value = MagicMock()

        ti = dagrun.get_task_instance(task_id="uploat_to_mlflow")
        ti.task = dag.get_task(task_id="uploat_to_mlflow")
        ti.task.op_kwargs = {"connection_id": 2929}

        ti.run(ignore_all_deps=True, ignore_ti_state=True, test_mode=True)
        ret = ti.xcom_pull(key="return_value")
        

    @patch('utils.get_webdav_client')
    @patch('utils.get_webdav_prefix')
    @patch('decors.get_connection')
    @patch('utils.walk_dir')
    def test_stageout(self, walk, g, get_prefix, getwebdav):
        getwebdav.return_value = MagicMock()
        get_prefix.retrun_value = '/prefix/'
        sft_client = MagicMock()

        g.get_conn().__enter__().open_sftp().return_value = sft_client
        walk.return_value = ['/home/foo/path/to/file.txt', '/home/foo/other/file.txt']

        dagbag = DagBag(".")

        dag = dagbag.get_dag(dag_id="webdav_stageout")
        self.assertIsNotNone(dag)
        print(dag.task_ids)
        self.assertEqual(len(dag.tasks), 1)

        dagrun = dag.create_dagrun(
            state=DagRunState.RUNNING, run_id=RUN_ID, run_type=DagRunType.MANUAL, 
            conf={"connection_id": "myconn","path":"/home/foo/"}
        )
        ti = dagrun.get_task_instance(task_id="copy")
        ti.task = dag.get_task(task_id="copy")
        ti.run(ignore_all_deps=True, ignore_ti_state=True, test_mode=True)
        ret = ti.xcom_pull(key="return_value")
