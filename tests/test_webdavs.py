import json
import os
import unittest
from collections import namedtuple
from unittest.mock import MagicMock, create_autospec, patch

from paramiko.sftp_client import SFTPClient

from airflow import settings
from airflow.models import DagBag
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from utils import LFSC, RFSC, get_webdav_prefix, resolve_oid, walk_dir

Entry = namedtuple("Entry", ["st_mode", "filename"])

RUN_ID = "webDAVRun"

class TestWebDAV(unittest.TestCase):
    def tearDown(self) -> None:
        session = settings.Session()
        _ = session.execute(f"delete from dag_run where run_id = '{RUN_ID}'")
        session.commit()

    def test_prefix_extract(self):
        client = MagicMock()
        client.list = MagicMock(return_value=None)

        prefix = get_webdav_prefix(client=client, dirname="/foo/")
        self.assertIsNone(prefix)

    def test_pref2(self):
        client = MagicMock()
        client.list = MagicMock(
            return_value=[
                {
                    "created": None,
                    "name": None,
                    "size": None,
                    "modified": "Tue, 27 Sep 2022 05:55:48 GMT",
                    "etag": '"633290647c5a4"',
                    "content_type": None,
                    "isdir": True,
                    "path": "/remote.php/webdav/airflow-tests/",
                },
                {
                    "created": None,
                    "name": None,
                    "size": "20",
                    "modified": "Mon, 25 Jul 2022 08:13:04 GMT",
                    "etag": '"29c62bc48f63cd57ba1463d2b7380ae0"',
                    "content_type": "application/octet-stream",
                    "isdir": False,
                    "path": "/remote.php/webdav/airflow-tests/file1",
                },
                {
                    "created": None,
                    "name": None,
                    "size": "49",
                    "modified": "Mon, 26 Sep 2022 09:21:41 GMT",
                    "etag": '"0302239984b26c628b875580fb8d8aac"',
                    "content_type": "text/plain",
                    "isdir": False,
                    "path": "/remote.php/webdav/airflow-tests/file1.txt",
                },
            ]
        )

        prefix = get_webdav_prefix(client=client, dirname="airflow-tests/")
        print(prefix)
        self.assertIsNotNone(prefix)
        self.assertEqual("/remote.php/webdav/", prefix)

        prefix = get_webdav_prefix(client=client, dirname="foo-bar/")
        self.assertIsNone(prefix)

        client.list.return_value = None
        prefix = get_webdav_prefix(client=client, dirname="airflow-tests/")
        self.assertIsNone(prefix)

    def test_walk(self):
        client = MagicMock()
        client.list = MagicMock(
            return_value=[
                {"isdir": True, "path": "/remote.php/webdav/airflow-tests/"},
                {"isdir": False, "path": "/remote.php/webdav/airflow-tests/file1"},
                {"isdir": False, "path": "/remote.php/webdav/airflow-tests/file1.txt"},
            ]
        )

        flist = list(
            walk_dir(client=client, prefix="/remote.php/webdav/", path="airflow-tests/")
        )
        print("flist", flist)
        self.assertEqual(len(flist), 2)

    def test_walk_local(self):
        local_client = LFSC()
        lst = local_client.list(path=".", get_info=True)
        self.assertTrue(len(lst) > 0)

        lst = walk_dir(client=local_client, prefix="", path="/tmp/")
        self.assertIsNotNone(lst)

    def test_walk_file(self):
        local_client = LFSC()
        with self.assertRaises(NotADirectoryError) as context:
            lst = local_client.list(path='./tests/test_webdavs.py', get_info=True)
    
    def test_walk_remote(self):
        sftp_client = create_autospec(SFTPClient)
        sftp_client.listdir_attr = MagicMock(
            side_effect=[
                [
                    Entry(st_mode=0o40700, filename="afile"),
                    Entry(st_mode=1, filename="foo"),
                ],
                [
                    Entry(st_mode=0, filename="barafile"),
                    Entry(st_mode=1, filename="barfoo"),
                ],
            ]
        )

        remote_client = RFSC(sftp_client)
        lst = list(walk_dir(client=remote_client, prefix="", path="/tmp/"))
        print(lst)
        self.assertEqual(len(lst), 3)
        self.assertListEqual(
            ["/tmp/afile/barafile", "/tmp/afile/barfoo", "/tmp/foo"], lst
        )

    @patch("utils.DataCatalogHook")
    def test_resolve(self, hook):

        ret = resolve_oid(oid=777)
        self.assertEqual(ret, ("default_webdav", "dls/"))

        mm = MagicMock()
        mm.get_entry.side_effect = [
            json.dumps({"url": "http://foo.bar/", "metadata": {"path": "/foo/bar/"}}),
            json.dumps({"url": "http://jorge@foo.bar/", "metadata": {"path": "/foo/barzz/"}})
        ]
        hook.return_value = mm

        ret = resolve_oid(oid=777)
        self.assertEqual(ret, ("foo.bar", "/foo/bar/"))

        ret = resolve_oid(oid=999)
        self.assertEqual(ret, ("jorge@foo.bar", "/foo/barzz/"))

    def test_makerelative(self):
        flist = ["eFlows4HPC/WPs/WP1/Testing_data/PTF/Regions/earlyEst/2000_1025_creta_test.json",
                 "eFlows4HPC/WPs/WP1/Testing_data/PTF/Regions/med-tsumaps-python/FocMech_PreProc/MeanProb_BS4_FocMech_Reg015.npy",
                 "eFlows4HPC/WPs/WP1/Testing_data/PTF/Regions/med-tsumaps-python/FocMech_PreProc/MeanProb_BS4_FocMech_Reg038.npy",
                 "eFlows4HPC/WPs/WP1/Testing_data/PTF/Regions/med-tsumaps-python/ScenarioList/ScenarioListBS_Reg042_W01553N6261E00800N4301.npy.npz"]
        
        dirname = "eFlows4HPC/WPs/WP1/Testing_data/PTF/Regions/"
        abso, rel = os.path.split(dirname[:-1])

        print('--'*20)
        target = "/gpfs/projects/bsc44/test_stage_in/"
        for fname in flist:
            tar = os.path.join(target, fname[len(abso)+1:])
            self.assertTrue(tar.startswith(target))
            self.assertTrue('Regions' in tar)
            self.assertFalse('WP1' in tar)
            print(fname, tar)
        print('-'*20)
        
    @patch("utils.resolve_oid")
    @patch("utils.get_webdav_client")
    @patch("utils.get_webdav_prefix")
    @patch('decors.get_connection')
    @patch('utils.walk_dir')
    @patch('utils.file_exist')
    @patch('utils.copy_streams')
    def test_runs(self, cpy, f_exist, walker, get_conn, get_prefix, get_webdav, resolver):
        resolver.return_value = ('webdav_conn', 'tmp/foo/bar')
        get_webdav.return_value = MagicMock()
        get_prefix.return_value ='foo'

        sft_client = MagicMock()
        
        get_conn.get_conn().__enter__().open_sftp().return_value = sft_client
        walker.return_value = ['/home/foo/path/to/file.txt', '/home/foo/other/file.txt']
        f_exist.return_value = 77


        dagbag = DagBag(".", include_examples=False)
        dag = dagbag.get_dag(dag_id="webdav_stagein")
        self.assertIsNotNone(dag)

        dagrun = dag.create_dagrun(
            state=DagRunState.RUNNING,
            run_id=RUN_ID,
            run_type=DagRunType.MANUAL,
            conf={
                "vault_id": "foo",
                "host": "foo_host",
                "login": "foo",
                "target": "/zmpt/",
                "force": True,
                "oid": "foo1",
            },
        )

        ti = dagrun.get_task_instance(task_id="load")
        ti.task = dag.get_task(task_id="load")

        ti.run(ignore_all_deps=True, ignore_ti_state=True, test_mode=True)
        self.assertEqual(ti.state, TaskInstanceState.SUCCESS)

        cpy.assert_called()


    @patch("utils.resolve_oid")
    @patch("utils.get_webdav_client")
    @patch("utils.get_webdav_prefix")
    @patch('decors.get_connection')
    @patch('utils.walk_dir')
    @patch('utils.file_exist')
    @patch('utils.copy_streams')
    def test_noforce_runs(self, cpy, f_exist, walker, get_conn, get_prefix, get_webdav, resolver):
        resolver.return_value = ('webdav_conn', 'tmp/foo/bar')
        get_webdav.return_value = MagicMock()
        get_prefix.return_value ='foo'

        sft_client = MagicMock()
        
        get_conn.get_conn().__enter__().open_sftp().return_value = sft_client
        walker.return_value = ['/home/foo/path/to/file.txt', '/home/foo/other/file.txt']
        f_exist.return_value = 77


        dagbag = DagBag(".", include_examples=False)
        dag = dagbag.get_dag(dag_id="webdav_stagein")
        self.assertIsNotNone(dag)

        dagrun = dag.create_dagrun(
            state=DagRunState.RUNNING,
            run_id=RUN_ID,
            run_type=DagRunType.MANUAL,
            conf={
                "vault_id": "foo",
                "host": "foo_host",
                "login": "foo",
                "target": "/zmpt/",
                "force": False,
                "oid": "foo1",
            },
        )

        ti = dagrun.get_task_instance(task_id="load")
        ti.task = dag.get_task(task_id="load")

        ti.run(ignore_all_deps=True, ignore_ti_state=True, test_mode=True)
        self.assertEqual(ti.state, TaskInstanceState.SUCCESS)
        # non forcing        
        cpy.assert_not_called()


    @patch('utils.get_webdav_client')
    @patch('utils.get_webdav_prefix')
    @patch('decors.get_connection')
    @patch('utils.walk_dir')
    @patch('utils.is_dir', return_value=False)
    def test_stageout(self, is_dir, walk, g, get_prefix, getwebdav):
        getwebdav.return_value = MagicMock()
        get_prefix.retrun_value = '/prefix/'
        sft_client = MagicMock()

        g.get_conn().__enter__().open_sftp().return_value = sft_client
        tbl=['/home/foo/path/to/file.txt', '/home/foo/other/file.txt']

        walk.return_value = tbl
        #is_dir.return_value 
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

        self.assertEqual(len(tbl), len(ret))
        self.assertListEqual(tbl, list(ret.keys()))


    @patch('utils.get_webdav_client')
    @patch('utils.get_webdav_prefix')
    @patch('decors.get_connection')
    @patch('utils.walk_dir', side_effect=IOError)
    @patch('utils.is_dir', return_value=True)
    def test_stageout_file(self, isdir, walk, g, get_prefix, getwebdav):
        getwebdav.return_value = MagicMock()
        get_prefix.retrun_value = '/prefix/'
        sft_client = MagicMock()

        g.get_conn().__enter__().open_sftp().return_value = sft_client
        tbl=['/home/foo/path/to/file.txt', '/home/foo/other/file.txt']

        dagbag = DagBag(".")

        dag = dagbag.get_dag(dag_id="webdav_stageout")
        self.assertIsNotNone(dag)
        print(dag.task_ids)
        self.assertEqual(len(dag.tasks), 1)

        dagrun = dag.create_dagrun(
            state=DagRunState.RUNNING, run_id=RUN_ID, run_type=DagRunType.MANUAL, 
            conf={"connection_id": "myconn","path":"/home/foo/bar.exe"}
        )
        ti = dagrun.get_task_instance(task_id="copy")
        ti.task = dag.get_task(task_id="copy")
        ti.run(ignore_all_deps=True, ignore_ti_state=True, test_mode=True)
        ret = ti.xcom_pull(key="return_value")

        self.assertEqual(1, len(ret))
        self.assertListEqual(["/home/foo/bar.exe"], list(ret.keys()))

