import json
import os
import unittest
from collections import namedtuple
from unittest.mock import MagicMock, create_autospec, patch

from paramiko.sftp_client import SFTPClient

from utils import LFSC, RFSC, get_webdav_prefix, resolve_oid, walk_dir

Entry = namedtuple("Entry", ["st_mode", "filename"])


class TestWebDAV(unittest.TestCase):
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
        mm.get_entry.return_value = json.dumps(
            {"url": "http://foo.bar/", "metadata": {"path": "/foo/bar/"}}
        )
        hook.return_value = mm

        ret = resolve_oid(oid=777)
        self.assertEqual(ret, ("foo.bar", "/foo/bar/"))

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
        

