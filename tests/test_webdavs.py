import unittest
from unittest.mock import MagicMock

from webdav_stagein import LFSC, get_webdav_prefix, walk_dir


class TestWebDAV(unittest.TestCase):

    def test_prefix_extract(self):
        client = MagicMock()
        client.list = MagicMock(return_value=None)

        prefix = get_webdav_prefix(client=client, dirname='/foo/')
        self.assertIsNone(prefix)

    def test_pref2(self):
        client = MagicMock()
        client.list = MagicMock(return_value=[
            {
                'created': None,
                'name': None,
                'size': None,
                'modified': 'Tue, 27 Sep 2022 05:55:48 GMT',
                'etag': '"633290647c5a4"',
                'content_type': None,
                'isdir': True,
                'path': '/remote.php/webdav/airflow-tests/'
            },
            {
                'created': None,
                'name': None,
                'size': '20',
                'modified': 'Mon, 25 Jul 2022 08:13:04 GMT',
                'etag': '"29c62bc48f63cd57ba1463d2b7380ae0"',
                'content_type': 'application/octet-stream',
                'isdir': False,
                'path': '/remote.php/webdav/airflow-tests/file1'},
            {
                'created': None,
                'name': None,
                'size': '49',
                'modified': 'Mon, 26 Sep 2022 09:21:41 GMT',
                'etag': '"0302239984b26c628b875580fb8d8aac"',
                'content_type': 'text/plain',
                'isdir': False,
                'path': '/remote.php/webdav/airflow-tests/file1.txt'}])

        prefix = get_webdav_prefix(client=client, dirname='airflow-tests/')
        print(prefix)
        self.assertIsNotNone(prefix)
        self.assertEqual('/remote.php/webdav/', prefix)

    def test_walk(self):
        client = MagicMock()
        client.list = MagicMock(return_value=[
            {
                'isdir': True,
                'path': '/remote.php/webdav/airflow-tests/'},
            {
                'isdir': False,
                'path': '/remote.php/webdav/airflow-tests/file1'},
            {
                'isdir': False,
                'path': '/remote.php/webdav/airflow-tests/file1.txt'}
        ])

        flist = list(walk_dir(
            client=client, prefix='/remote.php/webdav/', path='airflow-tests/'))
        print('flist', flist)
        self.assertEqual(len(flist), 2)

    def test_walk_local(self):
        local_client = LFSC()
        lst = walk_dir(client=local_client, prefix='', path='/tmp/')
        self.assertIsNotNone(lst)
