import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from utils import copy_streams, ssh2local_copy
from ssh2ssh import get_prefixed_params


class TestSSH(unittest.TestCase):
    def tearDown(self) -> None:
        super().tearDown()
        try:
            os.unlink("tmpA")
            os.unlink("tmpB")
        except OSError as err:
            print("Wasnt there probably", err)

    @patch("utils.tempfile.mktemp")
    def test_copy_files(self, tmp):
        tmp.side_effect = ["tmpA", "tmpB"]

        my_hook = MagicMock()
        a = MagicMock()
        a.return_value = ["a", "c"]
        stat = MagicMock(side_effect=["elo", "elo"])
        cpy = MagicMock(return_value=False)
        my_hook.get_conn().__enter__().open_sftp().listdir = a
        my_hook.get_conn().__enter__().open_sftp().stat = stat
        my_hook.get_conn().__enter__().open_sftp().open().__enter__().read = cpy

        mapps = ssh2local_copy(ssh_hook=my_hook, source="srcZ", target="trg")
        my_hook.get_conn.assert_any_call()
        a.assert_called_once_with(path="srcZ")
        cpy.assert_called()

        print(mapps)
        self.assertEqual(len(mapps), 2)

    @patch("utils.tempfile.mktemp")
    def test_skipdir_files(self, tmp):
        tmp.side_effect = ["tmpA", "tmpB"]

        my_hook = MagicMock()
        a = MagicMock()
        a.return_value = ["a", "c"]
        stat = MagicMock(side_effect=["elo", "d elo"])
        cpy = MagicMock(return_value=False)
        my_hook.get_conn().__enter__().open_sftp().listdir = a
        my_hook.get_conn().__enter__().open_sftp().stat = stat
        my_hook.get_conn().__enter__().open_sftp().open().__enter__().read = cpy

        mapps = ssh2local_copy(ssh_hook=my_hook, source="srcZ", target="trg")
        my_hook.get_conn.assert_any_call()
        a.assert_called_once_with(path="srcZ")
        cpy.assert_called()

        print(mapps)

        self.assertEqual(len(mapps), 1)

    def test_copy_streams(self):
        """
        def copy_streams(input, output):
        """
        with tempfile.TemporaryDirectory() as dir:
            text = "Some input text"
            input_name = os.path.join(dir, "input.txt")
            output_name = os.path.join(dir, "output")
            with open(input_name, "w") as fln:
                fln.write(text)

            with open(input_name, "rb") as inp:
                with open(output_name, "wb") as outp:
                    copy_streams(inp=inp, outp=outp)

            with open(output_name, "r") as f:
                txt = f.read()
                print("Read following: ", txt)

                self.assertEqual(text, txt)

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
        conn_id, cparams = get_prefixed_params(prefix='source', params=params)
        self.assertEqual('vault_foo_id', conn_id)
        self.assertEqual(cparams['host'], 'foo_host')
        self.assertEqual(cparams['port'], 44)

        conn_id, cparams = get_prefixed_params(prefix='target', params=params)
        self.assertEqual('vault_bar_id', conn_id)
        self.assertEqual(cparams['host'], 'bar_host')
        self.assertEqual(cparams['port'], 33)
