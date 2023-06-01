import unittest

from unittest.mock import MagicMock, patch
from utils import mkdir_rec

called = 0


class TestUtils(unittest.TestCase):
    def test_mkdirrec(self):
        client = MagicMock()
        client.check = lambda x: len(x) < 5

        def mymkdir(path):
            global called
            called = called + 1
            print("Making dir:", path)

        client.mkdir = mymkdir
        mkdir_rec(client=client, path="/tmp/foo/bar/woo/car/woow")
        self.assertEqual(called, 5)
