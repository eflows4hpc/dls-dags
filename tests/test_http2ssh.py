
import unittest
from unittest.mock import MagicMock, patch
from image_transfer import http2ssh

class TestHTTP(unittest.TestCase):

    @patch('image_transfer.file_exist')
    def test_copy_force(self, exists):
        exists.return_value = 661

        my_sftp = MagicMock()
        my_client = MagicMock()
        exec = MagicMock()
        my_client.open_sftp.return_value = my_sftp
        my_client.exec_command = exec
        
        r = http2ssh(url='foo.bar', ssh_client=my_client, remote_name='/foo/bar/', force=False)
        # file exists and no force:
        self.assertEqual(r, 0)
        exec.assert_not_called()

        exists.return_value = 0

        my_sftp = MagicMock()
        my_client = MagicMock()
        exec = MagicMock(side_effect=Exception('Boom!'))
        my_client.open_sftp.return_value = my_sftp
        my_client.exec_command = exec

        with self.assertRaises(Exception) as cm:
            r = http2ssh(url='foo.bar', ssh_client=my_client, remote_name='/foo/bar/', force=False)
        
           
        self.assertEqual(str(cm.exception), 'Boom!')
        
        # file does not exist and no force:
        #self.assertEqual(r, 0)
        exec.assert_called()

    @patch('image_transfer.requests.get')
    @patch('image_transfer.file_exist')
    def test_actual_cpy(self, exists, get):
        exists.return_value = 661
        my_sftp = MagicMock()
        wrt = MagicMock(return_value=2)
        my_sftp.open().__enter__().write = wrt
        my_client = MagicMock()
        exec = MagicMock()
        my_client.open_sftp.return_value = my_sftp
        my_client.exec_command = exec


        get().__enter__().raw.read = MagicMock(side_effect=[b'blabla', None])
        get().__enter__().headers.get = MagicMock(return_value=6)
        r = http2ssh(url='foo.bar', ssh_client=my_client, remote_name='/goo/bar', force=True)
        self.assertEqual(r, 0)
        exec.assert_called()
        wrt.assert_called_once_with(memoryview(b'blabla'))

    @patch('image_transfer.requests.get')
    @patch('image_transfer.file_exist')
    def test_missed_cpy(self, exists, get):
        exists.return_value = 661
        my_sftp = MagicMock()
        wrt = MagicMock(return_value=2)
        my_sftp.open().__enter__().write = wrt
        my_client = MagicMock()
        exec = MagicMock()
        my_client.open_sftp.return_value = my_sftp
        my_client.exec_command = exec


        get().__enter__().raw.read = MagicMock(side_effect=[b'blabla', None])
        get().__enter__().headers.get = MagicMock(return_value=699)
        r = http2ssh(url='foo.bar', ssh_client=my_client, remote_name='/goo/bar', force=True)
        self.assertEqual(r, -1)
        exec.assert_called()
        wrt.assert_called_once_with(memoryview(b'blabla'))
