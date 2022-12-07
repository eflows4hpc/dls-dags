import unittest
from unittest.mock import create_autospec, Mock, patch
from airflow.models.connection import Connection
from as3transfer import get_client, get_object_list

class TestS3(unittest.TestCase):

    def test_obj_list(self):
        s3client = Mock()

        s3client.list_objects.return_value = {
            'ResponseMetadata': {'RequestId': 'tx8881-006389c717',
                                 'HostId': 'tx8881-006389c717',
                                 'HTTPStatusCode': 200,
                                 'HTTPHeaders': {'date': 'Fri, 02 Dec 2022 09:36:23 GMT'},
                                 'IsTruncated': False,
                                 'Marker': ''},
            'Contents': [
                {'Key': 'test-file',
                 'LastModified': 'bbb',
                 'ETag': '"972c8e9cf77f26237d714b0c32a1af2d-120"',
                 'Size': 1000000000,
                 'StorageClass': 'STANDARD',
                 'Owner': {'DisplayName': 'foo:bar', 'ID': 'foo:bar'}
                 },
                {'Key': 'test-file2',
                 'LastModified': 'bbb',
                 'ETag': '"272c8e9cf77f26237d714b0c32a1af2d-120"',
                 'Size': 100,
                 'StorageClass': 'STANDARD',
                 'Owner': {'DisplayName': 'foo:bar', 'ID': 'foo:bar'}
                 }
            ],
            'Name': 'eFlows4HCP',
            'Prefix': '',
            'MaxKeys': 1000,
            'EncodingType': 'url'
        }
        lst = get_object_list(s3client=s3client, bucket_name='foo')
        self.assertEqual(len(lst), 2)
        self.assertTrue(s3client.list_objects.called)

    def test_noobj_list(self):
        s3client = Mock()
        s3client.list_objects = Mock(side_effect=Exception('Boom!'))
        s3client.list_buckets.return_value = {
            'ResponseMetadata': {
                'RequestId': 'tx888-00638db1e8',
                'HostId': 'tx888-00638db1e8',
                'HTTPStatusCode': 200,
                'HTTPHeaders': {
                    'date': 'Mon, 05 Dec 2022 08:55:04 GMT',
                },
                'RetryAttempts': 0},
            'Buckets': [
                {
                    'Name': 'eFlows4HCP',
                    'CreationDate': 7777
                },
                {
                    'Name': 'wol',
                    'CreationDate': 8888
                }],
            'Owner': {'DisplayName': 'foo:bar', 'ID': 'foo:bar'}
        }
        lst = get_object_list(s3client=s3client, bucket_name='bar')
        self.assertEqual(lst, -1)

    @patch('as3transfer.Connection')
    def test_client(self, conn):
        a_connection = create_autospec(Connection)
        a_connection.host = 'foo'
        a_connection.login = 'bar'
        a_connection.get_password.return_value = 'foobarZ'

        conn.get_connection_from_secrets.return_value = a_connection
        client = get_client()
        self.assertIsNotNone(client)
