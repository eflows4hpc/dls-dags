
import os


from airflow.decorators import dag, task
from airflow.models.connection import Connection
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.dates import days_ago
import boto3
import botocore


default_args = {
    'owner': 'airflow',
}


def get_client(s3_connid='s3conn'):
    connection = Connection.get_connection_from_secrets(s3_connid)
    session = boto3.session.Session()
    s3client = session.client(
        service_name='s3',
        endpoint_url=f"https://{connection.host}:8080",
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.get_password(),
        config=botocore.client.Config(signature_version='s3'),
    )

    return s3client


def get_object_list(s3client, bucket_name):
    try:
        lst = s3client.list_objects(Bucket=bucket_name)
        flist = [a['Key'] for a in lst['Contents']]
        return flist
    except:
        print(f"No such bucket {bucket_name}")

    buckets = s3client.list_buckets()
    print("Available buckets: ", [bucket['Name'] for bucket in buckets['Buckets']])
    return -1


@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['wp6', 's3'])
def as3transfer():
    @task(multiple_outputs=True)
    def get_flist(**kwargs):
        s3client = get_client()

        params = kwargs['params']
        bucket_name = params.get('bucket', 'eFlows4HCP')
        return get_object_list(s3client=s3client, bucket_name=bucket_name)


    @task()
    def stream_upload(flist: list, **kwargs):
        params = kwargs['params']
        target = params.get('target', '/tmp/')
        connection_id = params.get('connection', 'default_ssh')
        ssh_hook = SSHHook(ssh_conn_id=connection_id)

        s3client = get_client()

        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            for fname in flist:
                print(f"Processing: {fname}")
                with sftp_client.open(os.path.join(target, fname), 'wb') as f:
                    s3client.download_fileobj('eFlows4HCP', fname, f)

    flist = get_flist()
    stream_upload(flist)


dag = as3transfer()
