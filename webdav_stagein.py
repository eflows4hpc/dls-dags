import json
import os
from io import BytesIO
from urllib.parse import urlparse

from airflow.decorators import dag, task
from airflow.models.connection import Connection
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datacat_integration.hooks import DataCatalogHook
from webdav3.client import Client

from decors import get_connection, remove, setup
from uploadflow import copy_streams
import stat

default_args = {
    'owner': 'airflow',
}


def get_webdav_client(webdav_connid):
    connection = Connection.get_connection_from_secrets(webdav_connid)
    options = {
        'webdav_hostname': f"https://{connection.host}{connection.schema}",
        'webdav_login': connection.login,
        'webdav_password': connection.get_password()
    }
    return Client(options)


def get_webdav_prefix(client, dirname):
    # not so efficient
    flist = client.list(dirname, get_info=True)
    if not flist:
        print(f"Empty directory {dirname}")
        return None

    got = [fname for fname in flist if fname['path'].endswith(dirname)]
    if not got:
        print('Could not determine the prefix... quiting')
        return None

    prefix = got[0]['path'][0:-len(dirname)]
    print(f"Determined common prefix: {prefix}")

    return prefix


def walk_dir(client, path, prefix):
    for p in client.list(path, get_info=True):
        curr_name = p['path']
        if curr_name.startswith(prefix):
            curr_name = curr_name[len(prefix):]

        if curr_name == path:
            continue

        # will skip empty directories but we can live with that?
        if p['isdir']:
            yield from walk_dir(client, curr_name, prefix)
            continue
        yield curr_name


class LFSC(object):
    def list(self, path, get_info=True):
        lst = [os.path.realpath(os.path.join(path, el)) for el in os.listdir(path)]
        if not get_info:
            return lst
        return [{'path': el, 'isdir':os.path.isdir(el) } for el in lst]

class RFSC(object):
    def __init__(self, client, **kwargs):
        self.client = client
        
    def list(self, path, get_info=True):
        if not get_info:
            return [el.filename for el in self.client.listdir_attr(path)]
        return [{'path': os.path.join(path, el.filename), 'isdir':stat.S_ISDIR(el.st_mode) } for el in self.client.listdir_attr(path)]

@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['wp6', 'UCIS4EQ'])
def webdav_stagein():

    @task()
    def load(connection_id, **kwargs):
        params = kwargs['params']
        target = params.get('target', '/tmp/')

        if 'oid' not in params:
            print("Missing object id in pipeline parameters. Please provide  datacat id")
            return -1
        oid = params['oid'] #oid = 'd011b12b-4eca-4482-8425-8c410b349519'

        hook = DataCatalogHook()
        try:
            entry = json.loads(hook.get_entry('dataset', oid))
            webdav_connid = urlparse(entry['url']).netloc
            print("Will be using webdav connection", webdav_connid)
            dirname = entry['metadata']['path']
            print(f"Processing webdav dir: {dirname}")
        except:
            print(f"No entry {oid} in data cat found. Or entry invalid")
            return -1

        client = get_webdav_client(webdav_connid=webdav_connid)
        prefix = get_webdav_prefix(client=client, dirname=dirname)
        if not prefix:
            print('Unable to determine common prefix, quitting')
            return -1
        print(f"Determined common prefix: {prefix}")

        print(f"Using ssh {connection_id} connection")
        ssh_hook = get_connection(conn_id=connection_id, **kwargs)

        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            # check dir?
            ssh_client.exec_command(command=f"mkdir -p {target}")
            for fname in walk_dir(client=client, prefix=prefix, path=dirname):
                print(f"Processing {fname}")
                target_path = os.path.join(target, fname)
                dirname = os.path.dirname(target_path)
                ssh_client.exec_command(command=f"mkdir -p {dirname}")
                # safety measure
                ssh_client.exec_command(command=f"touch {target_path}")

                res1 = client.resource(fname)
                buf = BytesIO()
                res1.write_to(buf)
                buf.seek(0)

                with sftp_client.open(target_path, 'wb') as f:
                    f.set_pipelined(pipelined=True)
                    print(f"Copying {fname}--> {target_path}")
                    copy_streams(inp=buf, outp=f)

        return connection_id

    conn_id = PythonOperator(python_callable=setup, task_id='setup_connection')
    a_id = conn_id.output['return_value']

    ucid = load(connection_id=a_id)

    en = PythonOperator(python_callable=remove, op_kwargs={
                        'conn_id': ucid}, task_id='cleanup')

    conn_id >> ucid >> en


dag = webdav_stagein()
