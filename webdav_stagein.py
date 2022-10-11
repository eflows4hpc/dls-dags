import os
from io import BytesIO

from airflow.decorators import dag, task
from airflow.models.connection import Connection
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from webdav3.client import Client

from decors import get_connection, remove, setup
from uploadflow import copy_streams

default_args = {
    'owner': 'airflow',
}


def get_webdav_client(connection):
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


def walk_webdav(client, prefix, path):
    for p in client.list(path, get_info=True):
        curr_name = p['path']
        if curr_name.startswith(prefix):
            curr_name = curr_name[len(prefix):]

        if curr_name == path:
            continue

        # will skip empty directories but we can live with that?
        if p['isdir']:
            yield from walk_webdav(client, prefix, curr_name)
            continue
        yield curr_name


@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['example'])
def webdav_stagein():

    @task(multiple_outputs=True)
    def get_prefix(**kwargs):
        conn = Connection.get_connection_from_secrets('b2drop_webdav')
        client = get_webdav_client(connection=conn)

        dirname = 'airflow-tests/'

        prefix = get_webdav_prefix(client=client, dirname=dirname)
        if not prefix:
            print('Unable to determine common prefix, quitting')
            return -1

        print(f"Determined common prefix: {prefix}")

        return prefix

    @task()
    def load(connection_id, prefix, **kwargs):
        dirname = 'airflow-tests/'
        print(f"Processing: {dirname}")
        params = kwargs['params']
        target = params.get('target', '/tmp/')

        print(f"Using {connection_id} connection")
        ssh_hook = get_connection(conn_id=connection_id, **kwargs)

        conn = Connection.get_connection_from_secrets('b2drop_webdav')
        client = get_webdav_client(connection=conn)

        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            # check dir?
            ssh_client.exec_command(command=f"mkdir -p {target}")
            for fname in walk_webdav(client=client, prefix=prefix, path=dirname):
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

    prefix = get_prefix()
    ucid = load(connection_id=a_id, prefix=prefix)

    en = PythonOperator(python_callable=remove, op_kwargs={
                        'conn_id': ucid}, task_id='cleanup')

    conn_id >> prefix >> ucid >> en


dag = webdav_stagein()
