import os
from io import BytesIO

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
import pendulum

from decors import get_connection, get_parameter, remove, setup
from utils import (
    copy_streams,
    resolve_oid,
    get_webdav_client,
    get_webdav_prefix,
    walk_dir,
)

default_args = {
    "owner": "airflow",
}


@dag(
    default_args=default_args,
    schedule=None,
    start_date=pendulum.yesterday(),
    tags=["wp6", "UCIS4EQ"],
)
def webdav_stagein():
    @task()
    def load(connection_id, **kwargs):
        params = kwargs["params"]
        target = params.get("target", "/tmp/")

        oid = get_parameter(parameter="oid", default=False, **kwargs)
        if not oid:
            print(
                "Missing object id (oid) in pipeline parameters. Please provide  datacat id"
            )
            return -1

        webdav_connid, dirname = resolve_oid(oid=oid)
        if webdav_connid == -1:
            return -1

        client = get_webdav_client(webdav_connid=webdav_connid)
        prefix = get_webdav_prefix(client=client, dirname=dirname)
        if not prefix:
            print("Unable to determine common prefix, quitting")
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

                with sftp_client.open(target_path, "wb") as f:
                    f.set_pipelined(pipelined=True)
                    print(f"Copying {fname}--> {target_path}")
                    copy_streams(inp=buf, outp=f)

        return connection_id

    conn_id = PythonOperator(python_callable=setup, task_id="setup_connection")
    a_id = conn_id.output["return_value"]

    ucid = load(connection_id=a_id)

    en = PythonOperator(
        python_callable=remove, op_kwargs={"conn_id": ucid}, task_id="cleanup"
    )

    conn_id >> ucid >> en


dag = webdav_stagein()
