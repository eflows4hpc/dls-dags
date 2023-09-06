import os
from io import BytesIO
import pendulum

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

from decors import get_connection, get_parameter, remove, setup
from utils import (
    copy_streams,
    resolve_oid,
    get_webdav_client,
    get_webdav_prefix,
    walk_dir,
    file_exist,
    clean_up_vaultid
)

@dag(
    default_args={
    "owner": "airflow",
    },
    schedule=None,
    start_date=pendulum.yesterday(),
    on_success_callback=clean_up_vaultid,
    tags=["wp6", "UCIS4EQ"],
    params={
        "vault_id": Param(default="", type="string"),
        "host": Param(default="", type="string"),
        "port": Param(type="integer", default=22),
        "login": Param(default="", type="string"),
        "target": Param("/tmp/", type="string"),
        "force": Param(True, type="boolean"),
        "oid": Param("", description="id of the dataset in datacat", type="string"),
    },
)
def webdav_stagein():
    @task()
    def load(connection_id, **kwargs):
        params = kwargs["params"]
        target = params.get("target", "/tmp/")
        force = params.get("force", True)

        oid = get_parameter(parameter="oid", default=False, **kwargs)
        if not oid:
            print(
                "Missing object id (oid) in pipeline parameters. Please provide  datacat id"
            )
            return connection_id

        webdav_connid, dirname = resolve_oid(oid=oid)
        if webdav_connid == -1:
            return connection_id
        
        # fixing dirname
        if dirname.startswith("/"):
            dirname = dirname[1:]
        if dirname[-1] != "/":
            dirname = dirname + "/"

        abso, _ = os.path.split(dirname[:-1])
        

        client = get_webdav_client(webdav_connid=webdav_connid)
        prefix = get_webdav_prefix(client=client, dirname=dirname)
        if not prefix:
            print("Unable to determine common prefix, quitting")
            return connection_id

        print(f"Determined common prefix: {prefix}")

        print(f"Using ssh {connection_id} connection")
        ssh_hook = get_connection(conn_id=connection_id, **kwargs)
        cnt=0

        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            # check dir?
            ssh_client.exec_command(command=f"mkdir -p {target}")
            for fname in walk_dir(client=client, prefix=prefix, path=dirname):
                # make it relative
                target_path = os.path.join(target, fname[len(abso) + 1 :])

                target_dirname = os.path.dirname(target_path)
                
                size = file_exist(sftp=sftp_client, name=target_path)
                if size > 0:
                    print(f"File {target_path} exists and has {size} bytes")
                    if force is not True:
                        continue
                    print("Forcing overwrite")
                # safety measure
                ssh_client.exec_command(command=f"mkdir -p {target_dirname}")
                ssh_client.exec_command(command=f"touch {target_path}")

                res1 = client.resource(fname)
                buf = BytesIO()
                res1.write_to(buf)
                buf.seek(0)

                with sftp_client.open(target_path, "wb") as f:
                    f.set_pipelined(pipelined=True)
                    print(f"Copying {fname}--> {target_path}")
                    copy_streams(inp=buf, outp=f)
                    cnt+=1

        print(f"Copied {cnt} files")
        return connection_id

    conn_id = PythonOperator(python_callable=setup, task_id="setup_connection")
    a_id = conn_id.output["return_value"]

    ucid = load(connection_id=a_id)

    en = PythonOperator(
        python_callable=remove, op_kwargs={"conn_id": ucid}, task_id="cleanup"
    )

    conn_id >> ucid >> en


dag = webdav_stagein()
