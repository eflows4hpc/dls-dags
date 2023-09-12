import os
import tempfile
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.models.param import Param

import pendulum

from decors import get_connection, get_parameter
from utils import (
    RFSC,
    walk_dir,
    mkdir_rec,
    resolve_oid,
    get_webdav_client,
    get_webdav_prefix,
    walk_dir,
    clean_up_vaultid,
    file_exist
)


@dag(
    schedule=None,
    start_date=pendulum.today("UTC"),
    on_success_callback=clean_up_vaultid,
    params={
        "vault_id": Param(default="", type="string"),
        "host": Param(default="", type="string"),
        "port": Param(type="integer", default=22),
        "login": Param(default="", type="string"),
        "path": Param(default="/tmp/", type="string"),
        "oid": Param(default="", type="string"),
        "verify_webdav_cert": Param(default=True, type="boolean")
    },
)
def webdav_stageout():
    @task(multiple_outputs=True)
    def copy(**context):
        oid = get_parameter(parameter="oid", default=False, **context)
        if not oid:
            print(
                "Missing target storage id (oid) in pipeline parameters. Please provide  datacat id"
            )

        webdav_connid, dirname = resolve_oid(oid=oid, type="storage_target")
        
        
        client = get_webdav_client(webdav_connid=webdav_connid)
        client.verify = get_parameter(parameter="verify_webdav_cert", default=True, **context)
        
        prefix = get_webdav_prefix(client=client, dirname=dirname)
        if not prefix:
            print("Unable to determine common prefix")

        print(f"Determined common prefix: {prefix}")

        params = context["params"]
        if (s_con_id := params.pop("vault_id")) == "":
            s_con_id = params.get("connection_id", None)
        else:
            s_con_id=f"vault_{s_con_id}"

        source_ssh_hook = get_connection(conn_id=s_con_id, params=params)
        sftp_client = source_ssh_hook.get_conn().open_sftp()
        sclient = RFSC(sftp_client)

        working_dir = Variable.get("working_dir", default_var="/tmp/")

        copied = {}

        try:
            mappings = list(walk_dir(client=sclient, path=params["path"], prefix=""))
        except IOError:
            # single file?
            if file_exist(sftp=sftp_client, name=params['path']):
                mappings = [params['path']]
                params['path'] = os.path.dirname(params['path'])
            else:
                print("Invalid path or file name")
                return -1


        for fname in mappings:
            with tempfile.NamedTemporaryFile(dir=working_dir) as tmp:
                print(f"Getting: {fname}->{tmp.name}")
                sftp_client.get(remotepath=fname, localpath=tmp.name)

                directory, fl = os.path.split(fname[len(params["path"])+1:])
                remote_path = os.path.join(dirname, directory)
                mkdir_rec(client=client, path=remote_path)

                print(f"Uploading {tmp.name}->{os.path.join(remote_path, fl)}")
                client.upload_sync(
                    remote_path=os.path.join(remote_path, fl), local_path=tmp.name,
                )

            copied[fname] = os.path.join(remote_path, fl)

        return copied

    copy()


dag = webdav_stageout()
