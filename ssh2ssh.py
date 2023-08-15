import os

from airflow.decorators import dag, task
from airflow.models.param import Param

import pendulum

from decors import get_connection
from utils import copy_streams, RFSC, walk_dir
from utils import clean_up_vaultid


def get_prefixed_params(prefix, params):
    ret = {
        key[len(prefix):]: value for key, value in params.items() if key.startswith(prefix)
    }
    return ret

@dag(
    schedule=None,
    start_date=pendulum.today("UTC"),
    on_success_callback=clean_up_vaultid,
    params={
        "source_vault_id": Param("", type="string"),
        "source_host": Param("", type="string"),
        "source_port": Param(type="integer", default=22),
        "source_login": Param("", type="string"),
        "source_path": Param("/tmp/", type="string"),
        "target_vault_id": Param("", type="string"),
        "target_host": Param("", type="string"),
        "target_port": Param(type="integer", default=22),
        "target_login": Param("", type="string"),
        "target_path": Param("/tmp/", type="string"),
    },
)
def ssh2ssh():
    @task(multiple_outputs=True)
    def copy(**context):
        copied = {}
        params = context["params"]
        s_params = get_prefixed_params(prefix="source_", params=params)
        t_params = get_prefixed_params(prefix="target_", params=params)
        
        if (s_con_id:=s_params.pop('vault_id'))=="":
            s_con_id = s_params.get('connection_id', None)
    
        if (t_con_id:=t_params.pop('vault_id'))=="":
            t_con_id = t_params.get('connection_id', None)
        
        source_ssh_hook = get_connection(conn_id=s_con_id, params=s_params)
        target_ssh_hook = get_connection(conn_id=t_con_id, params=t_params)
        target_client = target_ssh_hook.get_conn().open_sftp()

        sftp_client = source_ssh_hook.get_conn().open_sftp()
        sclient = RFSC(sftp_client)

        mappings = list(walk_dir(client=sclient, path=s_params["path"], prefix=""))
        for fname in mappings:
            target_name = fname.replace(s_params["path"], t_params["path"])
            print("Processing", fname, "-->", target_name)

            di = os.path.dirname(target_name)
            print("Making direcotry", di)
            target_ssh_hook.get_conn().exec_command(command=f"mkdir -p {di}")
            # sometimes mkdir takes longer and is not sync?
            target_ssh_hook.get_conn().exec_command(command=f"touch {target_name}")

            with target_client.open(target_name, "wb") as tr:
                tr.set_pipelined(pipelined=True)
                with sftp_client.open(fname, "rb") as sr:
                    sr.set_pipelined(pipelined=True)
                    copy_streams(inp=sr, outp=tr)
            copied[fname] = target_name

        return copied

    copy()


dag = ssh2ssh()
