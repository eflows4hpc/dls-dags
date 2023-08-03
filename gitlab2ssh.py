import os
import pendulum

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from decors import get_connection, remove, setup
from utils import walk_dir

try:
    import gitlab
except ImportError:
    print("Unable to import gitlab library")


def get_gitlab_client(url):
    return gitlab.Gitlab(url)


def get_project(client, name):
    fn = [pr for pr in client.projects.list(iterator=True) if pr.name == name]
    if not fn:
        print("No project for given name found")
        return None

    return client.projects.get(fn[0].id)


class GitFSC:
    def __init__(self, client, **kwargs):
        self.client = client

    def list(self, path, get_info=True):
        if not get_info:
            return [
                el["name"]
                for el in self.client.repository_tree(path=path, get_all=True)
            ]
        return [
            {"path": el["path"], "isdir": el["type"] == "tree"}
            for el in self.client.repository_tree(path=path, get_all=True)
        ]


CHNK_SIZE = 1024 * 1000


@dag(
    default_args={"owner": "airflow",},
    schedule=None,
    start_date=pendulum.today("UTC"),
    tags=["gitlab", "ssh", "fesom2"],
    params={
        "vault_id": Param(default="", type="string"),
        "host": Param(default="", type="string"),
        "port": Param(default=22, type="integer"),
        "login": Param(default="", type="string"),
        "target": Param("/tmp/", type="string"),
        "gitlab_url": Param(default="https://gitlab.awi.de/", type="string"),
        "gitlab_repo": Param(default="fesom2_core2", type="string"),
    },
)
def git2ssh():
    @task
    def stream_vupload(connection_id, **kwargs):
        params = kwargs["params"]
        target = params.get("target", "/tmp/")
        url = params.get("gitlab_url")
        repo = params.get("gitlab_repo")

        client = get_gitlab_client(url=url)
        project = get_project(client=client, name=repo)
        gitf = GitFSC(client=project)

        print(f"Using ssh {connection_id} connection")
        ssh_hook = get_connection(conn_id=connection_id, **kwargs)

        cnt = 0
        with ssh_hook.get_conn() as ssh_client:
            sftp_client = ssh_client.open_sftp()
            # check dir?
            ssh_client.exec_command(command=f"mkdir -p {target}")
            for fname in walk_dir(client=gitf, prefix="", path=""):
                target_path = os.path.join(target, fname)
                target_dirname = os.path.dirname(target_path)
                print(f"Putting {fname} --> {target_path}")

                ssh_client.exec_command(command=f"mkdir -p {target_dirname}")
                ssh_client.exec_command(command=f"touch {target_path}")

                with sftp_client.open(target_path, "wb") as f:
                    f.set_pipelined(pipelined=True)
                    for chunk in project.files.raw(
                        file_path=fname, ref="HEAD", chunk_size=CHNK_SIZE, iterator=True
                    ):
                        content_to_write = memoryview(chunk)
                        f.write(content_to_write)
                    cnt+=1

        print(f"Copied: {cnt} files")
        return cnt

    setup_task = PythonOperator(python_callable=setup, task_id="setup_connection")
    a_id = setup_task.output["return_value"]
    cleanup_task = PythonOperator(
        python_callable=remove, op_kwargs={"conn_id": a_id}, task_id="cleanup"
    )

    setup_task >> stream_vupload(connection_id=a_id) >> cleanup_task


dag = git2ssh()
