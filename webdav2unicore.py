import os
import tempfile

import pendulum

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param

from utils import setup_webdav, walk_dir, get_unicore_client



@dag(
    default_args={"owner": "airflow",},
    schedule=None,
    start_date=pendulum.yesterday(),
    tags=["testing", "unicore"],
    params={
        "target": Param("/tmp/", type="string"),
        "oid": Param("", description="id of the dataset in datacat", type="string"),
        "working_dir": Param("/tmp/", type="string", description="local working dir"),
        "site_url": Param("", description="Unicore site url", type="string"),
        "user": Param("", description="Unicore user", type="string"),
        "password": Param("", description="Unicore password", type="string"),
    },
)
def webdav2unicore():
    @task
    #.virtualenv(requirements=["pyunicore"], system_site_packages=False)
    def load(**kwargs):
        params = kwargs["params"]
        target = params.get("target", "/tmp/")
        working_dir = Variable.get("working_dir", default_var="/tmp/")

        # setup webdav
        client, dirname, prefix = setup_webdav(params=params)

        print(f"WebDAV dirname {dirname}")
        abso, _ = os.path.split(dirname[:-1])

        # setup unicore
        uc_client = get_unicore_client(**params)
        uc_client.mkdir(target)
        cnt = 0
        for fname in walk_dir(client=client, prefix=prefix, path=dirname):
            print(f"Processing {fname}")
            target_path = os.path.join(target, fname[len(abso) + 1 :])
            target_dir = os.path.dirname(target_path)
            uc_client.mkdir(target_dir)

            with tempfile.NamedTemporaryFile(dir=working_dir) as tmp:
                print(f"Downloading {fname} --> {tmp.name}")
                client.download_file(fname, tmp.name)
                print(f"Uploading -> {target_path}")
                uc_client.upload(tmp.name, target_path)
                cnt += 1

        return f"Copied {cnt} files"

    load()


dag = webdav2unicore()
