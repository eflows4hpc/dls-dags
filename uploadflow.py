import json
import os

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.connection import Connection
from airflow.operators.python import PythonOperator
import pendulum
from datacat_integration.connection import DataCatalogEntry
from datacat_integration.hooks import DataCatalogHook

from b2shareoperator import add_file, create_draft_record, get_community, submit_draft
from decors import get_connection, get_parameter, remove, setup
from utils import ssh2local_copy, clean_up_vaultid
from airflow.models.param import Param


def create_template(hrespo):
    for k,v in hrespo.items():
        try:
            # a hack to give possibility to add list and dicts in md field
            hrespo[k] = json.loads(v)
        except:
            pass 
    
    
    rec =  {
        "titles": [{"title": hrespo["title"]}],
        "descriptions": [
            {"description": hrespo["description"], "description_type": "Abstract"}
        ],
        "community": hrespo["community"], #"2d58eb08-af65-4cad-bd25-92f1a17d325b",
        "community_specific": {
            "fb46b5a7-bf69-40d4-943e-bd35b483c8ad": {
                "helmholtz centre": ["Forschungszentrum Jülich"]
            }
        },
        "open_access": hrespo["open_access"] == "True",
    }

    if 'creator_name' in hrespo:
        rec["creators"]= [{"creator_name": hrespo["creator_name"]}]

    if 'creators' in hrespo:
        rec['creators'] = hrespo['creators']

    if 'license' in hrespo:
        rec['license'] = hrespo['license']
    
    return rec


@dag(
    default_args={
        "owner": "airflow",
    },
    on_success_callback=clean_up_vaultid,
    schedule=None,
    start_date=pendulum.today(),
    tags=["example"],
    params={
        "source": Param("/tmp/", type="string"),
        "mid": Param("", description="id of the metadata template in datacat", type="string"),
        "register": Param(False, description="register to data cat?", type="boolean")
    }
)
def upload_example():
    @task()
    def load(connection_id, **kwargs):
        params = kwargs["params"]
        target = Variable.get("working_dir", default_var="/tmp/")
        source = params.get("source", "/tmp/")
        ssh_hook = get_connection(conn_id=connection_id, **kwargs)

        mappings = ssh2local_copy(ssh_hook=ssh_hook, source=source, target=target)
        return mappings

    @task()
    def upload(files: dict, **kwargs):
        connection = Connection.get_connection_from_secrets("default_b2share")
        # hate such hacks:
        server = "https://" + connection.host
        token = connection.extra_dejson["access_token"]

        params = kwargs["params"]
        mid = params["mid"]

        hook = DataCatalogHook()
        print("Connected to datacat via hook")
        entry = json.loads(hook.get_entry(datacat_type="storage_target", oid=mid))
        print("Got following metadata", entry)

        template = create_template(hrespo=entry["metadata"])
        print("Submission template: ", template)
        community = get_community(server=server, community_id=template["community"])
        if not community:
            print("Not existing community")
            return -1
        cid, required = community
        missing = [r for r in required if r not in template]
        if any(missing):
            print(
                f"Community {cid} required field {missing} are missing. This could pose some problems"
            )

        r = create_draft_record(server=server, token=token, record=template)
        if "id" in r:
            print(f"Draft record created {r['id']} --> {r['links']['self']}")
        else:
            print("Something went wrong with registration", r)
            return -1

        for [local, true_name] in files.items():
            print(f"Uploading {local} --> {true_name}")
            _ = add_file(record=r, fname=local, token=token, remote=true_name)
            # delete local
            os.unlink(local)

        print("Submitting record for pubication")
        submitted = submit_draft(record=r, token=token)
        print(f"Record created {submitted}")

        return {
            'link': submitted["links"]["publication"],
            'md': template
        }

    @task()
    def register(dt_object, **kwargs):
        reg = get_parameter(parameter="register", default=False, **kwargs)
        if not reg:
            print("Skipping registration as 'register' parameter is not set")
            return 0

        hook = DataCatalogHook()
        print("Connected to datacat via hook")
        object_url = dt_object['link']
        name = dt_object['md']['titles'][0]['title']

        entry = DataCatalogEntry(
            name=f"{name} {kwargs['run_id']}",
            url=object_url,
            metadata={"author": "DLS on behalf of eFlows", "access": "hook-based"},
        )
        try:
            r = hook.create_entry(datacat_type="dataset", entry=entry)
            print("Hook registration returned: ", r)
            return f"{hook.base_url}/dataset/{r}"
        except ConnectionError as e:
            print("Registration failed", e)
            return -1

    setup_task = PythonOperator(python_callable=setup, task_id="setup_connection")
    a_id = setup_task.output["return_value"]

    files = load(connection_id=a_id)
    uid = upload(files)

    en = PythonOperator(
        python_callable=remove, op_kwargs={"conn_id": a_id}, task_id="cleanup"
    )

    reg = register(dt_object=uid)

    setup_task >> files >> uid >> reg >> en


dag = upload_example()
