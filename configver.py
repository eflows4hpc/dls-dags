from airflow.decorators import dag, task
from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection
from airflow.providers.hashicorp.hooks.vault import VaultHook
from datacat_integration.hooks import DataCatalogHook
import pendulum

default_args = {
    "owner": "airflow",
}


def get_hook(clazz, conn_id):
    print("\n")
    print("-" * 10)
    print(f"Testing {clazz.__name__}@{conn_id}")

    try:
        if conn_id:
            hook = clazz(conn_id)
        else:
            hook = clazz()
    except AirflowNotFoundException as e:
        print(f"Connection {clazz.__name__} ({conn_id}) not present ")
        raise

    print(f"Hook {clazz.__name__} {conn_id} present")
    return 0


@dag(
    default_args=default_args,
    schedule=None,
    start_date=pendulum.yesterday(),
    tags=["tester"],
    catchup=False,
)
def verify_config():
    @task(trigger_rule="all_done")
    def test_vault_connection(**kwargs):
        print("Vault connection is required for credntials handling when accessing SSH")
        return get_hook(clazz=VaultHook, conn_id="my_vault")

    @task(trigger_rule="all_done")
    def test_datacat_connection(**kwargs):
        print("DataCat connection is required to handle inputs and register outputs")
        return get_hook(clazz=DataCatalogHook, conn_id=None)

    @task
    def test_b2share_connection(**kwargs):
        print("B2SHARE connection is required for stage-outs")
        return get_hook(
            clazz=Connection.get_connection_from_secrets, conn_id="default_b2share"
        )

    test_vault_connection()
    test_datacat_connection()
    test_b2share_connection()


dag = verify_config()
