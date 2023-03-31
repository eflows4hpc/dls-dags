from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

def_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "testdag",
    default_args=def_args,
    description="simple testing dag",
    schedule_interval=None,
    start_date=pendulum.yesterday(),
) as dag:
    t1 = BashOperator(task_id="print_date", bash_command="date")
    t2 = BashOperator(task_id="do_noting", bash_command="sleep 1")

    t1 >> t2
