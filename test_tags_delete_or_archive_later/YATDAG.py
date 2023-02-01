from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

def_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "Yet_Another_Test_DAG",
    default_args=def_args,
    description="another simple testing dag - with a new modification for testing",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
) as dag:
    t11 = BashOperator(task_id="print_date1", bash_command="date")
    t12 = BashOperator(task_id="do_noting1", bash_command="sleep 5")
    t21 = BashOperator(task_id="print_date2", bash_command="date")
    t22 = BashOperator(task_id="do_noting2", bash_command="sleep 5")
    t31 = BashOperator(task_id="print_date3", bash_command="date")
    t32 = BashOperator(task_id="do_noting3", bash_command="sleep 5")

    t11 >> t12
    t21 >> t22
    t31 >> t32
