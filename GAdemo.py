from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import pendulum

def_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def train_model():
    print("Will start model training")


with DAG(
    "GAtest",
    default_args=def_args,
    description="testing GA",
    schedule=timedelta(days=1),
    start_date=pendulum.today('UTC'),
) as dag:
    s1 = FileSensor(task_id="file_sensor", filepath="/work/afile.txt")
    t1 = BashOperator(task_id="move_data", bash_command="date")
    t2 = PythonOperator(task_id="train_model", python_callable=train_model)
    t3 = BashOperator(task_id="eval_model", bash_command='echo "evaluating"')

    s1 >> t1 >> t2
    t2 >> t3
