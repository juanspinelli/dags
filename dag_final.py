import os
import sys
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
scriptpath = "./boto/"
sys.path.append(os.path.abspath(scriptpath))
import create_bucket_dos

default_args = {
    'owner' : 'boto3',
    'start_date' : days_ago(0)
}

dag = DAG(
    dag_id = 'create_bucket',
    default_args=default_args,
    description='Ejemplo simple de como crear un bucket usando boto3, simulado en localstack',
    schedule_interval='@once'
)

def print_success_message(**kwargs):
    print("Success!!")

create = PythonOperator(task_id='create_bucket',
python_callable=create_bucket_dos.bucket('bucket-from-airflow-dos'),
dag=dag)

success = PythonOperator(task_id='success',
                        python_callable=print_success_message,
                        dag=dag)

success >> create