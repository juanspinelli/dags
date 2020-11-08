from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

# https://towardsdatascience.com/dependencies-between-dags-in-apache-airflow-2f5935cde3f0

dag = DAG(
    dag_id = 'ejemplo_complejo',
    schedule_interval='@once',
    owner='medium',
    start_date=days_ago(0),
    catchup=False
)

def crear_cluster(**kwargs):
    print("Success!!")

def esperar_cluster(**kwargs):
    print("Success!!")

def enviar_proceso_a_livy(**kwargs):
    print("Success!!")

def enviar_otro_proceso_a_livy(**kwargs):
    print("Success!!")

def verificar_claves_primarias(**kwargs):
    print("Success!!")

def insertar_en_redshift(**kwargs):
    print("Success!!")

def mover_raw_data_a_backup(**kwargs):
    print("Success!!")

def terminar_cluster(**kwargs):
    print("Success!!")


crear_cluster = PythonOperator(task_id='crear_cluster',
python_callable=crear_cluster,
dag=dag)

esperar_cluster = PythonOperator(task_id='esperar_cluster',
python_callable=esperar_cluster,
dag=dag)

enviar_proceso_a_livy = PythonOperator(task_id='enviar_proceso_a_livy',
python_callable=enviar_proceso_a_livy,
dag=dag)

enviar_otro_proceso_a_livy = PythonOperator(task_id='enviar_otro_proceso_a_livy',
python_callable=enviar_otro_proceso_a_livy,
dag=dag)

verificar_claves_primarias = PythonOperator(task_id='verificar_claves_primarias',
python_callable=verificar_claves_primarias,
dag=dag)

insertar_en_redshift = PythonOperator(task_id='insertar_en_redshift',
python_callable=insertar_en_redshift,
dag=dag)

mover_raw_data_a_backup = PythonOperator(task_id='mover_raw_data_a_backup',
python_callable=mover_raw_data_a_backup,
dag=dag)

terminar_cluster = PythonOperator(task_id='terminar_cluster',
python_callable=terminar_cluster,
dag=dag)

crear_cluster >> esperar_cluster
esperar_cluster >> enviar_proceso_a_livy
esperar_cluster >> enviar_otro_proceso_a_livy
enviar_proceso_a_livy >> verificar_claves_primarias
enviar_otro_proceso_a_livy >> verificar_claves_primarias
verificar_claves_primarias >> insertar_en_redshift
insertar_en_redshift >> mover_raw_data_a_backup
mover_raw_data_a_backup >> terminar_cluster