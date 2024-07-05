from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2021, 1, 1)
}

def _downloading():
    print('downloading')

with DAG('trigger_dag', 
    schedule_interval='@daily', 
    default_args=default_args, 
    catchup=False) as dag:

    downloading = PythonOperator(
        task_id='downloading',
        python_callable=_downloading
    )

    trigger_dag = TriggerDagRunOperator(
        task_id = 'storing',
        trigger_dag_id='target_dag',
        execution_date='{{ds}}',
        reset_dag_run= True,
        wait_for_completion=True
    )

    downloading >> trigger_dag