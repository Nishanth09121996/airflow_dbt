import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG("Simple_TaskGroup_Example",default_args=default_args) as dag:

    start=EmptyOperator(task_id="Start")
    end=EmptyOperator(task_id="End")
    
    with TaskGroup('all_task') as all_task:
    
        with TaskGroup("Section_1",tooltip="TASK_GROUP_EV_description") as Section_1:

            t1=BashOperator(
                    task_id="Section-1-Task-1",
                    bash_command='echo "Section-1-Task-1"'
                    )
            t2=BashOperator(
                    task_id="Section-1-Task-2",
                    bash_command='echo "Section-1-Task-2"'
                    )

        with TaskGroup("Section_2",tooltip="TASK_GROUP_EV_description") as Section_2:
            
            t1=BashOperator(
                    task_id="Section-2-Task-1",
                    bash_command='echo "Section-2-Task-1"'
                    )
            t2=BashOperator(
                    task_id="Section-2-Task-2",
                    bash_command='echo "Section-2-Task-2"'
                    )
            
        
#serially run TaskGroup DAG in following dependencies
#start>>Section_1>>Section_2>>end

#Parallel run TaskGroup DAG in following dependencies
start>>all_task>>end
