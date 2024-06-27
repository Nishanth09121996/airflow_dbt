from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Define the function that will be executed as a task
def print_hello():
    print("Hello, World!")

def task1():
    print("Hello, task1!")

def task2():
    print("Hello, World!")

def task3():
    print("Hello, World!")

def task4():
    print("Hello, World!")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Define the DAG
dag = DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple DAG that prints Hello World!',
    schedule_interval=None,  # Run once, no schedule interval
)

# Define a task in the DAG
task_hello = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

task_task1 = PythonOperator(
    task_id='task1',
    python_callable=task1,
    dag=dag,
)

task_task2 = PythonOperator(
    task_id='task2',
    python_callable=task2,
    dag=dag,
)

task_task3 = PythonOperator(
    task_id='task3',
    python_callable=task3,
    dag=dag,
)




# Define task dependencies (not needed here since it's a single task)
task_hello>>task_task1>>task_task2>>task_task3
