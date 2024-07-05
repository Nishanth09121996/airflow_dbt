import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup


def load_to_postgress():
    # Replace with your PostgreSQL credentials
    engine = create_engine('postgresql://airflow_user:admin@localhost:5432/sales')
    # Sample DataFrame
    data = {
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    }
    df = pd.DataFrame(data)

    # Write DataFrame to PostgreSQL
    df.to_sql('users', con=engine, if_exists='replace', index=False)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Define the DAG
dag = DAG(
    'load_to_postgres',
    default_args=default_args,
    description='A Dag Pushes data to postgres',
    schedule_interval=None,  # Run once, no schedule interval
)

# Start 
start = EmptyOperator(
    task_id = 'Start',
    dag = dag
)



# Define a task in the DAG
load_employess = PythonOperator(
    task_id='load_to_postgress',
    python_callable=load_to_postgress,
    dag=dag,
)

# End Dag 

end = EmptyOperator(
    task_id = 'End_of_Load',
    dag = dag
)


start>>load_employess>>end

