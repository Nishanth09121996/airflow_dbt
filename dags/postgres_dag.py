from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='postgres_example_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task 1: Create a table
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='PostgresLocal',
        sql="""
        CREATE TABLE IF NOT EXISTS example_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            age INT
        );
        """
    )

    # Task 2: Insert data into the table
    insert_data = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id='PostgresLocal',
        sql="""
        INSERT INTO example_table (name, age) VALUES ('Alice', 30), ('Bob', 25);
        """
    )

    # Task 3: Select data from the table
    select_data = PostgresOperator(
        task_id='select_data',
        postgres_conn_id='PostgresLocal',
        sql="SELECT * FROM example_table;",
    )

    # Define task dependencies
    create_table >> insert_data >> select_data
