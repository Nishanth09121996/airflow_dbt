import pandas as pd
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator

def load_to_postgress(df:pd.DataFrame,table_name:str):
    engine = create_engine('postgresql://airflow_user:admin@localhost:5432/sales')
    # Write DataFrame to PostgreSQL
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

ad_calendar = pd.read_csv('/home/yamini/airflow_dbt/adventureworks/AdventureWorks_Calendar.csv')
ad_calendar['Date'] = ad_calendar['Date'].astype('datetime64[ns]')

# Customer
ad_customers = pd.read_csv('/home/yamini/airflow_dbt/adventureworks/AdventureWorks_Customers.csv',encoding='latin-1')

# Product Category
ad_category = pd.read_csv('/home/yamini/airflow_dbt/adventureworks/AdventureWorks_Product_Categories.csv')

# Product Subcategory 

ad_subcategory = pd.read_csv('/home/yamini/airflow_dbt/adventureworks/AdventureWorks_Product_Subcategories.csv')

# Products
ad_products = pd.read_csv('/home/yamini/airflow_dbt/adventureworks/AdventureWorks_Products.csv')

# Returns 
ad_returns = pd.read_csv('/home/yamini/airflow_dbt/adventureworks/AdventureWorks_Returns.csv')

# Sales
sales_2015 = pd.read_csv('/home/yamini/airflow_dbt/adventureworks/AdventureWorks_Sales_2015.csv')
sales_2016 = pd.read_csv('/home/yamini/airflow_dbt/adventureworks/AdventureWorks_Sales_2016.csv')
sales_2017 = pd.read_csv('/home/yamini/airflow_dbt/adventureworks/AdventureWorks_Sales_2017.csv')
# union of sales data
ad_sales  = pd.concat([sales_2015,sales_2016,sales_2017],ignore_index=True)

# Territories

ad_territories = pd.read_csv('/home/yamini/airflow_dbt/adventureworks/AdventureWorks_Territories.csv')

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Define the DAG
with DAG(
    'load_adventure_works_data',
    default_args=default_args,
    description='A Dag Pushes data to postgres',
    schedule_interval=None,  # Run once, no schedule interval
) as adventure_dag:
    
    start = BashOperator(
        task_id = 'start_of_load',
        dag = adventure_dag,
        bash_command='echo "Data Load Started"'
    )

    with TaskGroup('load_all_product_data') as load_all_product_data:
        # loading Category Data
        load_category = PythonOperator(
            task_id = 'load_category_data',
            dag = adventure_dag,
            python_callable = load_to_postgress,
            op_args = [ad_category,'dim_product_category']
        )

        load_subcategory = PythonOperator(
            task_id="python_task",
            python_callable=load_to_postgress,
            dag = adventure_dag,
            op_args= [ad_subcategory,'dim_product_subcategory'],
        )

        # loading Product information

        load_products  = PythonOperator(
            task_id = 'load_products_data',
            dag = adventure_dag,
            python_callable = load_to_postgress,
            op_args = [ad_products,'dim_products']
        )
        
        echo_product_load=BashOperator(
                    task_id="echo_product_load",
                    bash_command='echo "Product Data Loaded"',
                    dag = adventure_dag
                    )
        [load_category,load_subcategory,load_products] >> echo_product_load
        
    with TaskGroup('load_all_customer_data') as load_all_customer_data:
        # Loading Calendar
        load_calendar  = PythonOperator(
            task_id='load_calendar',
            python_callable=load_to_postgress,
            dag=adventure_dag,
            op_args=[ad_calendar, 'dim_calendar'],
            )

        # Loading customer data
        load_customers = PythonOperator(
            task_id = 'load_customers',
            dag = adventure_dag,
            python_callable = load_to_postgress,
            op_args=[ad_customers, 'dim_customer'],
        )

        # Logger
        echo_customer = BashOperator(task_id = 'echo_customer',
                                     dag = adventure_dag,
                                     bash_command='echo "Customer Data Loaded"')
        load_calendar >> load_customers >> echo_customer


    with TaskGroup('load_all_fact_data') as load_all_fact_data:
        # Load Returns
        load_returns = PythonOperator(
            task_id  = 'load_returns',
            dag = adventure_dag,
            python_callable = load_to_postgress,
            op_args = [ad_returns,'dim_returns']
        )

        # Load sales 

        load_sales = PythonOperator(
            task_id = 'load_sales_data',
            python_callable = load_to_postgress,
            dag = adventure_dag,
            op_args = [ad_sales,'fact_sales']
        )

        # load_territories 

        load_territories = PythonOperator(
            task_id = 'load_territory_data',
            python_callable = load_to_postgress,
            dag = adventure_dag,
            op_args = [ad_territories,'dim_territories']
        ) 
        # Logger
        echo_facts = BashOperator(task_id = 'echo_facts',
                                     dag = adventure_dag,
                                     bash_command='echo "Fact Data Loaded"')
        
        load_territories >> [load_returns,load_sales] >> echo_facts

    end_of_load = BashOperator(
        task_id = 'end_of_load',
        dag = adventure_dag,
        bash_command='echo "Data Loaded Succesfully"'
    )

start>>load_all_product_data>>load_all_customer_data>>load_all_fact_data>>end_of_load
