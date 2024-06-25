from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging
import fastavro

# Function to ingest data from JSON to PostgreSQL with transformation
def ingest_json(file_path, table_name):
    logging.info(f"Ingesting file: {file_path}")
    df = pd.read_json(file_path)
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, if_exists='replace', index=False)

# Function to ingest data from CSV to PostgreSQL with transformation
def ingest_csv(file_path, table_name):
    logging.info(f"Ingesting file: {file_path}")
    df = pd.read_csv(file_path)
    
    # Transformation: Remove rows with missing values
    df = df.dropna()
    
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, if_exists='replace', index=False)

# Function to ingest data from Parquet to PostgreSQL with transformation
def ingest_parquet(file_path, table_name):
    logging.info(f"Ingesting file: {file_path}")
    df = pd.read_parquet(file_path)
    
    # Transformation: Remove rows with missing values
    df = df.dropna()
    
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, if_exists='replace', index=False)

# Function to ingest data from Avro to PostgreSQL with transformation
def ingest_avro(file_path, table_name):
    logging.info(f"Ingesting file: {file_path}")
    with open(file_path, 'rb') as f:
        reader = fastavro.reader(f)
        records = [r for r in reader]
    df = pd.DataFrame(records)
    
    # Transformation: Remove rows with missing values
    df = df.dropna()
    
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, if_exists='replace', index=False)

# Function to ingest data from Excel to PostgreSQL with transformation
def ingest_excel(file_path, table_name):
    logging.info(f"Ingesting file: {file_path}")
    df = pd.read_excel(file_path)
    
    # Transformation: Remove rows with missing values
    df = df.dropna()
    
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, if_exists='replace', index=False)

# Function to ingest data from Avro to PostgreSQL with specific transformation for order_items
def ingest_order_items(file_path, table_name):
    logging.info(f"Ingesting file: {file_path}")
    with open(file_path, 'rb') as f:
        reader = fastavro.reader(f)
        records = [r for r in reader]
    df = pd.DataFrame(records)
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, if_exists='replace', index=False)

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'dagetl',
    default_args=default_args,
    description='Ingest data from various formats to PostgreSQL with transformation',
    schedule_interval='@once',
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Base path - Adjust this to the correct base path inside your Docker container
base_path = "/opt/airflow/data"

# Define the tasks
tasks = []

# Ingest Coupons
tasks.append(PythonOperator(
    task_id='ingest_coupons',
    python_callable=ingest_json,
    op_kwargs={'file_path': f'{base_path}/coupons.json', 'table_name': 'coupons'},
    dag=dag
))

# Ingest Customers
for i in range(10):
    tasks.append(PythonOperator(
        task_id=f'ingest_customers_{i}',
        python_callable=ingest_csv,
        op_kwargs={'file_path': f'{base_path}/customer_{i}.csv', 'table_name': 'customers'},
        dag=dag
    ))

# Ingest Login Attempts
for i in range(10):
    tasks.append(PythonOperator(
        task_id=f'ingest_login_attempts_{i}',
        python_callable=ingest_json,
        op_kwargs={'file_path': f'{base_path}/login_attempts_{i}.json', 'table_name': 'login_attempts'},
        dag=dag
    ))

# Ingest Orders
tasks.append(PythonOperator(
    task_id='ingest_orders',
    python_callable=ingest_parquet,
    op_kwargs={'file_path': f'{base_path}/order.parquet', 'table_name': 'orders'},
    dag=dag
))

# Ingest Order Items
tasks.append(PythonOperator(
    task_id='ingest_order_items',
    python_callable=ingest_order_items,
    op_kwargs={'file_path': f'{base_path}/order_item.avro', 'table_name': 'order_items'},
    dag=dag
))

# Ingest Product Categories
tasks.append(PythonOperator(
    task_id='ingest_product_categories',
    python_callable=ingest_excel,
    op_kwargs={'file_path': f'{base_path}/product_category.xls', 'table_name': 'product_categories'},
    dag=dag
))

# Ingest Products
tasks.append(PythonOperator(
    task_id='ingest_products',
    python_callable=ingest_excel,
    op_kwargs={'file_path': f'{base_path}/product.xls', 'table_name': 'products'},
    dag=dag
))

# Ingest Suppliers
tasks.append(PythonOperator(
    task_id='ingest_suppliers',
    python_callable=ingest_excel,
    op_kwargs={'file_path': f'{base_path}/supplier.xls', 'table_name': 'suppliers'},
    dag=dag
))

# Set task dependencies
for task in tasks:
    task
