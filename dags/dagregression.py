from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from sklearn.linear_model import LinearRegression
import logging

# Function to fetch sales data
def fetch_sales_data():
    sql = """
    SELECT
        DATE_TRUNC('day', orders.created_at) AS sales_date,
        EXTRACT(month FROM orders.created_at) AS bulan,
        EXTRACT(day FROM orders.created_at) AS tanggal,
        SUM(order_items.amount * products.price) AS total_sales,
        AVG(products.price) AS avg_price,
        AVG(coupons.discount_percent) AS avg_discount
    FROM
        orders
        JOIN order_items ON orders.id = order_items.order_id
        JOIN products ON order_items.product_id = products.id
        LEFT JOIN coupons ON order_items.coupon_id = coupons.id
    GROUP BY
        sales_date, bulan, tanggal
    ORDER BY
        sales_date;
    """
    hook = PostgresHook(postgres_conn_id='postgres_dw')
    conn = hook.get_conn()
    df = pd.read_sql(sql, conn)
    return df

# Function to predict sales
def predict_sales():
    df = fetch_sales_data()
    df['sales_date'] = pd.to_datetime(df['sales_date'])
    df.set_index('sales_date', inplace=True)
    df.sort_index(inplace=True)
    
    df['day'] = df.index.day
    df['month'] = df.index.month
    df['year'] = df.index.year

    X = df[['day', 'month', 'year', 'avg_price', 'avg_discount']]
    y = df['total_sales']

    model = LinearRegression()
    model.fit(X, y)

    future_dates = pd.date_range(start=df.index[-1] + timedelta(days=1), periods=30)
    future_X = pd.DataFrame({
        'day': future_dates.day,
        'month': future_dates.month,
        'year': future_dates.year,
        'avg_price': df['avg_price'].mean(),  # assuming future avg_price is similar to past
        'avg_discount': df['avg_discount'].mean()  # assuming future avg_discount is similar to past
    })

    predictions = model.predict(future_X)
    future_df = pd.DataFrame({
        'sales_date': future_dates,
        'predicted_sales': predictions
    })

    # Save the predictions to a database table
    hook = PostgresHook(postgres_conn_id='postgres_dw')
    engine = hook.get_sqlalchemy_engine()
    future_df.to_sql('predicted_sales', engine, if_exists='replace', index=False)
    logging.info(f"Predicted sales: {future_df}")

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'sales_prediction_dag',
    default_args=default_args,
    description='A simple sales prediction DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the task
predict_sales_task = PythonOperator(
    task_id='predict_sales',
    python_callable=predict_sales,
    dag=dag,
)

# Set task dependencies
predict_sales_task
