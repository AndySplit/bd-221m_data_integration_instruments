from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
from datetime import datetime


def get_orders():
	sql_stmt = "SELECT o.region, o.category, o.sub_category, o.quantity FROM orders o"
	pg_hook = PostgresHook(postgres_conn_id='postgres_default', schema='superstore')
	pg_conn = pg_hook.get_conn()
	cursor = pg_conn.cursor()
	cursor.execute(sql_stmt)
	data = cursor.fetchall()
	
	return data


def process_and_load_orders(ti):
	orders = ti.xcom_pull(task_ids=['get_orders'])
	orders2 = [item for sublist in orders for item in sublist]
	df = pd.DataFrame(data=orders2, columns=['region', 'category', 'sub_category', 'quantity'])
	df2 = df.groupby(['region', 'category', 'sub_category'])["quantity"].sum().reset_index()
	df2.to_csv('/home/zhidkov/airflow/dags/data/df.csv', index=False)
	
	
with DAG(
	dag_id='dag_zhidkov',
	schedule_interval='@daily',
	start_date = datetime(2023, 4, 6)
	) as dag:
	
	task_get_orders = PythonOperator(task_id='get_orders', python_callable=get_orders)
	
	task_process_and_load_orders = PythonOperator(task_id="process_and_load_orders", python_callable=process_and_load_orders)

	
task_get_orders >> task_process_and_load_orders
	
	
