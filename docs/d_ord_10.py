# -*- coding: utf-8 -*-
#

from __future__ import print_function
import airflow
from datetime import datetime, timedelta
from acme.operators.dwh_operators import PostgresOperatorWithTemplatedParams
from airflow.operators.sensors import ExternalTaskSensor
from airflow.models import Variable


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True
}

tmpl_search_path = Variable.get("sql_path")

dag = airflow.DAG(
    'd_ord_10',
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=60),
    template_searchpath=tmpl_search_path,
    default_args=args,
    max_active_runs=1)

wait_for_cust_staging = ExternalTaskSensor(
    task_id='wait_for_cust_staging',
    external_dag_id='customer_staging',
    external_task_id='extract_customer',
    execution_delta=None,  # Same day as today
    dag=dag)

wait_for_prod_staging = ExternalTaskSensor(
    task_id='wait_for_prod_staging',
    external_dag_id='product_staging',
    external_task_id='extract_product',
    execution_delta=None,  # Same day as today
    dag=dag)

# Flow dag    
DB_NAME = 'DWH'
my_taskid = 'O_CUSTOMER'
O_CUSTOMER = PostgresOperatorWithTemplatedParams(
    task_id=my_taskid,
    postgres_conn_id='postgres_dwh',
    sql=DB_NAME + '/' + my_taskid + '/' + my_taskid + '.sql',
    parameters={"window_start_date": "{{ ds }}", "window_end_date": "{{ tomorrow_ds }}"},
    dag=dag,
    pool='postgres_dwh')

my_taskid = 'O_PRODUCT'
O_PRODUCT = PostgresOperatorWithTemplatedParams(
    task_id=my_taskid,
    postgres_conn_id='postgres_dwh',
    sql=DB_NAME + '/' + my_taskid + '/' + my_taskid + '.sql',
    parameters={"window_start_date": "{{ ds }}", "window_end_date": "{{ tomorrow_ds }}"},
    dag=dag,
    pool='postgres_dwh')

my_taskid = 'O_ORDER_INFO'    
O_ORDER_INFO = PostgresOperatorWithTemplatedParams(
    task_id=my_taskid,
    postgres_conn_id='postgres_dwh',
    sql=DB_NAME + '/' + my_taskid + '/' + my_taskid + '.sql',
    parameters={"window_start_date": "{{ ds }}", "window_end_date": "{{ tomorrow_ds }}"},
    dag=dag,
    pool='postgres_dwh')

my_taskid = 'O_ORDERLINE'    
O_ORDERLINE = PostgresOperatorWithTemplatedParams(
    task_id=my_taskid,
    postgres_conn_id='postgres_dwh',
    sql=DB_NAME + '/' + my_taskid + '/' + my_taskid + '.sql',
    parameters={"window_start_date": "{{ ds }}", "window_end_date": "{{ tomorrow_ds }}"},
    dag=dag,
    pool='postgres_dwh')
    
    
wait_for_cust_staging >> wait_for_prod_staging
wait_for_prod_staging >> O_CUSTOMER
wait_for_prod_staging >> O_PRODUCT
wait_for_prod_staging >> O_ORDER_INFO
wait_for_prod_staging >> O_ORDERLINE


if __name__ == "__main__":
    dag.cli()
