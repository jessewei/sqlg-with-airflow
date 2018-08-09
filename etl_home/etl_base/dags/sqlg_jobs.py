from __future__ import print_function
import logging
import airflow
from datetime import datetime, timedelta
from acme.operators.dwh_operators import PostgresOperatorWithTemplatedParams
from airflow.operators.sensors import ExternalTaskSensor
from airflow.models import Variable
import sqlg_dag

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True
}

tmpl_search_path = Variable.get("sql_path")

dag = sqlg_dag.create_dag(tmpl_search_path, args)

DB_NAME = 'DWH'
my_taskid = 'O_CUSTOMER'
O_CUSTOMER = PostgresOperatorWithTemplatedParams(
    task_id=my_taskid,
    postgres_conn_id='postgres_dwh',
    sql=DB_NAME + '/' + my_taskid + '/' + my_taskid + '.sql',
    parameters={"window_start_date": "{{ ds }}", "window_end_date": "{{ tomorrow_ds }}"},
    #dag=dag,
    start_date=airflow.utils.dates.days_ago(7),
    pool='postgres_dwh')

    
my_taskid = 'O_PRODUCT'
O_PRODUCT = PostgresOperatorWithTemplatedParams(
    task_id=my_taskid,
    postgres_conn_id='postgres_dwh',
    sql=DB_NAME + '/' + my_taskid + '/' + my_taskid + '.sql',
    parameters={"window_start_date": "{{ ds }}", "window_end_date": "{{ tomorrow_ds }}"},
    #dag=dag,
    start_date=airflow.utils.dates.days_ago(7),
    pool='postgres_dwh')

    
my_taskid = 'O_ORDERLINE'    
O_ORDERLINE = PostgresOperatorWithTemplatedParams(
    task_id=my_taskid,
    postgres_conn_id='postgres_dwh',
    sql=DB_NAME + '/' + my_taskid + '/' + my_taskid + '.sql',
    parameters={"window_start_date": "{{ ds }}", "window_end_date": "{{ tomorrow_ds }}"},
    #dag=sqlg_dag.sqlg_dag_d,
    start_date=airflow.utils.dates.days_ago(7),
    pool='postgres_dwh')

    
my_taskid = 'O_ORDER_INFO'    
O_ORDER_INFO = PostgresOperatorWithTemplatedParams(
    task_id=my_taskid,
    postgres_conn_id='postgres_dwh',
    sql=DB_NAME + '/' + my_taskid + '/' + my_taskid + '.sql',
    parameters={"window_start_date": "{{ ds }}", "window_end_date": "{{ tomorrow_ds }}"},
    #dag=dag,
    start_date=airflow.utils.dates.days_ago(7),
    pool='postgres_dwh')


