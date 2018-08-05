# -*- coding: utf-8 -*-
#
#from __future__ import print_function
import logging
import airflow
from datetime import datetime, timedelta
from acme.operators.dwh_operators import PostgresOperatorWithTemplatedParams
from airflow.operators.sensors import ExternalTaskSensor
from airflow.models import Variable
import sqlg_jobs 
import sqlg_dag

# global dag


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True
}

tmpl_search_path = Variable.get("sql_path")

#dag = airflow.DAG(
#    'd_ord_10',
#    schedule_interval="@daily",
#    dagrun_timeout=timedelta(minutes=60),
#    template_searchpath=tmpl_search_path,
#    default_args=args,
#    max_active_runs=1)

dag = sqlg_dag.create_dag(tmpl_search_path, args)

#global dag
DB_NAME = 'DWH'
my_taskid = 'O_ORDERLINE'    
O_ORDERLINE = PostgresOperatorWithTemplatedParams(
    task_id=my_taskid,
    postgres_conn_id='postgres_dwh',
    sql=DB_NAME + '/' + my_taskid + '/' + my_taskid + '.sql',
    parameters={"window_start_date": "{{ ds }}", "window_end_date": "{{ tomorrow_ds }}"},
    dag=sqlg_dag.sqlg_dag_d,
    pool='postgres_dwh')
    
wait_for_cust_staging = ExternalTaskSensor(
    task_id='wait_for_cust_staging',
    external_dag_id='customer_staging',
    external_task_id='extract_customer',
    execution_delta=None,  # Same day as today
    dag=sqlg_dag.sqlg_dag_d)

wait_for_prod_staging = ExternalTaskSensor(
    task_id='wait_for_prod_staging',
    external_dag_id='product_staging',
    external_task_id='extract_product',
    execution_delta=None,  # Same day as today
    dag=sqlg_dag.sqlg_dag_d)

# logging.info('trace 2', sqlg_jobs.dag)
    
wait_for_cust_staging >> wait_for_prod_staging
# wait_for_prod_staging >> sqlg_jobs.O_PRODUCT
# wait_for_prod_staging >> sqlg_jobs.O_ORDERLINE
O_ORDERLINE >> wait_for_prod_staging


if __name__ == "__main__":
    sqlg_dag.sqlg_dag_d.cli()
