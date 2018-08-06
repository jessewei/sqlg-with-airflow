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

# logging.info('trace 2', sqlg_jobs.dag)
sqlg_jobs.O_CUSTOMER.dag = dag
sqlg_jobs.O_PRODUCT.dag = dag
sqlg_jobs.O_ORDERLINE.dag = dag
sqlg_jobs.O_ORDER_INFO.dag = dag
    
#wait_for_cust_staging >> wait_for_prod_staging
wait_for_cust_staging >> sqlg_jobs.O_CUSTOMER
wait_for_prod_staging >> sqlg_jobs.O_PRODUCT
sqlg_jobs.O_CUSTOMER >> sqlg_jobs.O_ORDER_INFO
sqlg_jobs.O_PRODUCT >> sqlg_jobs.O_ORDER_INFO
sqlg_jobs.O_ORDER_INFO >> sqlg_jobs.O_ORDERLINE


if __name__ == "__main__":
    sqlg_dag.sqlg_dag_d.cli()
