# -*- coding: utf-8 -*-
#

from __future__ import print_function
import airflow
from datetime import datetime, timedelta
from acme.operators.dwh_operators import PostgresOperatorWithTemplatedParams
from airflow.operators.sensors import ExternalTaskSensor
from airflow.models import Variable
from sqlg_jobs import *


global dag

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

print('trace 2', dag)        
    
wait_for_cust_staging >> wait_for_prod_staging
# wait_for_prod_staging >> O_CUSTOMER
wait_for_prod_staging >> O_PRODUCT
#wait_for_prod_staging >> O_ORDER_INFO >> O_ORDERLINE
wait_for_prod_staging >> O_ORDERLINE
O_CUSTOMER.set_upstream(wait_for_prod_staging)
O_ORDERLINE.set_upstream(O_ORDER_INFO) 


if __name__ == "__main__":
    dag.cli()
