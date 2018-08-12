#from __future__ import print_function
import logging
import airflow
from datetime import datetime, timedelta
from acme.operators.dwh_operators import PostgresOperatorWithTemplatedParams
from airflow.operators.sensors import ExternalTaskSensor
from airflow.models import Variable

global sqlg_dag_d

# sqlg_dag_d = None 

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True
}

tmpl_search_path = Variable.get("sql_path")


def create_dag(tmpl_search_path, args):
    if globals()['sqlg_dag_d'] == None:
        globals()['sqlg_dag_d'] = airflow.DAG(
            'd_ord_10',
            schedule_interval="@daily",
            dagrun_timeout=timedelta(minutes=60),
            template_searchpath=tmpl_search_path,
            default_args=args,
            max_active_runs=1)
    return globals()['sqlg_dag_d']

    
