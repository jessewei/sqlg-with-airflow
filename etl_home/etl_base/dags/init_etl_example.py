# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function
import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow import models
from airflow.settings import Session
from acme.operators.dwh_operators import PostgresOperatorWithTemplatedParams

import logging


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True
}


def initialize_etl_example():
    logging.info('Creating connections, pool and sql path')

    session = Session()

    def create_new_conn(session, attributes):
        new_conn = models.Connection()
        new_conn.conn_id = attributes.get("conn_id")
        new_conn.conn_type = attributes.get('conn_type')
        new_conn.host = attributes.get('host')
        new_conn.port = attributes.get('port')
        new_conn.schema = attributes.get('schema')
        new_conn.login = attributes.get('login')
        new_conn.set_password(attributes.get('password'))

        session.add(new_conn)
        session.commit()

    create_new_conn(session,
                    {"conn_id": "postgres_oltp",
                     "conn_type": "postgres",
                     "host": "postgres",
                     "port": 5432,
                     "schema": "orders",
                     "login": "oltp_read",
                     "password": "oltp_read"})

    # change from dwh_svc_account to db_owner                
    create_new_conn(session,
                    {"conn_id": "postgres_dwh",
                     "conn_type": "postgres",
                     "host": "postgres",
                     "port": 5432,
                     "schema": "dwh",
                     "login": "db_owner",
                     "password": "db_owner"})
#   Move variable setting to startup batch, to avoid console err msg when loading those dags refto this var.
#   new_var = models.Variable()
#   new_var.key = "sql_path"
#   new_var.set_val("/usr/local/airflow/sql")
#   session.add(new_var)
#   session.commit()

    new_pool = models.Pool()
    new_pool.pool = "postgres_dwh"
    new_pool.slots = 10
    new_pool.description = "Allows max. 10 connections to the DWH"

    session.add(new_pool)
    session.commit()
    session.close()

tmpl_search_path = models.Variable.get("sql_path")

dag = airflow.DAG(
    'init_docker_example',
    schedule_interval="@once",
    default_args=args,
    template_searchpath=tmpl_search_path,    
    max_active_runs=1)

initialize_example = PythonOperator(task_id='initialize_etl_example',
                    python_callable=initialize_etl_example,
                    provide_context=False,
                    dag=dag)

                    
SCH_NAME = '.'
my_taskid = 'DEMO-MERGE'

# Only .sql as file ext, so read .ddl into sqlstr then put in param
demo_ddl_file  = tmpl_search_path + '/' + my_taskid + '.ddl'
logging.info('demo_ddl_file=' + demo_ddl_file)
with open( demo_ddl_file, 'r') as myfile:
    demo_ddl = myfile.read()
#    demo_ddl = myfile.read().replace('\n', ' ')

logging.info('demo_ddl=' + demo_ddl)    
SQLG_DEMO_DDL = PostgresOperatorWithTemplatedParams(
    task_id=my_taskid,
    postgres_conn_id='postgres_dwh',
    sql = demo_ddl,
    parameters={"window_start_date": "{{ ds }}", "window_end_date": "{{ tomorrow_ds }}"},
    dag=dag,
    #start_date=airflow.utils.dates.days_ago(1),
    pool='postgres_dwh')

initialize_example >> SQLG_DEMO_DDL    

