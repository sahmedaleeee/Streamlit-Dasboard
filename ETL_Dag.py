import requests as r
from datetime import (date, datetime, timedelta)
import re
import json
import pandas as pd
import time as t
import gspread
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from airflow import DAG
from airflow.operators.python import PythonOperator
from ETL import _extract_data,_transform_data,_load_data


dag = DAG(
    dag_id= "ETL_dag_v01",
    start_date=datetime(2022,8, 27),
    catchup=False,
    
)

extract_data = PythonOperator(
    task_id = "extract_data",
    python_callable=_extract_data,
    do_xcom_push=True,
    dag=dag
)

transform_data = PythonOperator(
    task_id = "transform_data",
    python_callable=_transform_data,
    do_xcom_push=True,
    dag=dag
)

load_data = PythonOperator(
    task_id = "load_data",
    python_callable=_load_data,
    dag=dag
)

extract_data >> transform_data >> load_data
