'''
=================================================================================
Milestone 3

Name: Dwita Alya Windani
Batch: FTDS-RMT-024

This program is used to automate end-to-end data pipeline using Airflow. Program
consists of getting data from potsgresql, then cleaning data, and store the data 
to elasticsearch.
=================================================================================
'''

# import libraries
import datetime as dt
from datetime import timedelta
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# function get data from postgresql
def get_data_from_postgres(dbname, host, user, password, table):
    '''
    This function is used to get data from PostgreSQL.

    Parameters:
        dbname: string - Database Name
        host: string - Location of the server (example: localhost)
        user: string - your PostgreSQL username (example: postgres)
        password: string - your user password (example: postgres)
        table: string - Table Name.

    Return:
        data: list of string - records from database

    How To Use:
    data = get_data_from_postgres('milestone3','localhost','postgres','postgres','table_m3')
    '''
    conn = db.connect(database=dbname,
                      host=host,
                      user=user,
                      password=password)
    data = pd.read_sql(f"select * from {table}", conn)
    return data

# function cleaning data
def data_cleaning():
    '''
    This program purpose is to clean data after the data has been fetched from PostgreSQL.
    Program include:
        - change duration into seconds
        - add new column duration minutes
        - change datatype of release date into datetime
        - drop null values
        - save clean data into csv file
    '''
    df = get_data_from_postgres('milestone3','localhost','postgres','postgres','table_m3')
    df.duration_ms = df.duration_ms / 1000
    df.rename(columns={'duration_ms': 'duration_sec'}, inplace=True)
    df['duration_min'] = df['duration_sec'] / 60
    df['track_album_release_date'] = pd.to_datetime(df['track_album_release_date'], format='mixed', yearfirst=True, errors='coerce')
    num_cols = df.select_dtypes(include=('int64','float64')).columns.tolist()
    df[num_cols] = df[num_cols].fillna(df[num_cols].median())
    cat_cols = df.select_dtypes(include='object').columns.tolist()
    df = df.dropna(subset=cat_cols)
    df.to_csv('P2M3_dwita_data_clean.csv')
    
# function insert dataset into elasticsearch
def to_elasticsearch():
    '''
    This program purpose is to insert cleaned data into ElasticSearch
    '''
    es = Elasticsearch("http://localhost:9200")
    df = pd.read_csv('/Users/ita/github-classroom/FTDS-assignment-bay/p2-ftds024-rmt-m3-dwitawin/P2M3_dwita_data_clean.csv')
    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(index='data_clean', doc_type='doc', body=doc)
        print(res)

# Initialize Default Argument
default_args = {
    'owner' : 'Dwita',
    'start_date' : dt.datetime(2023, 11, 26, 23, 30, 0),
    'retries' : 1,
    'retry_delay' : dt.timedelta(minutes=5)
}

# Create DAG
with DAG(
    'P2M3DwitaDAG',
    default_args = default_args,
    schedule_interval = '30 6 * * *'
) as dag:
    
    # task 1: get data from postgresql
    GetData = PythonOperator(task_id='get',
                             python_callable=get_data_from_postgres)
    
    # task 2: cleaning data
    CleanData = PythonOperator(task_id='clean',
                             python_callable=data_cleaning)
    
    # task 3: insert data to elasticsearch
    InsertToES = PythonOperator(task_id='to_elasticsearch',
                             python_callable=to_elasticsearch)
    


GetData >> CleanData >> InsertToES
