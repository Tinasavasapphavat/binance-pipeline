from datetime import datetime
import pendulum
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery  import BigQueryInsertJobOperator,BigQueryCheckOperator
# import func.utils as u

from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd

import requests

url = 'https://api.binance.com/api/v3/historicalTrades'
trade_lst = ["BTCUSDT", "AAVEUSDT", "STXUSDT", "ARBUSDT"]
project_id = 'binance-416514'
dwh = 'dwh'
scd = 'scd'

client = bigquery.Client()

def extract_data(trade):
    table_id = f'{project_id}.{dwh}.{trade}'

    print(f'now we get {table_id}')
    r = requests.get(url, params=dict(symbol = trade))
    print('getting json')
    data = r.json()
    print('getting dataframe')
    df = pd.DataFrame(data
                    ,index = [0]
            )
    df['snapshot_date']= datetime.now()

    print(f' start uploading to {table_id}')

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(
        df, 
        table_id, 
        job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.
    client.get_table(table_id)



args = {
    'owner': 'opal',
    'retries': 1
    # 'on_failure_callback': alert.task_fail_slack_alert,
}

LOCAL_TZ = pendulum.timezone("Asia/Bangkok")


dag = DAG(
    dag_id = f'binacce',
    default_args=args,
    catchup=False,
    schedule_interval = '*/5 * * * *',
    start_date = datetime(2024,3,11, tzinfo = LOCAL_TZ),
    tags = ['bittaza','binance']
)

def pipeline(trade):
    
    append_historical_task =  BigQueryInsertJobOperator(
        task_id = f'append_{trade}_to_scd',
        configuration= {
            "query" :  {
                "query" : f"""
                    INSERT INTO `{project_id}.{scd}.{trade}_scd`
                    SELECT *          
                    FROM `{project_id}.{dwh}.{trade}`
                        """,
                "useLegacySql" :False,
                # "writeDisposition" : "WRITE_APPEND",
                # "destinationTable": {

                #     "projectId": f"{project_id}",
                #     "datasetId": "dynamic_bc",
                #     "tableId": "rebates_invoice_header"}
            
            }
        },
        gcp_conn_id = 'google_cloud_default',
        location='asia-southeast1',  
        dag = dag
    )
    
    extract_task = PythonOperator(
        task_id = f'extract_{trade}_transaction',
        python_callable = extract_data,
        op_args = [trade],
        dag = dag

    )

    check_inv_line_task = BigQueryCheckOperator(
        task_id = f'check_{trade}',
        sql = f"""
                select count(*) from `{project_id}.{dwh}.{trade}`
            """,
        use_legacy_sql= False,
        location='asia-southeast1',
        gcp_conn_id = 'google_cloud_default',
        dag = dag

    )
    
    
    append_historical_task >> extract_task >> check_inv_line_task 

for trade in trade_lst:
    pipeline(trade)

