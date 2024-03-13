from datetime import datetime
import pendulum
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
# import data_stream.stream as s
from google.cloud import bigquery
client = bigquery.Client()

### websocket
import json
import websocket
import pandas as pd
from google.cloud import bigquery
url = 'wss://stream.binance.com:9443/ws'
symbols  = ["BTCUSDT", "AAVEUSDT", "STXUSDT", "ARBUSDT"]
df = pd.DataFrame()
project_id = 'binance-416514'
dataset_id = "data_stream"
table_id = "kline_binance_stream"



def on_message(ws,message):
    msg = json.loads(message)
    d = [(
        # msg['t'],
    msg['s'], msg['T'], msg['p'], msg['q'],msg['b'],msg['a'])]
    global df 
    df = pd.concat([df,pd.DataFrame.from_records(d)])

    # Define BigQuery table schema
    # schema = [
    #     bigquery.SchemaField('symbol', 'STRING'),
    #     bigquery.SchemaField('time', 'TIMESTAMP'),
    #     bigquery.SchemaField('price', 'FLOAT'),
    #     bigquery.SchemaField('qty', 'FLOAT'),
    # ]
    rows_to_insert = {
        # 'trade_id': msg['t'],
        'symbol': msg['s'],
        'time': pd.to_datetime(msg['T'], unit='ms').strftime('%Y-%m-%d %H:%M:%S'),
        'price': float(msg['p']),
        'qty': float(msg['q']),
        'buy_order_id' : msg['b'],
        'sell_order_id' : msg['a']
    }
    

    # Insert data into BigQuery table
    # rows_to_insert = [(msg['s'], msg['T'], msg['p'], msg['q'])]
    errors = client.insert_rows_json(
        f"{project_id}.{dataset_id}.{table_id}",
        [rows_to_insert],
        row_ids=[None]*len(rows_to_insert),
        skip_invalid_rows=True,
        ignore_unknown_values=True
    )

    if errors:
        print(f"Errors: {errors}")

def on_error(ws, error):
    print(error)

def on_close(ws,close_status_code, close_msg):
    
    df.columns = [
        # 'trade_id',
         'symbol','time','price','qty','buy_order_id','sell_order_id']
    df['time'] = pd.to_datetime(df['time'],unit = 'ms')
    df.set_index(df['time'],inplace = True)
    df.drop(columns='time',inplace = True)
    print(df)
    print('### closed ###')

def on_open(ws):
    print('open connection')
    for symbol in symbols:
        ws.send(json.dumps({
            "method": "SUBSCRIBE",
            "params": [
                f"{symbol.lower()}@kline_1m"
            ],
            "id": 1
        }))

def stream():
    ws = websocket.WebSocketApp(url,
                                on_open =on_open,
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close)

    ws.run_forever()



args = {
    'owner': 'opal',
    'retries': 1
    # 'on_failure_callback': alert.task_fail_slack_alert,
}

LOCAL_TZ = pendulum.timezone("Asia/Bangkok")


dag = DAG(
    dag_id = f'binance_kline_streaming',
    default_args=args,
    catchup=False,
    schedule_interval = '*/10 * * * *',
    start_date = datetime(2024,3,13, tzinfo = LOCAL_TZ),
    tags = ['bittaza','binance']
)

extract_task = PythonOperator(
        task_id = f'extract_streaming',
        python_callable = stream,
        dag = dag

    )
extract_task