import json
import websocket
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

url = 'wss://stream.binance.com:9443/ws'
symbols  = ["BTCUSDT", "AAVEUSDT", "STXUSDT", "ARBUSDT"]
df = pd.DataFrame()
project_id = 'binance-416514'
dataset_id = "data_stream"
table_id = "binance_streaming"
keypath ='/home/tin/credentials/binance-cred.json'
credentials = service_account.Credentials.from_service_account_file(
            keypath, 
            scopes= ["https://www.googleapis.com/auth/cloud-platform"],
            )

# credentials= keypath ,scopes= ["https://www.googleapis.com/auth/cloud-platform"]
client = bigquery.Client(credentials=credentials)


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
                f"{symbol.lower()}@trade"
            ],
            "id": 1
        }))

ws = websocket.WebSocketApp(url,
                            on_open =on_open,
                            on_message = on_message,
                            on_error = on_error,
                            on_close = on_close)

ws.run_forever()