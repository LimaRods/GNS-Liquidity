
import sqlalchemy
import os
from dotenv import load_dotenv
import logging 
import pandas as pd
import time
import base64
import requests
from datetime import datetime, timedelta
from ..utils.timing import timing
from google.cloud import storage

# The Cloud SQL Python Connector can be used along with SQLAlchemy using the
# 'creator' argument to 'create_engine

load_dotenv()
def init_db_engine(db_name: str, echo: bool=False) -> sqlalchemy.engine.Engine:
    """Convenience wrapper to intialize sqlalchmey connection to our gcp databases
    
    Parameters
    db_name: one of ['analytics', 'livepeer_studio', 'studio']
    echo: passed into 'create_engine' - if true then connection will be verbose
    Returns
    sqlalchey engine to be passed into pandas sql 'con' variables
    """

    if db_name == 'analytics':
        user_alias = "POSTGRES_USER"
        pass_alias = "POSTGRES_PASS"
        host_alias = "POSTGRES_HOST"
        db_alias   = "POSTGRES_DB"
    elif db_name in ["livepeer_studio", "studio"]:
        user_alias = "LS_POSTGRES_USER"
        pass_alias = "LS_POSTGRES_PASS"
        host_alias = "LS_POSTGRES_HOST"
        db_alias   = "LS_POSTGRES_DB"
    else:
        print("invalid db_name choose one of ['analytics', 'livepeer_studio', 'studio']")
    
    load_dotenv(override=True)

    conn_string = f'postgresql://{os.getenv(user_alias)}:{os.getenv(pass_alias)}@{os.getenv(host_alias)}/{os.getenv(db_alias)}'

    return sqlalchemy.create_engine(conn_string, echo=echo)

@timing
def query_from_file(sql_file_path: str, engine_name: str):
    """query from saved sql file
    
    Parameters
    sql_file_path: path to the saved sql file
    engine_name: either 'livepeer_studio' or 'analytics'
    
    Returns
    pandas dataframe with queried results
    """
    file = open(sql_file_path, 'r')
    query = file.read()
    engine = init_db_engine(engine_name)
    logging.debug("Beginning " + sql_file_path + " query")
    result_df = pd.read_sql(query, con=engine)

    return result_df

def get_interval_ms(interval):
    if 'm' in interval.lower():
        hours = int(interval.replace('m', "")) / 60
    else:
        hours = int(interval.replace('H', ""))
    interval_ms = hours * 60 * 60 * 1000
    
    return interval_ms

@timing
def query_prometheus(query:str, from_date:datetime, to_date:datetime, interval:str, env:str='prod'):
    """Query prometheus metric
    Parameters
    ----------
    query: string version of prometheus query
    from_date: datetime.datetime date to start query
    to_date: datetime.datetime to end query
    interval: time between data points, one of month, week, day, hour, minute
    env: either 'prod' or 'staging'
    """
    username = os.getenv("GRAFANA_USER")
    password = os.getenv("GRAFANA_PASS") if env == 'prod' else os.getenv("GRAFANA_PASS_MONSTER") 
    auth = username + ":" + password
    auth_encoded = auth.encode()
    auth_64_enc = base64.b64encode(auth_encoded)

    from_ts = int(time.mktime(from_date.timetuple())) * 1000
    to_ts = int(time.mktime(to_date.timetuple())) * 1000

    interval_ms = get_interval_ms(interval)

    max_data_points = ((to_ts - from_ts) / interval_ms) * 1.1

    _env = 'live' if env == 'prod' else 'monster'
    response = requests.post(
        url=f"https://eu-metrics-monitoring.livepeer.{_env}/grafana/api/ds/query",
        json = {
        "queries": [
            {
            "exemplar": True,
            "expr": query,
            "format": "time_series",
            "interval": "1h",
            "intervalFactor": 1,
            "refId": "A",
            "datasource": {
                "uid": "PBFA97CFB590B2093",
                "type": "prometheus"
            },
            "key": "Q-a76ae6b1-fc00-4cb1-a4c7-94066bff03c2-0",
            "queryType": "timeSeriesQuery",
            "requestId": "Q-a76ae6b1-fc00-4cb1-a4c7-94066bff03c2-0A",
            "utcOffsetSec": -18000,
            "legendFormat": "",
            "datasourceId": 1,
            # "intervalMs": interval_ms,
            "maxDataPoints": max_data_points
            }
        ],
        "range": {
            "from": from_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "to": to_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        },
        'from':str(from_ts),
        'to':str(to_ts)
        },
        headers={
            'authorization':"Basic " + auth_64_enc.decode('ascii'),
            "x-grafana-org-id": "1",
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    )

    frames = response.json()['results']['A']['frames']
    frame_df_list = []
    for frame in frames:
        values = frame['data']['values'] 
        metric = frame['schema']['name']
        frame_df_list.append(pd.DataFrame({
            'timestamp':values[0],
            'value':values[1],
            'metric':metric
        })
        )
    frame_df = pd.concat(frame_df_list)
    frame_df['date_time'] = pd.to_datetime(frame_df.timestamp, unit='ms')
    return frame_df

def append_data(first_date, date_index_col, table_name, query):
    engine = init_db_engine('analytics')
    query = f"selct max({date_index_col}) from {table_name}"
    try:
        max_date = pd.read_sql(query, engine)
    except:
        max_date = None
    

def connect_to_bucket(bucket_name: str='livepeer-reports'):
    try:
        client = storage.Client.from_service_account_info(eval(os.getenv('GCS_CREDS')))
        bucket = client.get_bucket(bucket_name)
        return bucket
    except Exception as e:
        print("error connecting, check env file for credentials", e)

def write_to_bucket(local_file_name, bucket_file_name, bucket_name='livepeer-reports'):
    bucket = connect_to_bucket(bucket_name)
    bucket_file = bucket.blob(bucket_file_name)
    with open(local_file_name, 'rb') as f:
        bucket_file.upload_from_file(f)
    return bucket_file.public_url

def write_figure_to_bucket(fig, bucket_file_name, bucket_name='livepeer-reports'):
    fig.write_image('temp.jpeg')
    url = write_to_bucket('temp.jpeg', bucket_file_name, bucket_name)
    os.remove('temp.jpeg')
    return url

import json
def convert_dict_cols_to_str(col: pd.Series):
    # the series will be a generic object type - need to check the first element
    # to verify if its a dict
    if any(col.apply(lambda x: type(x) == dict)):
        print("json dumping", col.name)
        return col.apply(json.dumps)
    else:
        return col