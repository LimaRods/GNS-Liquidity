import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
import time
from datetime import datetime
import calendar
import os
import base64
load_dotenv()

import logging
from ..utils.timing import timing

username = os.getenv("GRAFANA_USER")
password = os.getenv("GRAFANA_PASS")
auth = username + ":" + password
auth_encoded = auth.encode()
auth_64_enc = base64.b64encode(auth_encoded)

@timing
def get_network_load(from_date: datetime.date, to_date:datetime.date):
    """Get number of sessions in 30 second intervals between two dates. 
    Only tested in month lengths. Grafana queries can act non-intuitively when changing
    time periods. Be sure to validate queries if using for a time period not a month
    """

    from_ts = int(time.mktime(from_date.timetuple())) * 1000
    to_ts = int(time.mktime(to_date.timetuple())) * 1000

    response = requests.post(
        url="https://eu-metrics-monitoring.livepeer.live/grafana/api/ds/query",
        json = {
        "queries": [
            {
            "exemplar": True,
            "expr": 'sum(livepeer_current_sessions_total{node_type=\"bctr\"})',
            "format": "time_series",
            "interval": "",
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
            "intervalMs": 1800000,
            "maxDataPoints": 1464
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

    logging.debug("Querying Grafana  api to get number of sessions")
    values = response.json()['results']['A']['frames'][0]['data']['values']
    
    df = pd.DataFrame(data={
        'timestamp':values[0],
        'sessions':values[1]
    })

    df['date_time'] = pd.to_datetime(df.timestamp, unit='ms')
    return df

@timing
def get_monthly_network_load():
    months = []
    for i in range(1, datetime.today().month):
        from_date = datetime(2022, i, 1)
        last_day = calendar.monthrange(2022, i)
        to_date = datetime(2022, i, last_day[1])
        logging.debug("running get_monthly_network_load from %s to %s", 
            from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d"))
        months.append(get_network_load(from_date, to_date))
    
    months_df = pd.concat(months)

    return months_df