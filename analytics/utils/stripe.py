import os
import pandas as pd
from analytics.utils.database import init_db_engine
from dotenv import load_dotenv
load_dotenv()
import stripe
stripe.api_key = os.getenv('STRIPE_KEY')
import logging
import sqlalchemy as sql
from analytics.utils.timing import timing 

def response_to_df(response):
    response_dict = response.to_dict_recursive()
    response_df = pd.DataFrame.from_records(response_dict['data'])
    # response_df = response_df.apply(lambda x: convert_dict_cols_to_str(x))

    return response_df

def get_latest_table_date(table_name:str, backfill:bool=False) -> int:
    engine = init_db_engine('analytics')
    insp = sql.inspect(engine)
    if backfill:
        return None
    elif insp.has_table(table_name):
        query = f"select max(created) from {table_name}"
        res = pd.read_sql(query, engine)
        return res['max'].values[0]
    else:
        return None
@timing
def generate_invoices_table(lower_bound_ts:int=None):
    response = response_to_df(stripe.Invoice.list(limit=10, created={'gt':lower_bound_ts, 'lt':None}))
    df_list = list()
    i = 0
    while response.shape[0] > 0:
        df_list.append(response)
        # logging.debug(str(i) + str(pd.to_datetime(response.created.min(), unit='s')))
        response = response_to_df(stripe.Invoice.list(limit=100, created={'gt':lower_bound_ts, 'lt':response.created.min()}))
        i += 1

    return pd.concat(df_list)
    

    



