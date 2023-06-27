import pandas as pd
from flipside import Flipside

# Manual Input
sdk_api_key= 'API_KEY'

#Creating a class to query Flipside Data
class Query:

    def __init__(self, script, api_key = sdk_api_key, data = None):
        self.api_key = api_key
        self.script = script
        self.data = data

    def query_data(self, category= None, groupby = 'day', start_date = None):
        try:
            if groupby is None and start_date  is None:
                sdk =  Flipside(self.api_key)
                sql = self.script

                query_result =  sdk.query(sql)
                self.data = pd.DataFrame.from_records(query_result.records)
                return self
            
            else : 
                sdk =  Flipside(self.api_key)
                sql = self.script.format(category,groupby,start_date)
                query_result =  sdk.query(sql)
                self.data = pd.DataFrame.from_records(query_result.records)
                return self
        
        except BaseException as e:
            print(e)

# Function to build simulation table for Pool liquidity
def AMM_contract(in_amount, in_price_usd, out_amount, out_price_usd, deposit_limit, token_from, step = 20):
    k = in_amount * out_amount
    out_price = out_price_usd/in_price_usd # Price of Token IN in Token Out
    in_price= out_price**-1 # Price of Token OUT in Token IN
    in_amount_new = 0
    out_amount_new = 0
    matrix = [[0,in_amount,out_amount, k, out_price, in_price]]
    deposit =deposit_limit/step
   
    while True:
        if deposit < deposit_limit + deposit_limit/step:
            in_amount_new = in_amount + deposit
            out_amount_new = k/ in_amount_new
            delta_out = out_amount - k/ in_amount_new
            out_price_new = (deposit/delta_out)
            matrix.append([deposit,in_amount_new,out_amount_new, k,  out_price_new,out_price_new**-1])
            deposit+= deposit_limit/step
        else:
            break
    df_amm = pd.DataFrame(matrix, columns = ['token_deposit',token_from, 'GNS', 'K', f'Price of GNS in {token_from}',f'Price of {token_from} in GNSD' ])
    df_amm = df_amm.drop(df_amm.index[0])
    df_amm.index = range(len(df_amm.index))
    df_amm['Price of GNS in USD'] = df_amm[f'Price of GNS in {token_from}'] * in_price_usd
    first_price = df_amm[f'Price of GNS in {token_from}'].iloc[0]
    df_amm['GNS Slippage percent'] = (df_amm[f'Price of GNS in {token_from}'] - first_price) / first_price * 100
    df_amm['Amount_IN_USD'] = df_amm.token_deposit * in_price_usd
    df_amm = df_amm.assign(Amount_OUT_USD = lambda x: (out_amount - x['GNS']) * out_price_usd)
    df_amm['Slippage_USD'] = df_amm.Amount_IN_USD	- df_amm.Amount_OUT_USD
    return df_amm