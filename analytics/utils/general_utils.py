import pandas as pd

def convert_col_to_numeric(col):
    try:
        return pd.to_numeric(col)
    except:
        return col

def convert_col_to_int(col):
    try:
        col = pd.to_numeric(col).round(0)
        return col
    except:
        return col