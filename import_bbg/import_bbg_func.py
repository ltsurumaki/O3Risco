# -*- coding: utf-8 -*-
"""
Created on Tue Nov  9 17:56:53 2021

@author: ludmilla.tsurumaki
"""

import os
import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from wraper import *

param_dic = {
    "host"      : "pp-appsrv13",
    "database"  : "o3risco",
    "user"      : "ltsurumaki",
    "password"  : "memude"
    }

connect = "postgresql+psycopg2://%s:%s@%s:5432/%s" % (
    param_dic['user'],
    param_dic['password'],
    param_dic['host'],
    param_dic['database']
    )  

engine = create_engine(connect)  
hoje = dt.datetime.today().strftime("%m/%d/%Y")

def import_bbg(ticker_list,table_name,start_date):
    ativos_bbg = pd.DataFrame()
    for i in range(len(ticker_list)):
        df = BBG.fetch_series(ticker_list[i],'PX_Last',start_date, hoje)
        df = df.reset_index()
        df  = df.rename(columns={'index':'val_date'})
        df.rename(columns={df.columns[1]:'valor'},inplace=True)
        df['ticker'] = ticker_list[i]
        ativos_bbg = ativos_bbg.append(df)
    connection = engine.connect()
    ativos_bbg.to_sql(table_name, 
                  con=engine, 
                  index=False, 
                  if_exists='replace')
    return(0)

tickers = ['CRM US Equity','DHR US Equity','EL US Equity','FB US Equity','MC FP Equity','MSFT US Equity','NKE US Equity','NOW US Equity','TMO US Equity','V US Equity']
a = import_bbg(tickers,'precos_transcripts','01-01-2000')
