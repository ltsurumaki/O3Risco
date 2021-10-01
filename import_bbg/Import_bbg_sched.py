# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 10:23:49 2021

@author: ludmilla.tsurumaki
"""
import os
import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
os.chdir(r'I:\Riscos\Codigos Automacao')
from wraper import *
import telegram_send

param_dic = {
    "host"      : "54.207.157.118",
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

def to_alchemy(df,tablename,tipo):       
    df.to_sql(tablename, 
                  con=engine, 
                  index=False, 
                  if_exists=tipo) 
    return 0

hoje = dt.datetime.today().strftime("%m/%d/%Y")
start = dt.datetime.today() - relativedelta(months=126)
start = start.strftime("%m/%d/%Y")

bzaccetp = BBG.fetch_series('BZACCETP Index','PX_Last','01/01/2012', hoje)
bzaccetp.to_csv('I:/Riscos/Maxdrawdown/bzaccetp.csv')
bzaccetp = bzaccetp.reset_index()
bzaccetp  = bzaccetp.rename(columns={'index':'val_date','BZACCETP Index':'px_last'})
bzaccetp['val_date'] = bzaccetp['val_date'].dt.date 
bzaccetp['ticker'] = 'BZACCETP Index'
#to_alchemy(bzaccetp,'cdi','replace')

lista1 = ['SPTR500N Index','SPX Index','USGG10YR Index','DXY Curncy','MXCN Index','FF1 Comdty']
lista2 = ['BZAD3Y Index','USDBRL REGN Curncy','IBOV Index']
df_final = pd.DataFrame()
for i in lista1:
    df = BBG.fetch_series(i,'PX_Last',start,hoje, fx = 'USD')
    df.to_csv('I:/Riscos/Maxdrawdown/'+ i +'.csv')
    df = df.reset_index()
    df  = df.rename(columns={'index':'val_date',i:'px_last'})
    df['val_date'] = df['val_date'].dt.date
    df['ticker'] = i
    df_final = df_final.append(df)

for i in lista2:
    df = BBG.fetch_series(i,'PX_Last',start,hoje, fx = 'BRL')
    df.to_csv('I:/Riscos/Maxdrawdown/'+ i +'.csv')
    df = df.reset_index()
    df  = df.rename(columns={'index':'val_date',i:'px_last'})
    df['val_date'] = df['val_date'].dt.date
    df['ticker'] = i
    df_final = df_final.append(df)
    
df_final = df_final.append(bzaccetp) 
to_alchemy(df_final,'precos','replace')


connection = engine.connect()
permisao = 'GRANT SELECT ON precos TO fstein;'
permisao2 = 'GRANT SELECT ON precos TO ttodesco;'
connection.execute(permisao)
connection.execute(permisao2)
telegram_send.send(messages=['BBG Importado!'])
