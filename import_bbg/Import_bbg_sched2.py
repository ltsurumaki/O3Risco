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
bzaccetp  = bzaccetp.rename(columns={'index':'val_date'})
bzaccetp['val_date'] = bzaccetp['val_date'].dt.date 
to_alchemy(bzaccetp,'cdi','replace')

sptr = BBG.fetch_series('SPTR500N Index','PX_Last',start,hoje, fx = 'USD')
sptr.to_csv('I:/Riscos/Maxdrawdown/sptr.csv')
sptr = sptr.reset_index()
sptr  = sptr.rename(columns={'index':'val_date'})
sptr['val_date'] = sptr['val_date'].dt.date 
to_alchemy(sptr,'sptr','replace')

ibov = BBG.fetch_series('IBOV Index','PX_Last', start,hoje, fx = 'BRL')
ibov.to_csv('I:/Riscos/Maxdrawdown/ibov.csv')
ibov = ibov.reset_index()
ibov  = ibov.rename(columns={'index':'val_date'})
ibov['val_date'] = ibov['val_date'].dt.date 
to_alchemy(ibov,'ibov','replace')

brl = BBG.fetch_series('BRL Curncy','PX_Last',start,hoje, fx = 'BRL')
brl.to_csv('I:/Riscos/Maxdrawdown/brl.csv')
brl = brl.reset_index()
brl  = brl.rename(columns={'index':'val_date'})
brl['val_date'] = brl['val_date'].dt.date 
to_alchemy(brl,'brl','replace')

spx = BBG.fetch_series('SPX Index','PX_Last',start,hoje, fx= 'USD')
spx.to_csv('I:/Riscos/Maxdrawdown/spx.csv')
spx = spx.reset_index()
spx  = spx.rename(columns={'index':'val_date'})
spx['val_date'] = spx['val_date'].dt.date 
to_alchemy(spx,'spx','replace')

ten_yr_treasury = BBG.fetch_series('USGG10YR Index','PX_Last',start,hoje, fx= 'USD')
ten_yr_treasury.to_csv('I:/Riscos/Maxdrawdown/10yr_treasury.csv')
ten_yr_treasury = ten_yr_treasury.reset_index()
ten_yr_treasury  = ten_yr_treasury.rename(columns={'index':'val_date'})
ten_yr_treasury['val_date'] = ten_yr_treasury['val_date'].dt.date 
to_alchemy(ten_yr_treasury,'ten_yr_treasury','replace')

pre_3_anos = BBG.fetch_series('BZAD3Y Index','PX_Last',start,hoje, fx= 'BRL')
pre_3_anos.to_csv('I:/Riscos/Maxdrawdown/pre_3_anos.csv')
pre_3_anos = pre_3_anos.reset_index()
pre_3_anos  = pre_3_anos.rename(columns={'index':'val_date'})
pre_3_anos['val_date'] = pre_3_anos['val_date'].dt.date 
to_alchemy(pre_3_anos,'pre_3_anos','replace')

dolar_real= BBG.fetch_series('USDBRL REGN Curncy','PX_Last',start,hoje, fx= 'BRL')
dolar_real.to_csv('I:/Riscos/Maxdrawdown/dolar_real.csv')
dolar_real = dolar_real.reset_index()
dolar_real  = dolar_real.rename(columns={'index':'val_date'})
dolar_real['val_date'] = dolar_real['val_date'].dt.date 
to_alchemy(dolar_real,'dolar_real','replace')

dxy= BBG.fetch_series('DXY Curncy','PX_Last',start,hoje, fx= 'USD')
dxy.to_csv('I:/Riscos/Maxdrawdown/dxy.csv')
dxy = dxy.reset_index()
dxy  = dxy.rename(columns={'index':'val_date'})
dxy['val_date'] = dxy['val_date'].dt.date 
to_alchemy(dxy,'dxy','replace')

mxcn= BBG.fetch_series('MXCN Index','PX_Last',start,hoje, fx= 'USD')
mxcn.to_csv('I:/Riscos/Maxdrawdown/mxcn.csv')
mxcn = mxcn.reset_index()
mxcn  = mxcn.rename(columns={'index':'val_date'})
mxcn['val_date'] = mxcn['val_date'].dt.date 
to_alchemy(mxcn,'mxcn','replace')

ff= BBG.fetch_series('FF1 Comdty','PX_Last','01/08/2021',hoje, fx= 'USD')
ff.to_csv('I:/Riscos/Maxdrawdown/ff.csv')
ff= ff.reset_index()
ff  = ff.rename(columns={'index':'val_date'})
ff['val_date'] = ff['val_date'].dt.date 
to_alchemy(ff,'ff','replace')

connection = engine.connect()
permisao = 'GRANT SELECT ON cdi TO fstein;'
permisao2 = 'GRANT SELECT ON cdi TO ttodesco;'
connection.execute(permisao)
connection.execute(permisao2)
telegram_send.send(messages=['BBG Importado!'])
