# -*- coding: utf-8 -*-
"""
Created on Wed Oct 13 15:10:09 2021

@author: ludmilla.tsurumaki
"""

import pandas as pd
import datetime as dt
from pandas.tseries.offsets import BDay

from sqlalchemy import create_engine

#--------------------------------- Conexoes ---------------------------------

param_dic = {
   "host"      : "54.207.157.118",
   "database"  : "o3risco",
   "user"      : "ltsurumaki",
   "password"  : "memude"
   }

param_dic2 = {
   "host"      : "54.207.157.118",
   "database"  : "o3backoffice",
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

connect2 = "postgresql+psycopg2://%s:%s@%s:5432/%s" % (
    param_dic2['user'],
    param_dic2['password'],
    param_dic2['host'],
    param_dic2['database']
    ) 
 
engine2 = create_engine(connect2) 
#--------------------------------- Variaveis ---------------------------------
yesterday = dt.date.today() - BDay(1)
yesterday_arq = yesterday.strftime("%Y%m%d")
            
#----------------------------- Leitura de Dados ------------------------------
#pl
hbp = pd.read_sql("SELECT val_date,trading_desk,position_close FROM consolidado_pnl WHERE trading_desk='O3 RETORNO GLOBAL MASTER FIM' OR trading_desk='O3 RETORNO GLOBAL QUALIFICADO MASTER FIM' ;",con=engine2)
hbp = hbp[hbp['val_date']==yesterday] 
pl = hbp.groupby(['trading_desk','val_date']).sum().reset_index()
pl_quali = pl[pl['trading_desk']=='O3 RETORNO GLOBAL QUALIFICADO MASTER FIM'].reset_index().iloc[0]['position_close']
pl_geral= pl[pl['trading_desk']=='O3 RETORNO GLOBAL MASTER FIM'].reset_index().iloc[0]['position_close']
#books
books_list = ['MACRO_O3','QUANT','TGC OFF','TOP PICKS TENDENCIAS','TRM']
books_pct = pd.DataFrame([['MACRO_O3',0.25],['QUANT',0.05],['TGC OFF',0.15],['TOP PICKS TENDENCIAS',0.05],['TRM',0.10]],columns=['Book','pct'])
#exposao
explosao = pd.read_sql("SELECT * FROM proporcao_fundos WHERE fundo_destino='O3 RETORNO GLOBAL MASTER FIM' OR fundo_destino='O3 RETORNO GLOBAL QUALIFICADO MASTER FIM';",con=engine2)
explosao  = explosao [explosao ['fundo_origem']=='O3 MACRO INTERNATIONAL FUND']
exp_quali = explosao [explosao ['fundo_destino']=='O3 RETORNO GLOBAL QUALIFICADO MASTER FIM'].sort_values('data_ref').tail(1).reset_index().iloc[0]['proporcao']/100
exp_geral = explosao [explosao ['fundo_destino']=='O3 RETORNO GLOBAL MASTER FIM'].sort_values('data_ref').tail(1).reset_index().iloc[0]['proporcao']/100
#cambio
dolar = pd.read_sql("SELECT val_date,px_last FROM precos WHERE ticker='USDBRL REGN Curncy'", con=engine)
dolar ['val_date'] = pd.to_datetime(dolar ['val_date'])
dolar = dolar.rename(columns={'px_last':'dolar','val_date':'Date'})
                                                                                                                                                   
quali_books = pd.read_csv('I:/Riscos/TotalVaR/historico/' + 'macro' + '_nivel2_' + yesterday_arq + '.txt', sep='\t',parse_dates=['Date'])

quali_fundo = quali_books.drop(columns=['Book']).groupby('Date').sum().reset_index()
quali_fundo = quali_fundo.merge(dolar,how='left')
quali_fundo['dolar'] = quali_fundo['dolar'].fillna(method='ffill')
geral_fundo = quali_fundo.copy()

quali_fundo['ret_fundo'] = (quali_fundo['TotalVar']*quali_fundo['dolar']*exp_quali)/pl_quali
geral_fundo['ret_fundo'] = (geral_fundo['TotalVar']*geral_fundo['dolar']*exp_geral)/pl_geral

quali_fundo = quali_fundo[['Date','ret_fundo']]
geral_fundo = geral_fundo[['Date','ret_fundo']]

quali_books = quali_books.merge(books_pct,how='outer').dropna()
quali_books = quali_books.merge(dolar,how='left')
quali_books['dolar'] = quali_books['dolar'].fillna(method='ffill')
geral_books = quali_books.copy()

quali_books ['ret'] = (quali_books['TotalVar']*quali_books['dolar']*exp_quali)/(pl_quali*quali_books['pct'])
geral_books ['ret'] = (geral_books['TotalVar']*geral_books['dolar']*exp_geral)/(pl_geral*geral_books['pct'])

quali_books = quali_books[['Book','Date','ret']]  
geral_books = geral_books[['Book','Date','ret']]  




spx = pd.read_sql("SELECT px_last,val_date FROM precos WHERE ticker='SPX Index';", con=engine)
spx['val_date'] = pd.to_datetime(spx['val_date'])
spx['SPX'] = spx['px_last']/spx['px_last'].shift(1)-1
spx = spx.drop(columns='px_last')
def matriz_correl(books,fundo):
    books2 = books.pivot_table(values='ret',index=books.Date,columns='Book',aggfunc='first').reset_index()
    books2 = books2.merge(fundo).rename(columns={'ret_fundo':'fundo'})
    books2 = books2.merge(spx, right_on = 'val_date', left_on='Date',how='left')
    books2 = books2.drop(columns='val_date') 
    books2 = books2.fillna(0)
    books2 = books2.sort_values('Date')
    matriz_12m = books2.sort_values('Date',ascending=False).head(252).corr()
    matriz_12m.index.name = None
    matriz_12m = matriz_12m .stack().reset_index()
    matriz_12m = matriz_12m.rename(columns={'level_0':'book','level_1':'book2',0:'correl'})
    matriz_12m ['janela'] = '12m' 
    
    matriz_24m = books2.sort_values('Date',ascending=False).head(504).corr()
    matriz_24m.index.name = None
    matriz_24m = matriz_24m .stack().reset_index()
    matriz_24m = matriz_24m.rename(columns={'level_0':'book','level_1':'book2',0:'correl'})
    matriz_24m ['janela'] = '24m'
    return(matriz_12m,matriz_24m)
    
matriz_quali_12,matriz_quali_24 = matriz_correl(quali_books, quali_fundo)
matriz_geral_12,matriz_geral_24 = matriz_correl(geral_books, geral_fundo)

matriz_quali_12['trading_desk'] = 'rgq'
matriz_quali_24['trading_desk'] = 'rgq'
matriz_geral_12['trading_desk'] = 'rgm'
matriz_geral_24['trading_desk'] = 'rgm'
matriz_final = matriz_quali_12.append([matriz_quali_24,matriz_geral_12,matriz_geral_24]) 
matriz_final ['data_base'] = yesterday
matriz_final['book']= matriz_final['book'].str.lower()
matriz_final['book2']= matriz_final['book2'].str.lower()
matriz_final = matriz_final.replace(' ', '_', regex=True)
matriz_final = matriz_final.replace('_tendencias', '', regex=True)
matriz_final = matriz_final.replace('_o3', '', regex=True)

matriz_final.to_sql('correl_books', 
              con=engine, 
              index=False, 
              if_exists='append')
