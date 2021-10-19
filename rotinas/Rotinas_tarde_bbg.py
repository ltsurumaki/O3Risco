
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 18 13:53:57 2021

@author: ludmilla.tsurumaki
"""

import os
os.chdir(r'I:\Riscos\Codigos Automacao')

import pandas as pd
import datetime as dt
from pandas.tseries.offsets import BDay
import telegram_send
import numpy as np
import math
from wraper import *
from sqlalchemy import create_engine


lastBusDay = dt.datetime.today()
yesterday = lastBusDay - BDay(1)
yesterday = dt.datetime(yesterday.year,yesterday.month,yesterday.day)
data_csv = yesterday.strftime("%d%b%Y")
yesterday_bbg = yesterday.strftime("%m/%d/%Y")
yesterday_2 = yesterday.strftime("%Y%m%d")
data_csv = yesterday.strftime("%d%b%Y")

def recupera_bd(tablename):
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
    df = pd.read_sql(tablename, con=engine)
    return df 
def to_alchemy_append(df,tablename):
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
    df.to_sql(tablename, 
                  con=engine, 
                  index=False, 
                  if_exists='append') 
    return 0
#---------------------------- Rotinas ja rodaram?------------------------------
shorts = recupera_bd('shorts').tail(1)
ultima_data = shorts['val_date'].iloc[-1]
#---------------------------------- Rotinas -----------------------------------
def tunel_precos():

       #------------------------Importação dos Dados-----------------------
    fundos = ['rgq','rgm','ms1','ms2','tge','mif']#,'glf']
    pasta_credito = 'I:/Riscos/Ludmilla/Distribuicao_Ordens/Historico/'
    tabela = pd.DataFrame()
    data_csv = yesterday.strftime("%Y%m%d")
    
    for i in range(len(fundos)):
        temp_df = pd.read_csv(pasta_credito + 'preco_medio_' + fundos[i]+'_'+ data_csv +'.txt', sep='\t')
        temp_df['tradingdesk'] = fundos[i]
        tabela = tabela.append(temp_df)
    #-----------------------------Rotina-------------------------------
    filtro = ~tabela['ProductClass'].isin(['Cash', 'Overnight','Provisions and Costs','Currencies NDF'])
    tabela = tabela.loc[filtro]
    tabela = tabela[['Product','Currency','Amount','Price']]
    amount = tabela[['Product','Amount']].groupby('Product').sum().reset_index()
    amount['Amount'] = abs(amount['Amount'])
    amount = amount.rename(columns={'Amount':'Volume'})
    tabela = tabela.merge(amount)
    n = tabela[['Product','Price']].groupby(['Product']).count().reset_index()
    n = n.rename(columns={'Price': 'Count'})
    tabela = tabela.merge(n)
    tabela['Amount'] = abs(tabela['Amount'])
    
    depara = pd.read_csv('I:\Riscos\Liquidez\DePara.csv',header=(0))
    tabela_final = tabela.merge(depara,how='left',left_on=['Product'],right_on=['Product'])
    tabela_final = tabela_final.dropna(axis=0)
    
    def funcbbg_high(x,data,moeda):
        try:
            return BBG.fetch_series(x, "PX_High", data, data, fx = moeda ).iloc[0,0]
        except:
            return 0
    
    tabela_final['price_high'] = tabela_final.apply(lambda x: funcbbg_high(x['Bloomberg'],yesterday_bbg,x['Currency']),axis=1)
    
    def funcbbg_low(x,data,moeda):
        try:
            return BBG.fetch_series(x, "PX_Low", data, data, fx = moeda ).iloc[0,0]
        except:
            return 0
    
    tabela_final['price_low'] = tabela_final.apply(lambda x: funcbbg_low(x['Bloomberg'],yesterday_bbg,x['Currency']),axis=1)
    
    def funcbbg_volume(x,data,moeda):
        try:
            return BBG.fetch_series(x, "PX_Volume", data, data, fx = moeda ).iloc[0,0]
        except:
            return 0
    
    tabela_final['px_volume'] = tabela_final.apply(lambda x: funcbbg_volume(x['Bloomberg'],yesterday_bbg,x['Currency']),axis=1)

    tabela_final['confere_preco'] = np.where((tabela_final['Price'] > tabela_final['price_high']) | (tabela_final['Price'] < tabela_final['price_low']),'Verificar','OK')
    pct_limite = 20/100
    tabela_final['confere_volume'] = np.where(tabela_final['Amount'] > tabela_final['px_volume'] * pct_limite,'Verificar','OK')
    #tabela_final['Data'] = dt.datetime(yesterday.year, yesterday.month, yesterday.day)
    tabela_final['val_date'] = dt.datetime(yesterday.year, yesterday.month, yesterday.day)
    tabela_final = tabela_final.rename(columns={'Product': 'product'})
    tabela_final = tabela_final.rename(columns={'Currency': 'currency'})
    tabela_final = tabela_final.rename(columns={'Amount': 'amount'})
    tabela_final = tabela_final.rename(columns={'Price': 'price'})
    tabela_final = tabela_final.rename(columns={'Count': 'count'})
    tabela_final = tabela_final.rename(columns={'Bloomberg': 'bloomberg'})
    tabela_final = tabela_final.drop(columns={'Volume'})
    
    tabela_final['obs'] = '' 
    
    
    ok = tabela_final[tabela_final['confere_preco']=='OK']
    ok = ok.drop(columns={'confere_volume'})
    
    #----------------Subindo na base-----------------------
    to_alchemy_append(ok,'tunel_precos')
    #backup provisorio
    tabela_final.to_excel('I:/Riscos/Tunel de Preço/Historico/tunel_preco_' + data_csv + '.xlsx' )
    
    print('Tunel OK.')
    return (tabela_final)


def liquidez():

    data_csv = yesterday.strftime("%d%b%Y")
    base_liqui = pd.read_csv('I:\Riscos\Liquidez\Base\OverviewReport_' + data_csv +'.txt',sep='\t',header=(0))
    base_liqui1 = base_liqui[['TradingDesk','ClassID','Product','ExpiryDate','Amount','Position']]
    #----------------------------------------------------------------------
    depara = pd.read_csv('I:\Riscos\Liquidez\DePara.csv',header=(0))
    
    base_liqui2 = base_liqui1.merge(depara,how='left',left_on=['Product'],right_on=['Product'])
    
    
    base_liqui2['Ativo_100p'] = base_liqui2['ClassID'].apply(lambda x: 1 if x == 153 or x ==  138 or \
                                                                  x == 70 \
                                                                  or x == 22 or x == 162 \
                                                                  else 0)
    
    base_aux1 = base_liqui2[['Product','Bloomberg']]
    
    base_aux1 = base_aux1.drop_duplicates()
    
    base_aux1 = base_aux1.dropna(axis=0)
    
    def funcbbg(x):
        try:
            return BBG.fetch_contract_parameter(x, "Volume_AVG_6M").iloc[0,0]
        except:
            return 0
    
    def funcbbg2(x):
        try:
            return BBG.fetch_contract_parameter(x, "Volume_AVG_30D").iloc[0,0]
        except:
            return 0
    #--------------------------------- 6 meses -------------------------------
    base_aux1['volume_p'] = base_aux1['Bloomberg'].apply(lambda x: funcbbg(x))
    
    base_liqui3 = base_liqui2.merge(base_aux1,how='left',left_on=['Bloomberg'],right_on=['Bloomberg'])
    
    base_liqui3 = base_liqui3.drop(['Product_y'], axis=1)
    
    base_liqui3['volume_pp'] = base_liqui3['volume_p'] / 5
    base_liqui3.loc[base_liqui3['volume_pp'].isnull(), 'volume_pp'] = 0
    
    base_liqui3['volume_s'] = base_liqui3['volume_pp'] / 2
    base_liqui3.loc[base_liqui3['volume_s'].isnull(), 'volume_s'] = 0
    
    base_liqui3['Liq_P'] = base_liqui3['Amount'].abs()/base_liqui3['volume_pp']
    base_liqui3['Liq_P'] = base_liqui3.apply(lambda x: x['Liq_P'] if x['Ativo_100p'] == 0 else 0,axis=1)
    base_liqui3['ExpiryDate'] = pd.to_datetime(base_liqui['ExpiryDate'])
    base_liqui3['aux'] = ((base_liqui3['ExpiryDate'] - yesterday).dt.days)*(252/365)
    base_liqui3['Liq_P'] = np.where(base_liqui3['Liq_P'] == np.inf,base_liqui3['aux'],base_liqui3['Liq_P'])
    base_liqui3['Liq_P'] = np.where(base_liqui3['Liq_P'] == math.nan ,252,base_liqui3['Liq_P'])
    #base_liqui3.loc[base_liqui3['Liq_P'].isnull(), 'Liq_P'] = 252        
    base_liqui3['Liq_S'] = base_liqui3['Amount'].abs()/base_liqui3['volume_s']
    base_liqui3['Liq_S'] = base_liqui3.apply(lambda x: x['Liq_S'] if x['Ativo_100p'] == 0 else 0,axis=1)
   
        
    #---------------------------------------------------------------------
    base_liqui3['Position'] = base_liqui3['Position'].abs()
    
    
    
    def liqq_P(x,y,w):
        return x * w/y
    
    #cenario padrao
    base_liqui_con_p = base_liqui3.groupby(['TradingDesk','Product_x'])[['Position','Liq_P']].sum().reset_index()
    
    base_liqui_con_p['1du'] = base_liqui_con_p.apply(lambda x: liqq_P(x['Position'],x['Liq_P'],1) if x['Liq_P'] \
                                                    > 1 else x['Position'],axis=1)
    
    base_liqui_con_p['5du'] = base_liqui_con_p.apply(lambda x: liqq_P(x['Position'],x['Liq_P'],5) if x['Liq_P'] \
                                                    > 5 else x['Position'],axis=1)
    
    base_liqui_con_p['10du'] = base_liqui_con_p.apply(lambda x: liqq_P(x['Position'],x['Liq_P'],10) if x['Liq_P'] \
                                                    > 10 else x['Position'],axis=1)
    
    base_liqui_con_p['15du'] = base_liqui_con_p.apply(lambda x: liqq_P(x['Position'],x['Liq_P'],15) if x['Liq_P'] \
                                                    > 15 else x['Position'],axis=1)
    
    base_liqui_con_p['21du'] = base_liqui_con_p.apply(lambda x: liqq_P(x['Position'],x['Liq_P'],21) if x['Liq_P'] \
                                                    > 21 else x['Position'],axis=1)
    
    base_liqui_con_p['42du'] = base_liqui_con_p.apply(lambda x: liqq_P(x['Position'],x['Liq_P'],42) if x['Liq_P'] \
                                                    > 42 else x['Position'],axis=1)
    
    base_liqui_con_p['63du'] = base_liqui_con_p.apply(lambda x: liqq_P(x['Position'],x['Liq_P'],63) if x['Liq_P'] \
                                                    > 63 else x['Position'],axis=1)
    
    base_liqui_con_p['126du'] = base_liqui_con_p.apply(lambda x: liqq_P(x['Position'],x['Liq_P'],126) if x['Liq_P'] \
                                                    > 126 else x['Position'],axis=1)
    
    base_liqui_con_p['252du'] = base_liqui_con_p.apply(lambda x: liqq_P(x['Position'],x['Liq_P'],252) if x['Liq_P'] \
                                                    > 252 else x['Position'],axis=1)
    
    base_liqui_con_p1 = base_liqui_con_p.groupby(['TradingDesk'])[['Position','1du','5du','10du','15du','21du','42du', \
                                                                  '63du','126du','252du']].sum().reset_index()
    
    base_liqui_con_p1['1du'] =  base_liqui_con_p1['1du']/base_liqui_con_p1['Position']
    base_liqui_con_p1['5du'] =  base_liqui_con_p1['5du']/base_liqui_con_p1['Position']
    base_liqui_con_p1['10du'] =  base_liqui_con_p1['10du']/base_liqui_con_p1['Position']
    base_liqui_con_p1['15du'] =  base_liqui_con_p1['15du']/base_liqui_con_p1['Position']
    base_liqui_con_p1['21du'] =  base_liqui_con_p1['21du']/base_liqui_con_p1['Position']
    base_liqui_con_p1['42du'] =  base_liqui_con_p1['42du']/base_liqui_con_p1['Position']
    base_liqui_con_p1['63du'] =  base_liqui_con_p1['63du']/base_liqui_con_p1['Position']
    base_liqui_con_p1['126du'] =  base_liqui_con_p1['126du']/base_liqui_con_p1['Position']
    base_liqui_con_p1['252du'] =  base_liqui_con_p1['252du']/base_liqui_con_p1['Position']
    
    
    base_liqui_con_p_fim = base_liqui_con_p1[['TradingDesk','1du','5du','10du','15du','21du' \
                                                 ,'42du','63du','126du','252du']]
    
    
    #cenario stress
    
    base_liqui_con_s = base_liqui3.groupby(['TradingDesk','Product_x'])[['Position','Liq_S']].sum().reset_index()
    
    base_liqui_con_s['1du'] = base_liqui_con_s.apply(lambda x: liqq_P(x['Position'],x['Liq_S'],1) if x['Liq_S'] \
                                                    > 1 else x['Position'],axis=1)
    
    base_liqui_con_s['5du'] = base_liqui_con_s.apply(lambda x: liqq_P(x['Position'],x['Liq_S'],5) if x['Liq_S'] \
                                                    > 5 else x['Position'],axis=1)
    
    base_liqui_con_s['10du'] = base_liqui_con_s.apply(lambda x: liqq_P(x['Position'],x['Liq_S'],10) if x['Liq_S'] \
                                                    > 10 else x['Position'],axis=1)
    
    base_liqui_con_s['15du'] = base_liqui_con_s.apply(lambda x: liqq_P(x['Position'],x['Liq_S'],15) if x['Liq_S'] \
                                                    > 15 else x['Position'],axis=1)
    
    base_liqui_con_s['21du'] = base_liqui_con_s.apply(lambda x: liqq_P(x['Position'],x['Liq_S'],21) if x['Liq_S'] \
                                                    > 21 else x['Position'],axis=1)
    
    base_liqui_con_s['42du'] = base_liqui_con_s.apply(lambda x: liqq_P(x['Position'],x['Liq_S'],42) if x['Liq_S'] \
                                                    > 42 else x['Position'],axis=1)
    
    base_liqui_con_s['63du'] = base_liqui_con_s.apply(lambda x: liqq_P(x['Position'],x['Liq_S'],63) if x['Liq_S'] \
                                                    > 63 else x['Position'],axis=1)
    
    base_liqui_con_s['126du'] = base_liqui_con_s.apply(lambda x: liqq_P(x['Position'],x['Liq_S'],126) if x['Liq_S'] \
                                                    > 126 else x['Position'],axis=1)
    
    base_liqui_con_s['252du'] = base_liqui_con_s.apply(lambda x: liqq_P(x['Position'],x['Liq_S'],252) if x['Liq_S'] \
                                                    > 252 else x['Position'],axis=1)
    
    base_liqui_con_s1 = base_liqui_con_s.groupby(['TradingDesk'])[['Position','1du','5du','10du','15du','21du','42du', \
                                                                  '63du','126du','252du']].sum().reset_index()
    
    base_liqui_con_s1['1du'] =  base_liqui_con_s1['1du']/base_liqui_con_s1['Position']
    base_liqui_con_s1['5du'] =  base_liqui_con_s1['5du']/base_liqui_con_s1['Position']
    base_liqui_con_s1['10du'] =  base_liqui_con_s1['10du']/base_liqui_con_s1['Position']
    base_liqui_con_s1['15du'] =  base_liqui_con_s1['15du']/base_liqui_con_s1['Position']
    base_liqui_con_s1['21du'] =  base_liqui_con_s1['21du']/base_liqui_con_s1['Position']
    base_liqui_con_s1['42du'] =  base_liqui_con_s1['42du']/base_liqui_con_s1['Position']
    base_liqui_con_s1['63du'] =  base_liqui_con_s1['63du']/base_liqui_con_s1['Position']
    base_liqui_con_s1['126du'] =  base_liqui_con_s1['126du']/base_liqui_con_s1['Position']
    base_liqui_con_s1['252du'] =  base_liqui_con_s1['252du']/base_liqui_con_s1['Position']
    
    
    base_liqui_con_s_fim = base_liqui_con_s1[['TradingDesk','1du','5du','10du','15du','21du' \
                                                 ,'42du','63du','126du','252du']]
    
    #Adicionando data
    base_liqui_con_p_fim['val_date'] = yesterday
    base_liqui_con_s_fim['val_date'] = yesterday
    
    #de para: fundos
    de_para_fundos = pd.read_excel('I:/Riscos/Ludmilla/de_para_fundos.xlsx')
    base_liqui_con_p_fim = base_liqui_con_p_fim.merge(de_para_fundos,how='left',left_on=['TradingDesk'],right_on=['TradingDesk'])
    base_liqui_con_s_fim = base_liqui_con_s_fim.merge(de_para_fundos,how='left',left_on=['TradingDesk'],right_on=['TradingDesk'])
    #export csv TIRAR
    base_liqui_con_p_fim.to_csv("I:\Riscos\Liquidez\Arq_Python\Liqui_P.csv",index=None, header=True,sep="\t")
    base_liqui_con_s_fim.to_csv("I:\Riscos\Liquidez\Arq_Python\Liqui_S.csv",index=None, header=True,sep="\t")
    # Ajustes
    base_liqui_con_p_fim = base_liqui_con_p_fim.drop(['TradingDesk'], axis = 1) 
    base_liqui_con_p_fim['cenario'] = 'padrao'
    base_liqui_con_s_fim = base_liqui_con_s_fim.drop(['TradingDesk'], axis = 1)
    base_liqui_con_s_fim['cenario'] = 'stress'
    #resumo
    tabela_6m = pd.DataFrame()
    tabela_6m = tabela_6m.append(base_liqui_con_p_fim)
    tabela_6m = tabela_6m.append(base_liqui_con_s_fim)
    
    
    #--------------------------------- 30 dias -------------------------------
    base_aux1['volume_p'] = base_aux1['Bloomberg'].apply(lambda x: funcbbg2(x))
    
    base_liqui3 = base_liqui2.merge(base_aux1,how='left',left_on=['Bloomberg'],right_on=['Bloomberg'])
    
    base_liqui3 = base_liqui3.drop(['Product_y'], axis=1)
    
    base_liqui3['volume_pp'] = base_liqui3['volume_p'] / 5
    base_liqui3.loc[base_liqui3['volume_pp'].isnull(), 'volume_pp'] = 0
    
    base_liqui3['volume_s'] = base_liqui3['volume_pp'] / 2
    base_liqui3.loc[base_liqui3['volume_s'].isnull(), 'volume_s'] = 0
    
    base_liqui3['Liq_P'] = base_liqui3['Amount'].abs()/base_liqui3['volume_pp']
    base_liqui3['Liq_P'] = base_liqui3.apply(lambda x: x['Liq_P'] if x['Ativo_100p'] == 0 else 0,axis=1)
    base_liqui3['ExpiryDate'] = pd.to_datetime(base_liqui['ExpiryDate'])
    base_liqui3['aux'] = ((base_liqui3['ExpiryDate'] - yesterday).dt.days)*(252/365)
    base_liqui3['Liq_P'] = np.where(base_liqui3['Liq_P'] == np.inf,base_liqui3['aux'],base_liqui3['Liq_P'])
    base_liqui3['Liq_P'] = np.where(base_liqui3['Liq_P'] == math.nan ,252,base_liqui3['Liq_P'])
    #base_liqui3.loc[base_liqui3['Liq_P'].isnull(), 'Liq_P'] = 252        
    base_liqui3['Liq_S'] = base_liqui3['Amount'].abs()/base_liqui3['volume_s']
    base_liqui3['Liq_S'] = base_liqui3.apply(lambda x: x['Liq_S'] if x['Ativo_100p'] == 0 else 0,axis=1)
   
        
    #---------------------------------------------------------------------
    base_liqui3['Position'] = base_liqui3['Position'].abs()
    
    #cenario padrao
    base_liqui_con_p = base_liqui3.groupby(['TradingDesk','Product_x'])[['Position','Liq_P']].sum().reset_index()
    
    base_liqui_con_p['1du'] = base_liqui_con_p.apply(lambda x: liqq_P(x['Position'],x['Liq_P'],1) if x['Liq_P'] \
                                                    > 1 else x['Position'],axis=1)
    
    base_liqui_con_p['5du'] = base_liqui_con_p.apply(lambda x: liqq_P(x['Position'],x['Liq_P'],5) if x['Liq_P'] \
                                                    > 5 else x['Position'],axis=1)
    
    base_liqui_con_p['10du'] = base_liqui_con_p.apply(lambda x: liqq_P(x['Position'],x['Liq_P'],10) if x['Liq_P'] \
                                                    > 10 else x['Position'],axis=1)
    
    base_liqui_con_p['15du'] = base_liqui_con_p.apply(lambda x: liqq_P(x['Position'],x['Liq_P'],15) if x['Liq_P'] \
                                                    > 15 else x['Position'],axis=1)
    
    base_liqui_con_p['21du'] = base_liqui_con_p.apply(lambda x: liqq_P(x['Position'],x['Liq_P'],21) if x['Liq_P'] \
                                                    > 21 else x['Position'],axis=1)
    
    base_liqui_con_p['42du'] = base_liqui_con_p.apply(lambda x: liqq_P(x['Position'],x['Liq_P'],42) if x['Liq_P'] \
                                                    > 42 else x['Position'],axis=1)
    
    base_liqui_con_p['63du'] = base_liqui_con_p.apply(lambda x: liqq_P(x['Position'],x['Liq_P'],63) if x['Liq_P'] \
                                                    > 63 else x['Position'],axis=1)
    
    base_liqui_con_p['126du'] = base_liqui_con_p.apply(lambda x: liqq_P(x['Position'],x['Liq_P'],126) if x['Liq_P'] \
                                                    > 126 else x['Position'],axis=1)
    
    base_liqui_con_p['252du'] = base_liqui_con_p.apply(lambda x: liqq_P(x['Position'],x['Liq_P'],252) if x['Liq_P'] \
                                                    > 252 else x['Position'],axis=1)
    
    base_liqui_con_p1 = base_liqui_con_p.groupby(['TradingDesk'])[['Position','1du','5du','10du','15du','21du','42du', \
                                                                  '63du','126du','252du']].sum().reset_index()
    
    base_liqui_con_p1['1du'] =  base_liqui_con_p1['1du']/base_liqui_con_p1['Position']
    base_liqui_con_p1['5du'] =  base_liqui_con_p1['5du']/base_liqui_con_p1['Position']
    base_liqui_con_p1['10du'] =  base_liqui_con_p1['10du']/base_liqui_con_p1['Position']
    base_liqui_con_p1['15du'] =  base_liqui_con_p1['15du']/base_liqui_con_p1['Position']
    base_liqui_con_p1['21du'] =  base_liqui_con_p1['21du']/base_liqui_con_p1['Position']
    base_liqui_con_p1['42du'] =  base_liqui_con_p1['42du']/base_liqui_con_p1['Position']
    base_liqui_con_p1['63du'] =  base_liqui_con_p1['63du']/base_liqui_con_p1['Position']
    base_liqui_con_p1['126du'] =  base_liqui_con_p1['126du']/base_liqui_con_p1['Position']
    base_liqui_con_p1['252du'] =  base_liqui_con_p1['252du']/base_liqui_con_p1['Position']
    
    
    base_liqui_con_p_fim = base_liqui_con_p1[['TradingDesk','1du','5du','10du','15du','21du' \
                                                 ,'42du','63du','126du','252du']]
    
    
    #cenario stress
    
    base_liqui_con_s = base_liqui3.groupby(['TradingDesk','Product_x'])[['Position','Liq_S']].sum().reset_index()
    
    base_liqui_con_s['1du'] = base_liqui_con_s.apply(lambda x: liqq_P(x['Position'],x['Liq_S'],1) if x['Liq_S'] \
                                                    > 1 else x['Position'],axis=1)
    
    base_liqui_con_s['5du'] = base_liqui_con_s.apply(lambda x: liqq_P(x['Position'],x['Liq_S'],5) if x['Liq_S'] \
                                                    > 5 else x['Position'],axis=1)
    
    base_liqui_con_s['10du'] = base_liqui_con_s.apply(lambda x: liqq_P(x['Position'],x['Liq_S'],10) if x['Liq_S'] \
                                                    > 10 else x['Position'],axis=1)
    
    base_liqui_con_s['15du'] = base_liqui_con_s.apply(lambda x: liqq_P(x['Position'],x['Liq_S'],15) if x['Liq_S'] \
                                                    > 15 else x['Position'],axis=1)
    
    base_liqui_con_s['21du'] = base_liqui_con_s.apply(lambda x: liqq_P(x['Position'],x['Liq_S'],21) if x['Liq_S'] \
                                                    > 21 else x['Position'],axis=1)
    
    base_liqui_con_s['42du'] = base_liqui_con_s.apply(lambda x: liqq_P(x['Position'],x['Liq_S'],42) if x['Liq_S'] \
                                                    > 42 else x['Position'],axis=1)
    
    base_liqui_con_s['63du'] = base_liqui_con_s.apply(lambda x: liqq_P(x['Position'],x['Liq_S'],63) if x['Liq_S'] \
                                                    > 63 else x['Position'],axis=1)
    
    base_liqui_con_s['126du'] = base_liqui_con_s.apply(lambda x: liqq_P(x['Position'],x['Liq_S'],126) if x['Liq_S'] \
                                                    > 126 else x['Position'],axis=1)
    
    base_liqui_con_s['252du'] = base_liqui_con_s.apply(lambda x: liqq_P(x['Position'],x['Liq_S'],252) if x['Liq_S'] \
                                                    > 252 else x['Position'],axis=1)
    
    base_liqui_con_s1 = base_liqui_con_s.groupby(['TradingDesk'])[['Position','1du','5du','10du','15du','21du','42du', \
                                                                  '63du','126du','252du']].sum().reset_index()
    
    base_liqui_con_s1['1du'] =  base_liqui_con_s1['1du']/base_liqui_con_s1['Position']
    base_liqui_con_s1['5du'] =  base_liqui_con_s1['5du']/base_liqui_con_s1['Position']
    base_liqui_con_s1['10du'] =  base_liqui_con_s1['10du']/base_liqui_con_s1['Position']
    base_liqui_con_s1['15du'] =  base_liqui_con_s1['15du']/base_liqui_con_s1['Position']
    base_liqui_con_s1['21du'] =  base_liqui_con_s1['21du']/base_liqui_con_s1['Position']
    base_liqui_con_s1['42du'] =  base_liqui_con_s1['42du']/base_liqui_con_s1['Position']
    base_liqui_con_s1['63du'] =  base_liqui_con_s1['63du']/base_liqui_con_s1['Position']
    base_liqui_con_s1['126du'] =  base_liqui_con_s1['126du']/base_liqui_con_s1['Position']
    base_liqui_con_s1['252du'] =  base_liqui_con_s1['252du']/base_liqui_con_s1['Position']
    
    
    base_liqui_con_s_fim = base_liqui_con_s1[['TradingDesk','1du','5du','10du','15du','21du' \
                                                 ,'42du','63du','126du','252du']]
    
    #Adicionando data
    base_liqui_con_p_fim['val_date'] = yesterday
    base_liqui_con_s_fim['val_date'] = yesterday
    
    #de para: fundos
    de_para_fundos = pd.read_excel('I:/Riscos/Ludmilla/de_para_fundos.xlsx')
    base_liqui_con_p_fim = base_liqui_con_p_fim.merge(de_para_fundos,how='left',left_on=['TradingDesk'],right_on=['TradingDesk'])
    base_liqui_con_s_fim = base_liqui_con_s_fim.merge(de_para_fundos,how='left',left_on=['TradingDesk'],right_on=['TradingDesk'])
    #export csv TIRAR
    base_liqui_con_p_fim.to_csv("I:\Riscos\Liquidez\Arq_Python\Liqui_P.csv",index=None, header=True,sep="\t")
    base_liqui_con_s_fim.to_csv("I:\Riscos\Liquidez\Arq_Python\Liqui_S.csv",index=None, header=True,sep="\t")
    # Ajustes
    base_liqui_con_p_fim = base_liqui_con_p_fim.drop(['TradingDesk'], axis = 1) 
    base_liqui_con_p_fim['cenario'] = 'padrao'
    base_liqui_con_s_fim = base_liqui_con_s_fim.drop(['TradingDesk'], axis = 1)
    base_liqui_con_s_fim['cenario'] = 'stress'
    #resumo
    #resumo
    tabela_30d = pd.DataFrame()
    tabela_30d = tabela_30d.append(base_liqui_con_p_fim)
    tabela_30d = tabela_30d.append(base_liqui_con_s_fim)
    #----------------------Exportacao para a base-------------------------------------------
    janelas = ['1du','5du','10du','15du','21du','42du','63du','126du','252du']
    for janela in janelas:
        tabela_30d[janela + '_final'] = np.where(tabela_6m[janela]<tabela_30d[janela],tabela_6m[janela],tabela_30d[janela])
        tabela_30d[janela] = tabela_30d[janela+ '_final'] 
    tabela = tabela_30d.drop(['1du_final','5du_final','10du_final','15du_final','21du_final','42du_final','63du_final','126du_final','252du_final'],axis = 1)

    
    to_alchemy_append(tabela,'liquidez')
    #-------------------------Backup Provisorio--------------------------
    backup = recupera_bd('liquidez')
    backup.to_csv('I:/Riscos/Liquidez/Backup/backup_liquidez.csv')
    
    print('Liquidez OK!')
    return (tabela)

def shorts():
    lastBusDay = dt.datetime.today()
    data_csv = yesterday.strftime("%d%b%Y")
    today = dt.datetime.today().strftime("%d-%m-%Y")
    #---------------------Importacao de bases-------------------------- 
    base_lote = pd.read_csv('I:\Riscos\Shorts\Base\FundOverviewByPrimitive_' + data_csv +'.txt',sep='\t',header=(0))
    base_JP = pd.read_excel('I:/Pedro.Silva/leverage_logs/Alavancagem_Off_'+ today + '_O3.xls')                       
    depara = pd.read_csv('I:\Riscos\Liquidez\DePara.csv',header=(0))
    
    #----------------------------Rotina--------------------------------
    #filtrando dados
    tabela_equity =  base_lote[base_lote['ProductClass'].str.contains('Equity')]
    tabela_etf =  base_lote[base_lote['ProductClass']=='ETF']
    tabela_adr =  base_lote[base_lote['ProductClass']=='ADR']
    tabela = tabela_equity.append(tabela_etf) 
    tabela = tabela.append(tabela_adr)
    
    qt = tabela[['Product','ProductClass','Amount']].groupby(['Product','ProductClass']).sum().reset_index()
    posicao = tabela[['Product','ProductClass','Position']].groupby(['Product','ProductClass']).sum().reset_index()
    
    tabela_final = qt.merge(posicao)
    tabela_final = tabela_final[tabela_final['Amount']<0]
    depara = pd.read_csv('I:\Riscos\Liquidez\DePara.csv',header=(0))
    tabela_final = tabela_final.merge(depara,how='left',left_on=['Product'],right_on=['Product'])
    tabela_final = tabela_final.dropna(axis=0)
    #Bloomberg
    
    def funcbbg(x,field):
        try:
            return BBG.fetch_contract_parameter(x, field).iloc[0,0]
        except:
            return 0
        
    def funcbbg_si(x):
        try:
            return BBG.fetch_contract_parameter(x, 'si_percent_equity_float').iloc[0,0]
        except:
            erro = 0
            return erro
        
    tabela_final['volume'] = tabela_final.apply(lambda x: funcbbg(x['Bloomberg'],'Volume_AVG_3M'),axis=1)
    tabela_final['volume']  = tabela_final['volume'] / 5
    tabela_final['mkt_cap'] = tabela_final.apply(lambda x: funcbbg(x['Bloomberg'],'cur_mkt_cap'),axis=1)
    tabela_final['mkt_cap'] = tabela_final['mkt_cap'] / 1000000000
    tabela_final['si']  = tabela_final.apply(lambda x: funcbbg_si(x['Bloomberg']),axis=1)
    tabela_final['si'] = tabela_final['si']/100
    tabela_final['si'] = tabela_final['si'].apply(lambda x: np.nan if x == 0 else x)
    tabela_final['days_to_cover'] = tabela_final['Amount'].abs()/tabela_final['volume']
    tabela_final['pct_mkt_cap'] = (tabela_final['Position']/1000000000)/tabela_final['mkt_cap']
    
    margem = tabela_final.merge(base_JP,'outer',left_on='Product', right_on='Ticker')
    margem['margem'] = margem['Requirement']/margem['Market Value'].abs()
    margem =margem[['Product','margem']]
    tabela_final = tabela_final.merge(margem)
    tabela_final['val_date'] = yesterday
    tabela_final = tabela_final.rename(columns={'Product': 'product'})
    tabela_final = tabela_final.rename(columns={'Position': 'position'})
    tabela_final = tabela_final.rename(columns={'ProductClass': 'product_class'})
    tabela_final = tabela_final.rename(columns={'Currency': 'currency'})
    tabela_final = tabela_final.rename(columns={'Amount': 'amount'})
    tabela_final = tabela_final.rename(columns={'Price': 'price'})
    tabela_final = tabela_final.rename(columns={'Bloomberg': 'bloomberg'})
    
    #------------------Subindo na base---------------------
    to_alchemy_append(tabela_final,'shorts')
    #backup provisorio
    backup = recupera_bd('shorts')
    backup.to_csv('I:/Riscos/Shorts/Backup/backup_shorts.csv')
    
    print('Shorts OK')
    return (tabela_final)

if ultima_data == yesterday:
    telegram_send.send(messages=["Rotinas da tarde ja rodaram!"])
else:
    telegram_send.send(messages=["Rodando rotinas da tarde..."])
    a = tunel_precos()
    b = liquidez()
    c = shorts()
    telegram_send.send(messages=["Rotinas da tarde OK!"])

