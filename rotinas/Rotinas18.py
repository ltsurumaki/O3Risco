# -*- coding: utf-8 -*-
"""
Created on Mon Mar 29 09:21:49 2021

@author: ludmilla.tsurumaki

v6: inclusao de todos os fundos no sharpe e na vol 126 
v7: inclusao tgq
v8: atualiza fatores e apontamentos
v9: backtest Gus
v10: grafos
v11: janela 6m e 30d na liquidez
v12: ajuste janelas do performance attribution rodando com servidor e nuvem em paralelo
v13: ajuste no sharpe (v3)
v15: puxar bbg do bd
v16: ajustes nas porcentagens TNK,TDD
v17: ajuste perfee zerada
v18: retirar de botoes nao usados
cauda superior
"""

import os
os.chdir(r'I:\Riscos\Codigos Automacao')
import pandas as pd
import time
from sqlalchemy import create_engine
import datetime as dt
from dateutil import parser
import pickle
from dateutil.relativedelta import relativedelta
from wraper import *
import numpy as np
import math
from pandas.tseries.offsets import BDay
import win32com.client
import shutil
from tkinter import *
import sys


lastBusDay = dt.datetime.today()
yesterday = lastBusDay - BDay(1)
yesterday = dt.datetime(yesterday.year,yesterday.month,yesterday.day)
data_csv = yesterday.strftime("%d%b%Y")
yesterday_bbg = yesterday.strftime("%m/%d/%Y")
yesterday_2 = yesterday.strftime("%Y%m%d")


#------------------------------FUNCOES AUXILIARES------------------------------
def truncate(number, decimals=2):
    """
    Returns a value truncated to a specific number of decimal places.
    """
    if not isinstance(decimals, int):
        raise TypeError("decimal places must be an integer.")
    elif decimals < 0:
        raise ValueError("decimal places has to be 0 or more.")
    elif decimals == 0:
        return math.trunc(number)

    factor = 10.0 ** decimals
    return math.trunc(number * factor) / factor
    return math.trunc(stepper * number) / stepper

def to_alchemy_replace(df,tablename):
    param_dic = {
    "host"      : "PP-APPSRV13",
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
                  if_exists='replace') 
    return 0

def to_alchemy_append(df,tablename):
    param_dic = {
    "host"      : "PP-APPSRV13",
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

def to_alchemy_replace_n(df,tablename):
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
                  if_exists='replace') 
    return 0

def to_alchemy_append_n(df,tablename):
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

def recupera_bd2(tablename):
    param_dic = {
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
    df = pd.read_sql(tablename, con=engine)
    return df 

def query_bd(query):
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
    connection = engine.connect()
    connection.execute(query)
    return 0

def import_bbg():
    hoje = dt.datetime.today().strftime("%m/%d/%Y")
    start = dt.datetime.today() - relativedelta(months=126)
    start = start.strftime("%m/%d/%Y")
    
    bzaccetp = BBG.fetch_series('BZACCETP Index','PX_Last','01/01/2012', hoje)
    bzaccetp.to_csv('I:/Riscos/Maxdrawdown/bzaccetp.csv')
    
    sptr = BBG.fetch_series('SPTR500N Index','PX_Last',start,hoje, fx = 'USD')
    sptr.to_csv('I:/Riscos/Maxdrawdown/sptr.csv')
    
    ibov = BBG.fetch_series('IBOV Index','PX_Last', start,hoje, fx = 'BRL')
    ibov.to_csv('I:/Riscos/Maxdrawdown/ibov.csv')
    
    brl = BBG.fetch_series('BRL Curncy','PX_Last',start,hoje, fx = 'BRL')
    brl.to_csv('I:/Riscos/Maxdrawdown/brl.csv')
    
    spx = BBG.fetch_series('SPX Index','PX_Last',start,hoje, fx= 'USD')
    spx.to_csv('I:/Riscos/Maxdrawdown/spx.csv')
    
    ten_yr_treasury = BBG.fetch_series('USGG10YR Index','PX_Last',start,hoje, fx= 'USD')
    ten_yr_treasury .to_csv('I:/Riscos/Maxdrawdown/10yr_treasury.csv')
 
    pre_3_anos = BBG.fetch_series('BZAD3Y Index','PX_Last',start,hoje, fx= 'BRL')
    pre_3_anos.to_csv('I:/Riscos/Maxdrawdown/pre_3_anos.csv')
    
    dolar_real= BBG.fetch_series('USDBRL REGN Curncy','PX_Last',start,hoje, fx= 'BRL')
    dolar_real.to_csv('I:/Riscos/Maxdrawdown/dolar_real.csv')
    
    dxy= BBG.fetch_series('DXY Curncy','PX_Last',start,hoje, fx= 'USD')
    dxy.to_csv('I:/Riscos/Maxdrawdown/dxy.csv')
    
    print('Dados da BBG importados.')
    return 0

def atualiza_depara():
    data_csv = yesterday.strftime("%d%b%Y")
    #----------------Liquidez-----------------------
    base_liqui = pd.read_csv('I:\Riscos\Liquidez\Base\OverviewReport_' + data_csv +'.txt',sep='\t',header=(0))
    base_liqui1 = base_liqui[['TradingDesk','ClassID','Product','ExpiryDate','Amount','Position']]
    
    depara = pd.read_csv('I:\Riscos\Liquidez\DePara.csv',header=(0))
    
    base_liqui2 = base_liqui1.merge(depara,how='left',left_on=['Product'],right_on=['Product'])
    
    
    base_liqui2['Ativo_100p'] = base_liqui2['ClassID'].apply(lambda x: 1 if x == 153 or x ==  138 or \
                                                                  x == 70 \
                                                                  or x == 22 or x == 162 \
                                                                  else 0)
    base_liqui2 = base_liqui2[base_liqui2['Ativo_100p'] ==0]
    
    base_aux1 = base_liqui2.drop_duplicates()
    
    de_para_liqu = base_aux1[['Product','Bloomberg']]
    de_para_liqu = de_para_liqu[de_para_liqu['Bloomberg'].isnull()]
    
    base_aux = pd.read_csv('I:\Riscos\Liquidez\Base\FundOverviewByPrimitive_' + data_csv +'.txt',sep='\t',header=(0))
    base_aux = base_aux [['Product', 'ProductClass']]
    de_para_liqu = de_para_liqu.merge(base_aux) 
    de_para_liqu = de_para_liqu.drop_duplicates()
    de_para_liqu = de_para_liqu[['Product', 'ProductClass', 'Bloomberg']]
    
    
    #----------------Shorts-----------------------
    base_lote = pd.read_csv('I:\Riscos\Shorts\Base\FundOverviewByPrimitive_' + data_csv +'.txt',sep='\t',header=(0))
      
    
    tabela_equity =  base_lote[base_lote['ProductClass'].str.contains('Equity')]
    tabela_etf =  base_lote[base_lote['ProductClass']=='ETF']
    tabela_adr =  base_lote[base_lote['ProductClass']=='ADR']
    tabela_swap = base_lote[base_lote['ProductClass']=='Swaption USD']
    tabela = tabela_equity.append(tabela_etf) 
    tabela = tabela.append(tabela_adr)
    tabela = tabela.append(tabela_swap)
    tabela_final = tabela.merge(depara,how='left',left_on=['Product'],right_on=['Product'])
    
    de_para_shorts = tabela_final[['Product','ProductClass','Bloomberg']]
    de_para_shorts = de_para_shorts[de_para_shorts['Bloomberg'].isnull()].drop_duplicates()
    
    #----------------Tunel-----------------------
    fundos = ['rgq','rgm','ms1','ms2','tge','mif','glf']
    pasta_credito = 'I:/Riscos/Ludmilla/Distribuicao_Ordens/Historico/'
    tabela = pd.DataFrame()
    data_csv = yesterday.strftime("%Y%m%d")
    for i in range(len(fundos)):
        temp_df = pd.read_csv(pasta_credito + 'distrib_ordens_' + fundos[i]+ data_csv +'.txt', sep='\t')
        temp_df['tradingdesk'] = fundos[i]
        tabela = tabela.append(temp_df)
        
    filtro = ~tabela['ProductClass'].isin(['Cash', 'Overnight','Provisions and Costs','Currencies NDF'])
    tabela = tabela.loc[filtro]
    tabela = tabela.merge(depara,how='left',left_on=['Product'],right_on=['Product']) 
    de_para_tunel = tabela[['Product','ProductClass','Bloomberg']]
    de_para_tunel = de_para_tunel[de_para_tunel['Bloomberg'].isnull()].drop_duplicates()
    #----------------Todos-----------------------
    atualiza_de_para = de_para_liqu.append(de_para_shorts)
    atualiza_de_para = atualiza_de_para.append(de_para_tunel).drop_duplicates()
    atualiza_de_para = atualiza_de_para[atualiza_de_para ['ProductClass'] != 'Currencies Forward']
    atualiza_de_para = atualiza_de_para[atualiza_de_para ['ProductClass'] != 'ADR']
    atualiza_de_para = atualiza_de_para[atualiza_de_para ['ProductClass'] != 'ETF Options']
    atualiza_de_para = atualiza_de_para[atualiza_de_para ['ProductClass'] != 'USDBRLFuture']
    atualiza_de_para = atualiza_de_para[atualiza_de_para ['ProductClass'] != 'Debenture com participação nos lucros']
    atualiza_de_para = atualiza_de_para[atualiza_de_para ['ProductClass'] != 'Funds BR']
    atualiza_de_para = atualiza_de_para[atualiza_de_para ['ProductClass'] != 'Currencies NDF']
    atualiza_de_para = atualiza_de_para[atualiza_de_para ['ProductClass'] != 'LFT']
    #atualiza_de_para = pd.DataFrame([[1,2,3]])
    if len(atualiza_de_para)>0:
        atualiza_de_para.to_csv('I:/Riscos/Liquidez/atualiza_de_para.csv')
        print('Atualizar DePara!!!')
    else:
        print('DePara OK.')
    return 0

def atualiza_cambio():
    cambio = pd.read_csv(r'I:\Riscos\Ludmilla\cambio.csv')
    to_alchemy_replace_n(cambio,'cambio')
    print('Cambio atualizado no BD.')
    return 0

def atualiza_fatores():
    fatores = pd.read_excel('I:/Riscos/Fatores Equities/historico.xlsx',parse_dates =['val_date'])
    fatores = fatores[fatores['val_date']==yesterday]
    fatores['val_date'] = fatores['val_date'].dt.date 
    to_alchemy_append(fatores,'fatores_equities')
    to_alchemy_append_n(fatores,'fatores_equities')
    print('Fatores Atualizados!')
    return 0

def atualiza_obs():
    data_csv = yesterday.strftime("%Y%m%d")
    #----------------------------Autualiza Preço médio-----------------------------
    
    distrib_ordens = pd.read_excel('I:/Riscos/Distribuição de Ordens/Historico/PrecoMedio_' + data_csv + '.xlsx' )
    distrib_ordens = distrib_ordens[distrib_ordens['status']=='Verificar']
    if len(distrib_ordens)>0:
        distrib_ordens  = distrib_ordens.drop(distrib_ordens.columns[[0]],axis=1)
        distrib_ordens['val_date'] = distrib_ordens['val_date'].dt.date 
        to_alchemy_append(distrib_ordens,'distrib_ordens')
        to_alchemy_append_n(distrib_ordens,'distrib_ordens')
    else:
        print('Preço médio ok')
    ''
    #----------------------------Autualiza Tunel----------------------------------
    tunel = pd.read_excel('I:/Riscos/Tunel de Preço/Historico/tunel_preco_' + data_csv + '.xlsx')
    tunel = tunel[tunel['confere_preco']=='Verificar']
    if len(tunel)>0:
        tunel  = tunel.drop(columns={'confere_volume'})
        tunel  = tunel.drop(tunel.columns[[0]],axis=1)
        tunel['val_date'] = tunel['val_date'].dt.date 
        to_alchemy_append(tunel,'tunel_precos')
        to_alchemy_append_n(tunel,'tunel_precos')
    else:
        print('Tunel ok')
    print('Observacoes atualizadas!')
    return 0
#################################### Dolar ###############################
dolar = recupera_bd('cambio')
dolar = dolar.iloc[0][0]
############################ Risco de Mercado ############################



def var():

    yesterday_2 = yesterday.strftime("%Y-%m-%d")
    yesterday_arq = yesterday.strftime("%Y%m%d")
    
    
    fundos = ['rgq','rgm','ms1','tge','ms2','glf','tgq']
    #-------------------------------Calculo PL------------------------------------
    hbp = recupera_bd2('select * from consolidado_pnl')
    de_para_fundos = pd.read_excel('I:/Riscos/Ludmilla/de_para_fundos.xlsx')
    #de_para_fundos = de_para_fundos.rename(columns={'TradingDesk': 'trading_desk'})
    hbp = hbp.merge(de_para_fundos,left_on = 'trading_desk', right_on = 'TradingDesk')
    hbp['val_date'] = pd.to_datetime(hbp['val_date'])
    hbp =hbp.drop(columns = 'trading_desk_x') 
    hbp = hbp.rename(columns = {'trading_desk_y':'trading_desk' })  
    
    
    pl_fundos = pd.DataFrame()
    for i in fundos:
        if i == 'glf': 
            pl = hbp[hbp['trading_desk'] == i] 
            pl = pl[pl['book']!=('Default')]
            pl = pl[pl['book']!=('O3')]
            pl = pl[['val_date','position_close']].groupby('val_date').sum().reset_index()
            pl = pl[pl['val_date']==yesterday]
            pl['trading_desk']=i
            pl_fundos = pl_fundos.append(pl)
        else:
            pl = hbp[hbp['trading_desk'] == i] 
            pl = pl[['val_date','position_close']].groupby('val_date').sum().reset_index()
            pl = pl[pl['val_date']==yesterday]
            pl['trading_desk']=i
            pl_fundos = pl_fundos.append(pl)
    #ajuste no PL do Global 
    #dolar = 5.40685464499801
    sta_rita = 0
    hedge = hbp[hbp['trading_desk'] == 'hdf'] 
    hedge = hedge[['val_date','position_close']].groupby('val_date').sum().reset_index()
    hedge = hedge.iloc[-1]['position_close']
    pl_fundos ['position_close'] = np.where(pl_fundos['trading_desk'] == 'glf',pl_fundos ['position_close']+((sta_rita+hedge)/dolar),pl_fundos ['position_close'])
    
    #------------------------------Iportação de Retornos---------------------------
    '''
    Calculo da vol realizada 126du, incluir outros fundos a medida que completarem
    os 126 du
    '''
    fundos_vol_126 = ['rgq','ms1','ms2','rgm','tge','glf']
    retornos = recupera_bd('retornos')
    
    def vol126 (df,fundo):
        df = df[df['trading_desk'] == fundo]
        df = df.sort_values('val_date')
        a = min(126,len(df))
        vol = np.std(df['returns'].tail(a))*math.sqrt(252)
        final = pd.DataFrame([[fundo,vol]],columns = ['trading_desk','vol_realizada_126du'])
        return (final)
    
    vol_126du = pd.DataFrame()
    for i in fundos_vol_126:
        vol = vol126(retornos,i)
        vol_126du = vol_126du.append(vol) 
    #-------------------------------Importacao de dados----------------------------    
    
    def le_dados(fundo,nivel):
        if 'param' in fundo:
            tabela = pd.read_csv('I:/Riscos/TotalVaR/historico/' + fundo + '_nivel' + nivel +'_' + yesterday_arq + '.txt', sep='\t',parse_dates=['ValDate'])
            tabela = tabela.rename(columns = {'ValDate' : 'val_date', 'ParametricVaR' : 'param_var'})
            tabela['val_date'] = tabela['val_date'].dt.date 
        elif 'stress' in fundo:
            tabela = pd.read_csv('I:/Riscos/TotalVaR/historico/' + fundo + '_nivel' + nivel +'_' + yesterday_arq + '.txt', sep='\t')
        else:
            tabela = pd.read_csv('I:/Riscos/TotalVaR/historico/' + fundo + '_nivel' + nivel +'_' + yesterday_arq + '.txt', sep='\t',parse_dates=['Date'])
            tabela = tabela.rename(columns = {'Date' : 'val_date', 'TotalVar' : 'total_var'})
            tabela['val_date'] = tabela['val_date'].dt.date 
        return (tabela)  
    
    
    quali60_6 = le_dados('quali_param_60','6')  
    quali24_6 = le_dados('quali_param_24','6')  
    quali120 = le_dados('quali_120','6')  
    quali2 = le_dados('quali','2')  
    quali60_3 = le_dados('quali_param_60','3')  
    quali24_3 = le_dados('quali_param_24','3')  
    quali120_3 = le_dados('quali_120','3') 
    
    geral60_6 = le_dados('geral_param_60','6')  
    geral24_6 = le_dados('geral_param_24','6')  
    geral120 = le_dados('geral_120','6') 
    geral2 = le_dados('geral','2')
    geral60_3 = le_dados('geral_param_60','3')  
    geral24_3 = le_dados('geral_param_24','3')  
    geral120_3 = le_dados('geral_120','3') 
    
    global60_6 = le_dados('global_param_60','6')  
    global24_6 = le_dados('global_param_24','6')  
    global120 = le_dados('global_120','6') 
    global2 = le_dados('global','2')
    
    multi60_6 = le_dados('multi_param_60','7')  
    multi24_6 = le_dados('multi_param_24','7')  
    multi120 = le_dados('multi_120','7') 
    multi2 = le_dados('multi','2')
    
    tendencias60_6 = le_dados('tendencias_param_60','7')  
    tendencias24_6 = le_dados('tendencias_param_24','7')  
    tendencias120 = le_dados('tendencias_120','7') 
    tendencias2 = le_dados('tendencias','2')
    
    tendencias_q60_6 = le_dados('tendencias_q_param_60','7')  
    tendencias_q24_6 = le_dados('tendencias_q_param_24','7')  
    tendencias_q120 = le_dados('tendencias_q_120','7') 
    tendencias_q2 = le_dados('tendencias_q','2')
    
    multi260_6 = le_dados('multi2_param_60','7')  
    multi224_6 = le_dados('multi2_param_24','7')  
    multi2120 = le_dados('multi2_120','7') 
    multi22 = le_dados('multi2','2')
    
    #stress
    stress_quali = le_dados('stress_quali','1')
    stress_geral = le_dados('stress_geral','1')
    stress_global = le_dados('stress_global','2')
    stress_global  = stress_global[stress_global['Book']=='O3 CAPITAL']
    stress_multi= le_dados('stress_multi','1')
    stress_tendencias = le_dados('stress_tendencias','1')
    stress_tendencias_q = le_dados('stress_tendencias_q','1')
    stress_multi2 = le_dados('stress_multi2','1')
     
    quali_24 = le_dados('quali_24','6')  
    geral_24 = le_dados('geral_24','6')  
    global_24 = le_dados('global_24','6')  
    multi_24 = le_dados('multi_24','7') 
    tendencias_24 = le_dados('tendencias_24','7') 
    tendencias_q_24 = le_dados('tendencias_q_24','7') 
    multi2_24 = le_dados('multi2_24','7')
    #-------------------------------Funcoes Rotina---------------------------------
    def resultados_fundo(df,df_stress,data,fundo):    
        df = df.sort_values('val_date')
        PL = pl_fundos[pl_fundos['trading_desk']==fundo].iloc[0]['position_close']
        vol_24 = abs(np.std(df['total_var'].tail(504))*math.sqrt(252))/PL
        vol_60 = abs(np.std(df['total_var'].tail(1260))*math.sqrt(252))/PL
        
        percentil = np.percentile(df['total_var'],1)
        var_1pct = abs(percentil/PL)
        filtro_es = df[df['total_var']<percentil]
        es_pct = abs(np.mean(filtro_es['total_var'])*math.sqrt(21)/PL)
        
        percentil_sup = np.percentile(df['total_var'],99)
        var_1pct_sup = abs(percentil_sup/PL)
        filtro_sup = df[df['total_var']>percentil_sup]
        es_sup = abs(np.mean(filtro_sup['total_var'])*math.sqrt(21)/PL)
        
        ewma = list()
        ewma.append(0)
        tv = list(df['total_var'])
        for i in range(1,len(df)):
            ewma_nova =math.sqrt( 0.94*ewma[i-1]*ewma[i-1]+(1-0.94)*tv[i]*tv[i])
            ewma.append(ewma_nova)
        ewma_ano = pd.DataFrame(ewma)*math.sqrt(252)
        vol_ewma = ewma_ano.tail(1)
        vol_ewma = abs(vol_ewma.iloc[0][0]/PL)
        stress = abs(df_stress.iloc[0]['TotalStress'])/PL
        resultados = pd.DataFrame([[data,fundo,vol_24,vol_60,vol_ewma,var_1pct,es_pct,stress,es_sup]],columns=['val_date','trading_desk','vol_24_meses','vol_60_meses','vol_ewma','var_1du','es_21du','stress_test','cauda_sup_21du'])
        return(resultados)  
    
    def resultados_book(df,data,fundo,book):
        pct = {
            'tdd': 0.1,
            'tdm': 0.60,
            'tgc_local': 0.06,
            'tgc_off': 0.20,
            'trm': 0.05,
            'tml': 0.1,
            'tnk':0.1
            }    
        df = df.sort_values('val_date')
        PL = pl_fundos[pl_fundos['trading_desk']==fundo].iloc[0]['position_close']
        vol_24 = abs(np.std(df['total_var'].tail(504))*math.sqrt(252))/(PL*pct[book])
        vol_60 = abs(np.std(df['total_var'].tail(1260))*math.sqrt(252))/(PL*pct[book])
        percentil = np.percentile(df['total_var'],1)
        var_1pct = abs(percentil/(PL*pct[book]))
        filtro_es = df[df['total_var']<percentil]
        es_pct = abs(np.mean(filtro_es['total_var'])*math.sqrt(21)/(PL*pct[book]))
        ewma = list()
        ewma.append(0)
        tv = list(df['total_var'])
        for i in range(1,len(df)):
            ewma_nova =math.sqrt( 0.94*ewma[i-1]*ewma[i-1]+(1-0.94)*tv[i]*tv[i])
            ewma.append(ewma_nova)
        ewma_ano = pd.DataFrame(ewma)*math.sqrt(252)
        vol_ewma = ewma_ano.tail(1)
        vol_ewma = abs(vol_ewma.iloc[0][0]/(PL*pct[book]))
        resultados = pd.DataFrame([[data,fundo,book,'book',vol_24,vol_60,vol_ewma,es_pct,var_1pct]],columns=['val_date','trading_desk','estrategia','tipo','vol_24_meses','vol_60_meses','vol_ewma','es_21du','var_1du'])
        return(resultados)  
    def resultados_book2(df,data,fundo,book):   
        df = df.sort_values('val_date')
        vol_24 = abs(np.std(df['total_var'].tail(504))*math.sqrt(252))
        vol_60 = abs(np.std(df['total_var'].tail(1260))*math.sqrt(252))
        percentil = np.percentile(df['total_var'],1)
        var_1pct = abs(percentil)
        filtro_es = df[df['total_var']<percentil]
        es_pct = abs(np.mean(filtro_es['total_var'])*math.sqrt(21))
        ewma = list()
        ewma.append(0)
        tv = list(df['total_var'])
        for i in range(1,len(df)):
            ewma_nova =math.sqrt( 0.94*ewma[i-1]*ewma[i-1]+(1-0.94)*tv[i]*tv[i])
            ewma.append(ewma_nova)
        ewma_ano = pd.DataFrame(ewma)*math.sqrt(252)
        vol_ewma = ewma_ano.tail(1)
        vol_ewma = abs(vol_ewma.iloc[0][0])
        resultados = pd.DataFrame([[data,fundo,book,'te_equities',vol_24,vol_60,vol_ewma,es_pct,var_1pct]],columns=['val_date','trading_desk','estrategia','tipo','vol_24_meses','vol_60_meses','vol_ewma','es_21du','var_1du'])
        return(resultados)  
    
    def resultados_estrategia(df24,df60,df6,df120,data,fundo,estrategia):    
        df6 = df6.sort_values('val_date').tail(126)
        PL = pl_fundos[pl_fundos['trading_desk']==fundo].iloc[0]['position_close']
        z = 1.64485362695147
        vol_24 = abs(df24.iloc[0]['param_var']/z*math.sqrt(252)/PL)
        vol_60 = abs(df60.iloc[0]['param_var']/z*math.sqrt(252)/PL)
        es_pct = abs(np.mean(df120['total_var'])*math.sqrt(21)/PL)
        var_1pct = abs(np.max(df120['total_var'])/PL)
        ewma = list()
        ewma.append(0)
        tv = list(df6['total_var'])
        for i in range(1,len(df6)):
            ewma_nova =math.sqrt( 0.94*ewma[i-1]*ewma[i-1]+(1-0.94)*tv[i]*tv[i])
            ewma.append(ewma_nova)
        ewma_ano = pd.DataFrame(ewma)*math.sqrt(252)
        vol_ewma = ewma_ano.tail(1)
        vol_ewma = abs(vol_ewma.iloc[0][0]/PL)
        resultados = pd.DataFrame([[data,fundo,'risco',estrategia,vol_24,vol_60,vol_ewma,es_pct,var_1pct]],columns=['val_date','trading_desk','tipo','estrategia','vol_24_meses','vol_60_meses','vol_ewma','es_21du','var_1du'])
        return(resultados) 
    #------------------------------Resultado por fundo----------------------------
    
    quali_fundo = quali2.drop(columns=['Book']).groupby('val_date').sum()
    res_quali = resultados_fundo(quali_fundo,stress_quali, yesterday, 'rgq')
    
    geral_fundo = geral2.drop(columns=['Book']).groupby('val_date').sum()
    res_geral = resultados_fundo(geral_fundo,stress_geral, yesterday, 'rgm')
    
    
    global_fundo = global2[global2['Book'] != 'O3']
    global_fundo = global_fundo.drop(columns=['Book']).groupby('val_date').sum()
    res_global = resultados_fundo(global_fundo, stress_global, yesterday, 'glf')
    
    multi_fundo = multi2.drop(columns=['Book']).groupby('val_date').sum()
    res_multi = resultados_fundo(multi_fundo,stress_multi, yesterday, 'ms1')
    
    tendencias_fundo = tendencias2.drop(columns=['Book']).groupby('val_date').sum()
    res_tendencias = resultados_fundo(tendencias_fundo,stress_tendencias, yesterday, 'tge')
    
    tendencias_q_fundo = tendencias_q2.drop(columns=['Book']).groupby('val_date').sum()
    res_tendencias_q = resultados_fundo(tendencias_q_fundo,stress_tendencias_q, yesterday, 'tgq')
    
    multi2_fundo = multi22.drop(columns=['Book']).groupby('val_date').sum()
    res_multi2 = resultados_fundo(multi2_fundo,stress_multi2, yesterday, 'ms2')
    
    res_fundo = res_quali.append(res_geral).append(res_global).append(res_multi).append(res_tendencias).append(res_tendencias_q).append(res_multi2)
    #incluindo a vol realizada
    ###################################################
    res_fundo = res_fundo.merge(vol_126du,how='outer') 
    ###################################################
    #----------------------------Estrategis Poli---------------------------------
    def filtro_estrat(df24,df60,df6,df120,estrat,estrategia,fundo):    
        novo24 = df24[df24['Book'].str.contains(estrat)]
        novo24 = novo24.drop(columns=['Book']).groupby('val_date').sum()
        novo60 = df60[df60['Book'].str.contains(estrat)]
        novo60 = novo60.drop(columns=['Book']).groupby('val_date').sum()
        novo6 = df6[df6['Book'].str.contains(estrat)]
        novo6 = novo6.drop(columns=['Book']).groupby('val_date').sum()
        novo120 = df120[df120['Book'].str.contains(estrat)]
        novo120 = novo120.drop(columns=['Book']).groupby('val_date').sum()
        if len(novo6.total_var) == 0:
            final = pd.DataFrame()
            return (final)
        final = resultados_estrategia(novo24,novo60,novo6,novo120 ,yesterday,fundo,estrategia)
        return(final)
    
    de_para_estrat = pd.read_excel('I:/Riscos/TotalVaR/estrategias.xlsx', header=None)
    estrat_lote = list(de_para_estrat[0])
    estrat_bd = list(de_para_estrat[1])
    estrat_cont_bd = estrat_bd 
    estrat_cont_lote = estrat_lote 
    filtro_global = ~(de_para_estrat[0].str.contains('ON'))
    estrat_global = de_para_estrat[filtro_global]
    filtro_global = ~(estrat_global[0].str.contains('TRM'))
    estrat_global = estrat_global[filtro_global]
    estrat_global_lote = list(estrat_global[0])
    estrat_global_bd = list(estrat_global[1])
    estrat_global_cont_bd = estrat_global_bd
    estrat_global_cont_lote = estrat_global_lote
    quali_res_estrat = pd.DataFrame()
    
    for i in range (len(estrat_lote)):
        a = filtro_estrat(quali24_6,quali60_6,quali_24,quali120,estrat_lote[i],estrat_bd[i],'rgq')
        if len(a) == 0 :
            pass
        else:
            quali_res_estrat = quali_res_estrat.append(a)
    
    geral_res_estrat = pd.DataFrame()
    for i in range (len(estrat_lote)):
        a = filtro_estrat(geral24_6,geral60_6,geral_24,geral120,estrat_lote[i],estrat_bd[i],'rgm')
        if len(a) == 0 :
            pass
        else:
            geral_res_estrat = geral_res_estrat.append(a)
    
    global_res_estrat = pd.DataFrame()
    for i in range (len(estrat_global_lote)):
        a = filtro_estrat(global24_6,global60_6,global_24,global120,estrat_global_lote[i],estrat_global_bd[i],'glf')
        if len(a) == 0 :
            pass
        else:
            global_res_estrat = global_res_estrat.append(a)
        
    #ON e OFF
    quali_6 = quali_24
    quali_6_on_off = quali_24[quali_6['Book'].isin(estrat_lote)]
    quali_6_on = quali_6_on_off[quali_6['Book'].str.contains('ON')]
    quali_6_on = quali_6_on.append(quali_6[quali_6['Book'].str.contains('CAIXA O3 LOCAL')])
    quali_6_on = quali_6_on.drop(columns=['Book']).groupby('val_date').sum().sort_values('val_date').tail(126)
    
    quali24_on = quali24_3[quali24_3['Book'] == 'ONSHORE'].reset_index()
    quali60_on = quali60_3[quali60_3['Book'] == 'ONSHORE']
    quali120_on = quali120_3[quali120_3['Book'] == 'ONSHORE']
    quali120_on = quali120_on.drop(columns=['Book']).groupby('val_date').sum()
    
    quali_6_off = quali_6_on_off[quali_6['Book'].str.contains('OF')]
    quali_6_off = quali_6_off.append(quali_6[quali_6['Book'].str.contains('CAIXA O3 OFF')])
    quali_6_off = quali_6_off.drop(columns=['Book']).groupby('val_date').sum().sort_values('val_date').tail(126)
    quali24_off = quali24_3[quali24_3['Book'] == 'OFFSHORE'].reset_index()
    quali60_off = quali60_3[quali60_3['Book'] == 'OFFSHORE']
    quali120_off = quali120_3[quali120_3['Book'] == 'OFFSHORE']
    quali120_0ff = quali120_off.drop(columns=['Book']).groupby('val_date').sum()
    
    quali_res_on = resultados_estrategia(quali24_on,quali60_on,quali_6_on,quali120_on,yesterday,'rgq','on')
    quali_res_off = resultados_estrategia(quali24_off,quali60_off,quali_6_off,quali120_off,yesterday,'rgq','off')
    
    geral_6 = geral_24
    geral_6_on_off = geral_6[geral_6['Book'].isin(estrat_lote)]
    geral_6_on = geral_6_on_off[geral_6['Book'].str.contains('ON')]
    geral_6_on = geral_6_on.append(geral_6[geral_6['Book'].str.contains('CAIXA O3 LOCAL')])
    geral_6_on = geral_6_on.drop(columns=['Book']).groupby('val_date').sum().sort_values('val_date').tail(126)
    
    geral24_on = geral24_3[geral24_3['Book'] == 'ONSHORE'].reset_index()
    geral60_on = geral60_3[geral60_3['Book'] == 'ONSHORE']
    geral120_on = geral120_3[geral120_3['Book'] == 'ONSHORE']
    geral120_on = geral120_on.drop(columns=['Book']).groupby('val_date').sum()
    
    geral_6_off = geral_6_on_off[geral_6['Book'].str.contains('OF')]
    geral_6_off = geral_6_off.append(geral_6[geral_6['Book'].str.contains('CAIXA O3 OFF')])
    geral_6_off = geral_6_off.drop(columns=['Book']).groupby('val_date').sum().sort_values('val_date').tail(126)
    geral24_off = geral24_3[geral24_3['Book'] == 'OFFSHORE'].reset_index()
    geral60_off = geral60_3[geral60_3['Book'] == 'OFFSHORE']
    geral120_off = geral120_3[geral120_3['Book'] == 'OFFSHORE']
    geral120_0ff = geral120_off.drop(columns=['Book']).groupby('val_date').sum()
    
    geral_res_on = resultados_estrategia(geral24_on,geral60_on,geral_6_on,geral120_on,yesterday,'rgm','on')
    geral_res_off = resultados_estrategia(geral24_off,geral60_off,geral_6_off,geral120_off,yesterday,'rgm','off')
    
    res_estrat = quali_res_estrat.append(quali_res_on).append(quali_res_off).append(geral_res_estrat).append(geral_res_on).append(geral_res_off).append(global_res_estrat)
    #----------------------------Estrategias Maeji---------------------------------
    de_para_estrat = pd.read_excel('I:/Riscos/TotalVaR/estrategias_maeji.xlsx', header=None)
    estrat_lote = list(de_para_estrat[0])
    estrat_bd = list(de_para_estrat[1])
    multi_res_estrat = pd.DataFrame()
    for i in range (len(estrat_lote)):
        a = filtro_estrat(multi24_6,multi60_6,multi_24,multi120,estrat_lote[i],estrat_bd[i],'ms1')
        multi_res_estrat = multi_res_estrat.append(a)
    
    tendencias_res_estrat = pd.DataFrame()
    for i in range (len(estrat_lote)):
        a = filtro_estrat(tendencias24_6,tendencias60_6,tendencias_24,tendencias120,estrat_lote[i],estrat_bd[i],'tge')
        tendencias_res_estrat = tendencias_res_estrat.append(a)
    
    tendencias_q_res_estrat = pd.DataFrame()
    for i in range (len(estrat_lote)):
        a = filtro_estrat(tendencias_q24_6,tendencias_q60_6,tendencias_q_24,tendencias_q120,estrat_lote[i],estrat_bd[i],'tgq')
        tendencias_q_res_estrat = tendencias_q_res_estrat.append(a)
        
    multi2_res_estrat = pd.DataFrame()
    for i in range (len(estrat_lote)):
        a = filtro_estrat(multi224_6,multi260_6,multi2_24,multi2120,estrat_lote[i],estrat_bd[i],'ms2')
        multi2_res_estrat = multi2_res_estrat.append(a)
    
    res_estrat_maeji = multi_res_estrat.append(tendencias_res_estrat).append(tendencias_q_res_estrat).append(multi2_res_estrat)
    ########################################################
    res_estrat_final = res_estrat.append(res_estrat_maeji)
    ########################################################
    #--------------------------------Books-----------------------------------------
    novo = geral2[geral2['Book'].str.contains('TDM')]
    novo = novo.drop(columns=['Book']).groupby('val_date').sum()
    final = resultados_book(novo,yesterday,'rgm','tdm')
        
    
    book_lote = ['TDD','TDM','TGC LOCAL','TGC OFF', 'TRM','TNK']
    book_bd = ['tdd','tdm','tgc_local','tgc_off', 'trm','tnk']
    
    def filtro_book(df,book,book2,fundo):
        novo = df[df['Book'].str.contains(book)]
        novo = novo.drop(columns=['Book']).groupby('val_date').sum()
        if len(novo) == 0:
            final = pd.DataFrame()
        else:    
            final = resultados_book(novo,yesterday,fundo,book2)
        return(final)
    
    
    quali_res_book= pd.DataFrame()
    for i in range (len(book_lote)):
        a = filtro_book(quali2,book_lote[i],book_bd[i],'rgq')
        if len(a) == 0:
            pass
        else:
            quali_res_book = quali_res_book.append(a)
    
    geral_res_book= pd.DataFrame()
    for i in range (len(book_lote)):
        a = filtro_book(geral2,book_lote[i],book_bd[i],'rgm')
        if len(a) == 0:
            pass
        else:
            geral_res_book = geral_res_book.append(a)
        
    book_global_lote = ['TDM','TGC LOCAL','TGC OFF', 'TRM','TNK']
    book_global_bd = ['tdm','tgc_local','tgc_off', 'trm','tnk']    
    
    global_res_book= pd.DataFrame()
    for i in range (len(book_global_lote)):
        a = filtro_book(global2,book_global_lote[i],book_global_bd[i],'glf')
        if len(a) == 0:
            pass
        else:
            global_res_book = global_res_book.append(a)
        
    #Books TGC e TML
    ibov = recupera_bd('ibov')
    ibov['ibov'] = ibov['IBOV Index']/ibov['IBOV Index'].shift(1)-1 
    spx = recupera_bd('spx')
    spx['spx'] = spx['SPX Index']/spx['SPX Index'].shift(1)-1 
    
    pct_tgc_off = 0.10
    pct_tgc_on = 0.03
    pl_quali = pl_fundos[pl_fundos['trading_desk'] == 'rgq'].iloc[0]['position_close']
    pl_geral = pl_fundos[pl_fundos['trading_desk'] == 'rgm'].iloc[0]['position_close']
    
    # leitura de dados
    tgc_on_quali = pd.read_csv('I:/Riscos/TotalVaR/historico/usd_brl/tgc_on_quali_' + yesterday_arq + '.txt', sep='\t',parse_dates=['Date'])
    tgc_on_quali = tgc_on_quali.rename(columns={'Date':'val_date'})
    tgc_on_geral = pd.read_csv('I:/Riscos/TotalVaR/historico/usd_brl/tgc_on_geral_' + yesterday_arq + '.txt', sep='\t',parse_dates=['Date'])
    tgc_on_geral = tgc_on_geral.rename(columns={'Date':'val_date'})
    
    tgc_off_quali = pd.read_csv('I:/Riscos/TotalVaR/historico/usd_brl/tgc_off_quali_' + yesterday_arq + '.txt', sep='\t',parse_dates=['Date'])
    tgc_off_quali = tgc_off_quali.rename(columns={'Date':'val_date'})
    tgc_off_geral = pd.read_csv('I:/Riscos/TotalVaR/historico/usd_brl/tgc_off_geral_' + yesterday_arq + '.txt', sep='\t',parse_dates=['Date'])
    tgc_off_geral = tgc_off_geral.rename(columns={'Date':'val_date'})
    # ajustes off
    tgc_off_quali['aux'] = (tgc_off_quali['TotalVar']-tgc_off_quali['Total USDBRLSpot'])/(pl_quali*pct_tgc_off)   
    tgc_off_quali = tgc_off_quali.merge(spx,how = 'left')
    tgc_off_quali['dif_off_spx'] = tgc_off_quali['spx'] - tgc_off_quali['aux']
    tgc_off_quali = tgc_off_quali[['val_date','dif_off_spx']].fillna(0)
    tgc_off_quali = tgc_off_quali.rename(columns = {'dif_off_spx':'total_var'})
    
    tgc_off_geral['aux'] = (tgc_off_geral['TotalVar']-tgc_off_geral['Total USDBRLSpot'])/(pl_geral*pct_tgc_off)   
    tgc_off_geral = tgc_off_geral.merge(spx,how = 'left')
    tgc_off_geral['dif_off_spx'] = tgc_off_geral['spx'] - tgc_off_geral['aux']
    tgc_off_geral = tgc_off_geral[['val_date','dif_off_spx']].fillna(0)
    tgc_off_geral = tgc_off_geral.rename(columns = {'dif_off_spx':'total_var'})
    
    #ajustes on
    tgc_on_quali['aux'] = (tgc_on_quali['TotalVar']-tgc_on_quali['Total USDBRLSpot'])/(pl_quali*pct_tgc_on)   
    tgc_on_quali = tgc_on_quali.merge(ibov,how = 'left')
    tgc_on_quali['dif_on_ibov'] = tgc_on_quali['ibov'] - tgc_on_quali['aux']
    tgc_on_quali = tgc_on_quali[['val_date','dif_on_ibov']].fillna(0)
    tgc_on_quali = tgc_on_quali.rename(columns = {'dif_on_ibov':'total_var'})
    
    tgc_on_geral['aux'] = (tgc_on_geral['TotalVar']-tgc_on_geral['Total USDBRLSpot'])/(pl_geral*pct_tgc_on)   
    tgc_on_geral = tgc_on_geral.merge(ibov,how = 'left')
    tgc_on_geral['dif_on_ibov'] = tgc_on_geral['ibov'] - tgc_on_geral['aux']
    tgc_on_geral = tgc_on_geral[['val_date','dif_on_ibov']].fillna(0)
    tgc_on_geral = tgc_on_geral.rename(columns = {'dif_on_ibov':'total_var'})
    #aplicando funcoes
    res_tgc_on_geral = resultados_book2(tgc_on_geral,yesterday,'rgm','te_local_ibov')
    res_tgc_on_quali = resultados_book2(tgc_on_quali,yesterday,'rgq','te_local_ibov')
    
    res_tgc_off_geral = resultados_book2(tgc_off_geral,yesterday,'rgm','te_off_spx')
    res_tgc_off_quali = resultados_book2(tgc_off_quali,yesterday,'rgq','te_off_spx')
    #ajustes TML quali
    tml_quali = pd.read_csv('I:/Riscos/TotalVaR/historico/usd_brl/tml_quali_' + yesterday_arq + '.txt', sep='\t',parse_dates=['Date'])
    tml_usd  = pd.read_csv('I:/Riscos/TotalVaR/historico/usd_brl/usd_future' + '.csv',)
    raz_quali = tml_usd.iloc[0]['usd_future']/tml_usd.iloc[0]['total']
    tml_quali['total_var'] = tml_quali['TotalVar']-(tml_quali['Total USDBRLSpot']*(1-raz_quali))
    tml_quali['trading_desk'] = 'tml'
    tml_quali = tml_quali.rename(columns={'Date':'val_date'})
    #ajustes TML geral
    tml_geral = pd.read_csv('I:/Riscos/TotalVaR/historico/usd_brl/tml_geral_' + yesterday_arq + '.txt', sep='\t',parse_dates=['Date'])
    tml_usd  = pd.read_csv('I:/Riscos/TotalVaR/historico/usd_brl/usd_future' + '.csv',)
    raz_geral = tml_usd.iloc[1]['usd_future']/tml_usd.iloc[1]['total']
    tml_geral['total_var'] = tml_geral['TotalVar']-(tml_geral['Total USDBRLSpot']*(1-raz_geral))
    tml_geral['trading_desk'] = 'tml'
    tml_geral = tml_geral.rename(columns={'Date':'val_date'})
    
    res_tml_geral = resultados_book(tml_geral,yesterday,'rgm','tml')
    res_tml_quali = resultados_book(tml_quali,yesterday,'rgq','tml') 
    
    ###########################################################################
    res_book = quali_res_book.append(geral_res_book).append(global_res_book)
    res_book = res_book.append([res_tml_geral, res_tml_quali,res_tgc_on_geral,res_tgc_on_quali,res_tgc_off_geral,res_tgc_off_quali])
    res_book = res_book.sort_values('trading_desk')
    ###########################################################################
    #Ajustes
    res_fundo = res_fundo[['val_date','trading_desk','vol_24_meses','vol_60_meses','vol_ewma','var_1du','es_21du','vol_realizada_126du','stress_test','cauda_sup_21du']]
    res_fundo['val_date'] = res_fundo['val_date'].dt.date
    res_book['val_date'] = res_book['val_date'].dt.date
    res_estrat_final['val_date']  = res_estrat_final['val_date'].dt.date
    
    
    estrat_maeji = res_book[res_book['estrategia']=='trm']
    estrat_maeji['tipo'] = np.where(estrat_maeji['tipo'] == 'book','risco','risco')
    estrat_maeji['estrategia'] = np.where(estrat_maeji['estrategia'] == 'trm','equities_rel_value','equities_rel_value')
    estrat_maeji = estrat_maeji[estrat_maeji['trading_desk']!= 'glf']
    ajuste = 0.05
    estrat_maeji2 = estrat_maeji[['val_date','estrategia','trading_desk','tipo']].copy()
    estrat_maeji2['vol_24_meses'] = estrat_maeji['vol_24_meses'] * ajuste
    estrat_maeji2['vol_60_meses'] = estrat_maeji['vol_60_meses'] * ajuste
    estrat_maeji2['vol_ewma'] = estrat_maeji['vol_ewma'] * ajuste
    estrat_maeji2['es_21du'] = estrat_maeji['es_21du'] * ajuste
    estrat_maeji2['var_1du'] = estrat_maeji['var_1du'] * ajuste
    
    res_estrat_final = res_estrat_final.append(estrat_maeji2)
    
    #-------------------------------contribuicao-----------------------------------
    
    def pct_estrat(res,fundo,estrat,on_off):
        pct = res[res['trading_desk'] == fundo]
        if on_off == False:
            pct = pct[pct['estrategia'] !='on']
            pct = pct[pct['estrategia'] !='off']
            nova_estrat = pct['estrategia']
        else:
            nova_estrat = ['on','off']
        pct = pct[pct['estrategia'].isin(nova_estrat)] 
        total=pct.sum()
        total_vol24 = total.loc['vol_24_meses']
        total_es = total.loc['es_21du']
        pct['pct_vol_24'] = pct['vol_24_meses']/total_vol24
        pct['pct_es_21du'] = pct['es_21du']/total_es
        return(pct)
    
    def matriz(df24,estrat,estrat_bd,ajuste):
        est = pd.Series()
        matriz = pd.DataFrame()
        for i in range(len(estrat)):
            df24_novo = df24[df24['Book'].str.contains(estrat[i])]
            df24_novo =df24_novo.rename(columns = {'total_var': estrat[i]})
            df24_novo = df24_novo.drop(columns={'Book'})
            if i == 0 or len(matriz)==0:
                if len(df24_novo) == 0:
                    pass
                else:
                    matriz = df24_novo
                    est = est.append(pd.Series(estrat[i]))
            else:
                if len(df24_novo) == 0:
                    pass
                else:
                    matriz = matriz.merge(df24_novo,on='val_date')
                    est = est.append(pd.Series(estrat[i]))
            zip_it = zip(est, range(len(est)))
            dic = dict(zip_it)
            matriz = matriz.rename(columns = dic)
        if ajuste == True:
            df24_novo = df24[df24['Book'].str.contains('OFF TRM')]
            df24_novo = df24_novo.append(df24[df24['Book'].str.contains('TRM CAIXA OFF')])
            df24_novo = df24_novo.drop(columns={'Book'})
            df24_novo = df24_novo.groupby('val_date').sum()
            df24_novo =df24_novo.rename(columns = {'total_var': len(est)})
            matriz = matriz.merge(df24_novo,on='val_date')
        else:
            pass
        return(matriz)
    
    def matriz_es(df24,estrat,estrat_bd,ajuste):
        est = pd.Series()
        matriz = pd.DataFrame()
        for i in range(len(estrat)):
            df24_novo = df24[df24['Book'].str.contains(estrat[i])]
            df24_novo = df24_novo.sort_values('total_var').reset_index()
            df24_novo =df24_novo.rename(columns = {'total_var': estrat[i]})
            df24_novo = df24_novo.drop(columns={'Book','val_date','index'})
            if i == 0 or len(matriz)==0:
                if len(df24_novo) == 0:
                    pass
                else:
                    matriz = df24_novo
                    est = est.append(pd.Series(estrat[i]))
            else:
                 if len(df24_novo) == 0:
                    pass
                 else:
                    matriz = pd.concat([matriz,df24_novo],axis=1)
                    est = est.append(pd.Series(estrat[i]))
            zip_it = zip(est, range(len(est)))
            dic = dict(zip_it)
            matriz = matriz.rename(columns = dic)
        return(matriz)
    
    def contrib_vol24(matriz, matriz_pct,estrat_bd,fundo):
        a = matriz_pct['estrategia'].tolist()
        nova_pct = pd.DataFrame(matriz_pct['pct_vol_24']).reset_index().drop(columns={'index'})
        prod = matriz.dot(nova_pct)
        prod = prod.rename(columns = {'pct_vol_24':'prod'})
        prod = pd.concat([pd.DataFrame(a),prod,nova_pct], axis = 1)
        prod['aux'] = prod['prod']*prod['pct_vol_24']
        tot = prod['aux'].sum()
        prod['contribuicao_vol_24'] = prod['aux']/tot 
        prod['trading_desk'] = fundo
        prod = prod.drop(columns=['prod','pct_vol_24','aux'])
        prod = prod.rename(columns={0:'estrategia'}) 
        return(prod)
    
    def contrib_es(matriz, matriz_pct,estrat_bd,fundo):
        a = matriz_pct['estrategia'].tolist()
        nova_pct = pd.DataFrame(matriz_pct['pct_es_21du']).reset_index().drop(columns={'index'})
        prod = matriz.dot(nova_pct)
        prod = prod.rename(columns = {'pct_es_21du':'prod'})
        prod = pd.concat([pd.DataFrame(a),prod,nova_pct], axis = 1)
        prod['aux'] = prod['prod']*prod['pct_es_21du']
        tot = prod['aux'].sum()
        prod['contribuicao_es_21du'] = prod['aux']/tot 
        prod['trading_desk'] = fundo
        prod = prod.drop(columns=['prod','pct_es_21du','aux'])
        prod = prod.rename(columns={0:'estrategia'}) 
        return(prod)
    
    
    #multi   
    matriz_multi= matriz(multi_24,estrat_lote,estrat_bd,False)
    matriz_cov_multi = matriz_multi.cov().reset_index().drop(columns={'index'})
    nova_pct_multi = pct_estrat(res_estrat_final,'ms1',estrat_bd,False)
    contrib_multi = contrib_vol24(matriz_cov_multi,nova_pct_multi,estrat_bd,'ms1')
    #tendencias
    matriz_tendencias= matriz(tendencias_24,estrat_lote,estrat_bd,False)
    matriz_cov_tendencias = matriz_tendencias.cov().reset_index().drop(columns={'index'})
    nova_pct_tendencias = pct_estrat(res_estrat_final,'tge',estrat_bd,False)
    contrib_tendencias = contrib_vol24(matriz_cov_tendencias,nova_pct_tendencias,estrat_bd,'tge')
    
    #tendencias_q
    matriz_tendencias_q= matriz(tendencias_q_24,estrat_lote,estrat_bd,False)
    matriz_cov_tendencias_q = matriz_tendencias_q.cov().reset_index().drop(columns={'index'})
    nova_pct_tendencias_q = pct_estrat(res_estrat_final,'tgq',estrat_bd,False)
    contrib_tendencias_q = contrib_vol24(matriz_cov_tendencias_q,nova_pct_tendencias_q,estrat_bd,'tgq')
    
    #multi2
    matriz_multi2= matriz(multi2_24,estrat_lote,estrat_bd,False)
    matriz_cov_multi2 = matriz_multi2.cov().reset_index().drop(columns={'index'})
    nova_pct_multi2 = pct_estrat(res_estrat_final,'ms2',estrat_bd,False)
    contrib_multi2 = contrib_vol24(matriz_cov_multi2,nova_pct_multi2,estrat_bd,'ms2')
    
    #quali
    matriz_quali= matriz(quali_24,estrat_cont_lote,estrat_cont_bd,True)
    matriz_cov_quali = matriz_quali.cov().reset_index().drop(columns={'index'})
    matriz_corr_quali = matriz_quali.corr().reset_index().drop(columns={'index'})
    nova_pct_quali = pct_estrat(res_estrat_final,'rgq',estrat_cont_bd,False)
    contrib_quali = contrib_vol24(matriz_cov_quali,nova_pct_quali,estrat_cont_bd,'rgq')
    contrib_quali = contrib_quali.rename(columns={0:'estrategia'})         
    
    #geral
    matriz_geral= matriz(geral_24,estrat_cont_lote,estrat_cont_bd,True)
    matriz_cov_geral = matriz_geral.cov().reset_index().drop(columns={'index'})
    nova_pct_geral = pct_estrat(res_estrat_final,'rgm',estrat_cont_bd,False)
    contrib_geral = contrib_vol24(matriz_cov_geral,nova_pct_geral,estrat_cont_bd,'rgm')
    contrib_geral = contrib_geral.rename(columns={0:'estrategia'})     
    
    
    #global 
    
    matriz_global= matriz(global_24, estrat_global_cont_lote, estrat_global_cont_bd,False)
    matriz_cov_global = matriz_global.cov().reset_index().drop(columns={'index'})
    nova_pct_global = pct_estrat(res_estrat_final,'glf',estrat_global_cont_bd,False)
    nova_pct_global  = nova_pct_global[nova_pct_global['tipo'] == 'risco']
    contrib_global = contrib_vol24(matriz_cov_global,nova_pct_global,estrat_global_cont_bd,'glf')
    contrib_global = contrib_global.rename(columns={0:'estrategia'})  
    
    contribuicao_vol =contrib_multi.append([contrib_tendencias,contrib_multi2,contrib_quali,contrib_geral, contrib_global]) 
    
    #contribuicao_vol =contrib_multi.append([contrib_tendencias,contrib_multi2,contrib_quali,contrib_geral]) 
    #---------------------- ES
    #multi
    matriz_multi_es= matriz_es(multi120,estrat_lote,estrat_bd,False)
    matriz_cov_multi_es = matriz_multi_es.cov().reset_index().drop(columns={'index'})
    contrib_multi_es = contrib_es(matriz_cov_multi_es,nova_pct_multi,estrat_bd,'ms1')
    contrib_multi_es = contrib_multi_es.rename(columns={0:'estrategia'})       
    
    #tendencias
    matriz_tendencias_es= matriz_es(tendencias120,estrat_lote,estrat_bd,False)
    matriz_cov_tendencias_es = matriz_tendencias_es.cov().reset_index().drop(columns={'index'})
    contrib_tendencias_es = contrib_es(matriz_cov_tendencias_es,nova_pct_tendencias,estrat_bd,'tge')
    contrib_tendencias_es = contrib_tendencias_es.rename(columns={0:'estrategia'})        
    
    #tendencias_q
    matriz_tendencias_q_es= matriz_es(tendencias_q120,estrat_lote,estrat_bd,False)
    matriz_cov_tendencias_q_es = matriz_tendencias_q_es.cov().reset_index().drop(columns={'index'})
    contrib_tendencias_q_es = contrib_es(matriz_cov_tendencias_q_es,nova_pct_tendencias_q,estrat_bd,'tgq')
    contrib_tendencias_q_es = contrib_tendencias_q_es.rename(columns={0:'estrategia'})        
    
    #multi2
    matriz_multi2_es= matriz_es(multi2120,estrat_lote,estrat_bd,False).dropna()
    matriz_cov_multi2_es = matriz_multi2_es.cov().reset_index().drop(columns={'index'})
    contrib_multi2_es = contrib_es(matriz_cov_multi2_es,nova_pct_multi2,estrat_bd,'ms2')
    contrib_multi2_es = contrib_multi2_es.rename(columns={0:'estrategia'})     
    
    #quali
    
    matriz_quali_es = matriz_es(quali120,estrat_cont_lote,estrat_cont_bd,True)
    quali120_es = quali120_3[quali120_3['Book'].str.contains('OFF TRM')]
    quali120_es = quali120_es.sort_values('total_var').reset_index()
    quali120_es = quali120_es.rename(columns = {'total_var': len(matriz_quali_es.columns)})
    quali120_es = quali120_es.drop(columns={'Book','val_date','index'})
    matriz_quali_es = pd.concat([matriz_quali_es,quali120_es],axis=1)
    matriz_cov_quali_es = matriz_quali_es.cov().reset_index().drop(columns={'index'})
    contrib_quali_es = contrib_es(matriz_cov_quali_es,nova_pct_quali,estrat_cont_bd,'rgq')
    contrib_quali_es = contrib_quali_es.rename(columns={0:'estrategia'})         
    #geral
    matriz_geral_es = matriz_es(geral120,estrat_cont_lote,estrat_cont_bd,True).dropna()
    geral120_es = geral120_3[geral120_3['Book'].str.contains('OFF TRM')]
    geral120_es = geral120_es.sort_values('total_var').reset_index()
    geral120_es = geral120_es.rename(columns = {'total_var': len(matriz_geral_es.columns)})
    geral120_es = geral120_es.drop(columns={'Book','val_date','index'})
    matriz_geral_es = pd.concat([matriz_geral_es,geral120_es],axis=1)
    matriz_cov_geral_es = matriz_geral_es.cov().reset_index().drop(columns={'index'})
    contrib_geral_es = contrib_es(matriz_cov_geral_es,nova_pct_geral,estrat_cont_bd,'rgm')
    contrib_geral_es = contrib_geral_es.rename(columns={0:'estrategia'})         
         
    #global
    
    matriz_global_es= matriz_es(global120, estrat_global_cont_lote, estrat_global_cont_bd,False).dropna()
    matriz_cov_global_es = matriz_global_es.cov().reset_index().drop(columns={'index'})
    contrib_global_es = contrib_es(matriz_cov_global_es,nova_pct_global,estrat_global_cont_bd,'glf')
    contrib_global_es = contrib_global_es.rename(columns={0:'estrategia'})    
    
    contribuicao_es = contrib_multi_es.append([contrib_tendencias_es,contrib_multi2_es,contrib_quali_es,contrib_geral_es, contrib_global_es]) 
    
    #contribuicao_es = contrib_multi_es.append([contrib_tendencias_es,contrib_multi2_es,contrib_quali_es,contrib_geral_es]) 
    #------------------------------ON OFF
    quali_24_on_off = quali_24[quali_24['Book'].isin(estrat_cont_lote)]
    quali_24_off = quali_24_on_off [quali_24_on_off ['Book'].str.contains('OF')]
    quali_24_off = quali_24_off.append(quali_24[quali_24['Book'].str.contains('CAIXA O3 OFF')])
    quali_24_off = quali_24_off.drop(columns=['Book']).groupby('val_date').sum()
    
    quali_24_on = quali_24_on_off [quali_24_on_off ['Book'].str.contains('ON')]
    quali_24_on = quali_24_on.append(quali_24[quali_24['Book'].str.contains('CAIXA O3 ON')])
    quali_24_on = quali_24_on.drop(columns=['Book']).groupby('val_date').sum()
    
    quali_on_off = quali_24_on.merge(quali_24_off,on = 'val_date',how='outer')
    matriz_cov_on_off_quali = quali_on_off.cov().reset_index().drop(columns={'index'})
    matriz_cov_on_off_quali = matriz_cov_on_off_quali.rename(columns = {'total_var_x' : 0,'total_var_y' : 1}) 
    pct_on_off = pct_estrat(res_estrat_final,'rgq',['on','off'],True)
    
    contrib_quali_on_off_vol = contrib_vol24(matriz_cov_on_off_quali, pct_on_off,['on','off'],'rgq')
    
    quali_120_off = quali120_3[quali120_3['Book']=='OFFSHORE']
    quali_120_off = quali_120_off.drop(columns={'Book','val_date'}).sort_values('total_var').reset_index() 
    quali_120_off = quali_120_off.rename(columns={'total_var' : 'total_var_off'})
    quali_120_off = quali_120_off.drop(columns={'index'})
    
    quali_120_on = quali120_3[quali120_3['Book']=='ONSHORE']
    quali_120_on = quali_120_on.drop(columns={'Book','val_date'}).sort_values('total_var').reset_index()
    quali_120_on = quali_120_on.drop(columns={'index'})
    quali_120_on_off = pd.concat([quali_120_on, quali_120_off],axis=1)
    
    matriz_cov_on_off_quali_es = quali_120_on_off.cov().reset_index().drop(columns={'index'})
    matriz_cov_on_off_quali_es = matriz_cov_on_off_quali_es.rename(columns = {'total_var_off' : 0,'total_var' : 1}) 
    pct_on_off_es = pct_on_off [['pct_es_21du','estrategia']].reset_index().drop(columns={'index'})
    pct_on_off_es = pct_on_off_es.rename(columns={'pct_es_21du':'pct_vol_24'})
    
    contrib_quali_on_off_es = contrib_vol24(matriz_cov_on_off_quali_es, pct_on_off_es,['on','off'],'rgq')
    contrib_quali_on_off_es = contrib_quali_on_off_es.rename(columns={'contribuicao_vol_24':'contribuicao_es_21du'})
    
    geral_24_on_off = geral_24[geral_24['Book'].isin(estrat_cont_lote)]
    geral_24_off = geral_24_on_off [geral_24_on_off ['Book'].str.contains('OF')]
    geral_24_off = geral_24_off.append(geral_24[geral_24['Book'].str.contains('CAIXA O3 OFF')])
    geral_24_off = geral_24_off.drop(columns=['Book']).groupby('val_date').sum()
    
    geral_24_on = geral_24_on_off [geral_24_on_off ['Book'].str.contains('ON')]
    geral_24_on = geral_24_on.append(geral_24[geral_24['Book'].str.contains('CAIXA O3 ON')])
    geral_24_on = geral_24_on.drop(columns=['Book']).groupby('val_date').sum()
    
    geral_on_off = geral_24_on.merge(geral_24_off,on = 'val_date',how='outer')
    matriz_cov_on_off_geral = geral_on_off.cov().reset_index().drop(columns={'index'})
    matriz_cov_on_off_geral = matriz_cov_on_off_geral.rename(columns = {'total_var_x' : 0,'total_var_y' : 1}) 
    pct_on_off = pct_estrat(res_estrat_final,'rgq',['on','off'],True)
    
    contrib_geral_on_off_vol = contrib_vol24(matriz_cov_on_off_geral, pct_on_off,['on','off'],'rgm')
    
    geral_120_off = geral120_3[geral120_3['Book']=='OFFSHORE']
    geral_120_off = geral_120_off.drop(columns={'Book','val_date'}).sort_values('total_var').reset_index() 
    geral_120_off = geral_120_off.rename(columns={'total_var' : 'total_var_off'})
    geral_120_off = geral_120_off.drop(columns={'index'})
    
    geral_120_on = geral120_3[geral120_3['Book']=='ONSHORE']
    geral_120_on = geral_120_on.drop(columns={'Book','val_date'}).sort_values('total_var').reset_index()
    geral_120_on = geral_120_on.drop(columns={'index'})
    geral_120_on_off = pd.concat([geral_120_on, geral_120_off],axis=1)
    
    matriz_cov_on_off_geral_es = geral_120_on_off.cov().reset_index().drop(columns={'index'})
    matriz_cov_on_off_geral_es = matriz_cov_on_off_geral_es.rename(columns = {'total_var_off' : 0,'total_var' : 1}) 
    pct_on_off_es = pct_on_off [['pct_es_21du','estrategia']].reset_index().drop(columns={'index'})
    pct_on_off_es = pct_on_off_es.rename(columns={'pct_es_21du':'pct_vol_24'})
    
    
    contrib_geral_on_off_es = contrib_vol24(matriz_cov_on_off_geral_es, pct_on_off_es,['on','off'],'rgm')
    contrib_geral_on_off_es = contrib_geral_on_off_es.rename(columns={'contribuicao_vol_24':'contribuicao_es_21du'})
    
    contribuicao_es = contribuicao_es.append([contrib_quali_on_off_es,contrib_geral_on_off_es]) 
    contribuicao_es ['chave'] = contribuicao_es ['estrategia']+contribuicao_es ['trading_desk']
    
    contribuicao_vol = contribuicao_vol.append([contrib_quali_on_off_vol,contrib_geral_on_off_vol]) 
    contribuicao_vol ['chave'] = contribuicao_vol ['estrategia']+contribuicao_vol ['trading_desk']
    
    contribuicao_final = contribuicao_vol.merge(contribuicao_es)
    contribuicao_final = contribuicao_final.drop(columns={'chave'})
    
    #ajustes
    contribuicao_final ['val_date'] = yesterday.date()
    contribuicao_final ['tipo'] = 'contribuicao'
    contribuicao_final =contribuicao_final.rename(columns={'contribuicao_vol_24':'vol_24_meses','contribuicao_es_21du':'es_21du'})
    contribuicao_final = pd.concat([res_estrat_final,contribuicao_final], axis=0, ignore_index=True)
    res_estrat_final = pd.concat([contribuicao_final,res_book], axis=0, ignore_index=True)
    res_estrat_final['val_date'] =res_estrat_final['val_date']  
    
    
    #--------------------------------- proporcoes-----------------------------   
    res_prop = res_fundo.drop(columns=['val_date','vol_realizada_126du','stress_test']).set_index('trading_desk')
        
    prop_quali = pd.concat([res_prop.loc['glf']/res_prop.loc['rgq'],res_prop.loc['rgm']/res_prop.loc['rgq']],axis=1).transpose()
    prop_quali['estrategia'] = 'retorno_global'
    prop_quali['trading_desk'] = ['glf','rgm']
        
    prop_multi = pd.concat([res_prop.loc['tge']/res_prop.loc['ms1'],res_prop.loc['tgq']/res_prop.loc['ms1'],res_prop.loc['ms2']/res_prop.loc['ms1']],axis=1).transpose()
    prop_multi['estrategia'] = 'multi_strategy'
    prop_multi['trading_desk'] = ['tge','tgq','ms2']
    prop_final = prop_quali.append(prop_multi)
    prop_final['val_date'] = yesterday.date()
    prop_final = prop_final.drop(columns='cauda_sup_21du')
    #--------------------------------- Grafo ---------------------------------
    import networkx as nx
    import matplotlib.pyplot as plt
    k = 2000000
    # Transform it in a links data frame (3 columns only):
    links = matriz_corr_quali.stack().reset_index()
    links.columns = ['var1', 'var2', 'value']
    estrat_grafo = nova_pct_quali[['estrategia','vol_24_meses']].copy().reset_index().drop(columns={'index'})
    estrat_grafo['var1'] = estrat_grafo.index
    links=estrat_grafo.merge(links)
    estrat_grafo['var2'] = estrat_grafo.index
    links=estrat_grafo.drop(columns = {'vol_24_meses','var1'}).merge(links,on='var2')
    
    links['cor'] = np.where(links['value']<0,'#f43737','#21a567')
    
    links['value_modulo'] = abs(links['value'])
    
    links['tamanhos'] = links['vol_24_meses']*k
    
    # Keep only correlation over a threshold and remove self correlation (cor(A,A)=1)
    links_filtered=links.loc[(links['value_modulo'] > 0.4) & (links['var1'] != links['var2'])]
    links_filtered['link'] = True
    
    isolados = links_filtered['estrategia_x'].drop_duplicates().append(links_filtered['estrategia_y'].drop_duplicates())
    isolados = list(isolados.drop_duplicates())
    isolados_df = estrat_grafo[~estrat_grafo['estrategia'].isin(isolados)]
    isolados_df = isolados_df.rename(columns={'estrategia':'estrategia_y'}).sort_values('estrategia_y')
    isolados_df ['tamanhos'] = isolados_df ['vol_24_meses']*k
    
    tamanhos = links_filtered[['tamanhos','estrategia_y']].drop_duplicates(['estrategia_y']).sort_values('estrategia_y').reset_index()
    tamanhos = tamanhos.drop(columns = ['index'])
    tamanhos = pd.concat([tamanhos,isolados_df[['estrategia_y','tamanhos']]],axis=0,ignore_index = True).reset_index()
    tamanhos = tamanhos.drop(columns = ['index'])
    
    # Build your graph
    links_filtered = links_filtered.sort_values('estrategia_x')
    G = nx.from_pandas_edgelist(links_filtered, 'estrategia_x', 'estrategia_y')
    
    G.add_nodes_from(list(isolados_df['estrategia_y']))
    
    # Plot the network:
    fig = plt.figure(figsize=(145,80))
    
    plt.title('O3 Retorno Global Qualificado', fontsize=100,color='#26262b',fontweight="bold")
    pos = nx.spring_layout(G, k=0.7, iterations=20)
    dic_tamanhos = tamanhos.set_index('estrategia_y').T.to_dict('list')
    
    nodes = list(G.nodes())
    size = []
    for i in nodes:
        size.append(dic_tamanhos[i])
        
    nx.draw(G,with_labels=True,node_color='#a1a8ad', font_color="#26262b",width=10,font_weight="bold",node_size = size,edgecolors='#a1a8ad', linewidths=3, font_size=75,edge_color = links_filtered['cor'],alpha=0.9,pos=pos)

    fig.set_facecolor("#e7edee")
    
    fig.savefig(r'I:\Riscos\Grafos\grafo2'+ yesterday_2 +'.jpg')
    #----------------------------- Subindo no BD-------------------------------

    to_alchemy_append_n(res_fundo,'resultados')
    to_alchemy_append_n(res_estrat_final,'resultados_estrategia')
    to_alchemy_append_n(prop_final,'proporcao')
    
    print ('Risco OK.')
    return(res_estrat_final)

def perf_attrib():
  
    semana = (yesterday - BDay(5))
    mes = (yesterday - BDay(21))
    ytd = dt.datetime(2021,1,4)
    mtd = dt.datetime(yesterday.year,yesterday.month,1)
    mtd_str = mtd.strftime("%Y-%m-%d")
    yesterday_count= yesterday.strftime("%Y-%m-%d")
    mtd_n = np.busday_count(mtd_str,yesterday_count)
    
    ytd_geral = dt.datetime(2021,1,4)
    ytd_tendencias = dt.datetime(2021,1,4)
    ytd_tendencias_q = dt.datetime(2021,5,3)
    
    fundos = ['rgq','rgm','ms1','tge','tgq']
    janelas = ['dia','semana','mes','ytd','mtd']
    
    def refresh_excel(SourcePathName,FileName):
        # Open Excel
        Application = win32com.client.Dispatch("Excel.Application") 
        # Show Excel. While this is not required, it can help with debugging
        Application.Visible = 1 
        # Open Your Workbook
        Workbook = Application.Workbooks.open(SourcePathName + '/' + FileName) 
        # Refesh All
        Workbook.RefreshAll() 
        # Saves the Workbook
        Workbook.Save() 
        # Closes Excel
        Application.Quit()
        return 0
    #------------------------------ leitura de dados-------------------------------
    pnl = pd.DataFrame()
    for i in fundos:
            for j in janelas:
                novo_pnl = pd.read_csv('I:/Riscos/PnL/historico/pnl_' + i + '_' + j + '_' + yesterday_2 + '.txt', sep = '\t')
                novo_pnl['trading_desk'] = i
                novo_pnl['janela'] = j
                pnl = pnl.append(novo_pnl)
                
    pnl['product_pl_pct'] = pnl['PositionPL Pct']+pnl['TradesPL Pct']-pnl['FxPL Pct']
    
    #de para
    de_para = pd.read_csv('I:/Riscos/PnL/de_para1.csv')
    pnl = pnl.merge(de_para, how = 'left')
    # verficando se precisa atualiar o DePara
    fora = pnl['Book'][pd.isnull(pnl.estrategia1)].drop_duplicates()
    fora  = pd.DataFrame(fora)
    
    hbp = recupera_bd2('select * from consolidado_pnl')
    de_para_fundos = pd.read_excel('I:/Riscos/Ludmilla/de_para_fundos.xlsx')
    hbp = hbp.merge(de_para_fundos,left_on = 'trading_desk', right_on = 'TradingDesk')
    hbp['val_date'] = pd.to_datetime(hbp['val_date'])
    hbp =hbp.drop(columns = 'trading_desk_x') 
    hbp = hbp.rename(columns = {'trading_desk_y':'trading_desk' })  
    
    #----------------------------------fx pnl--------------------------------------
    if len (fora) > 0:
        fora.to_excel('I:/Riscos/PnL/fora.xlsx') 
        sys.exit('Atualizar DePara antes de rodar novamente!')
         
         
    
    def fx_pnl(df,fundo,janela):
        novo_df = df[df['trading_desk'] == fundo]
        novo_df = novo_df[novo_df['janela'] == janela]
        fx_pnl = novo_df['FxPL Pct'].sum()
        fx_pnl_row = pd.DataFrame([['fx_pnl','fx_pnl',fx_pnl,'fx_pnl','fx_pnl','fx_pnl',janela,fundo]],columns = ['Book','Product','product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])  
        return(fx_pnl_row)
    
    fx_pnl_df = pd.DataFrame()
    for i in fundos:
            for j in janelas:
                a = fx_pnl(pnl, i, j)
                fx_pnl_df = fx_pnl_df.append(a)
    
    fx_pnl_df['estrategia2'] = 'fx_pnl'
    fx_pnl_df['estrategia3'] = 'fx_pnl'
    fx_pnl_df['estrategia4'] = 'fx_pnl'
    
    pnl2= pnl.append(fx_pnl_df,ignore_index=True )
    
    #------------------------------spx ibov----------------------------------------
    ibov = recupera_bd('ibov')
    ibov_feriado = pd.DataFrame([[dt.datetime(2021, 7, 9),125427.8]],columns=['val_date','IBOV Index'])
    ibov_feriado2 = pd.DataFrame([[dt.datetime(2021, 9, 7),117868.6]],columns=['val_date','IBOV Index'])
    ibov = ibov.append([ibov_feriado,ibov_feriado2]).sort_values('val_date')
    spx = recupera_bd('sptr')
    spx = spx.rename(columns={'SPX Index':'SPTR500N Index'})
    sp_feriado = pd.DataFrame([[dt.datetime(2021, 5, 31),7684.3]],columns=['val_date','SPX Index']) 
    sp_feriado2 = pd.DataFrame([[dt.datetime(2021, 7, 5),7962.48]],columns=['val_date','SPX Index']) 
    sp_feriado3 = pd.DataFrame([[dt.datetime(2021, 9, 6),8311.47]],columns=['val_date','SPX Index']) 
    spx = spx.append([sp_feriado,sp_feriado2,sp_feriado3]).sort_values('val_date')
    
    def spx_ibov(spx,ibov,data,sem,mes,ytd1,ytd2,mtd1):
        peso_ibov = 0.03
        peso_spx = 0.10
        dia_spx = spx[spx['val_date']==data]
        
        ret_dia_spx = dia_spx.append(spx[spx['val_date']== data - BDay(1)]).reset_index()
        ret_dia_spx = (ret_dia_spx.iloc[0]['SPX Index']/ret_dia_spx.iloc[1]['SPX Index']-1)*peso_spx
        ret_sem_spx = dia_spx.append(spx[spx['val_date']== sem]).reset_index()
        ret_sem_spx = (ret_sem_spx.iloc[0]['SPX Index']/ret_sem_spx.iloc[1]['SPX Index']-1)*peso_spx
        ret_mes_spx = dia_spx.append(spx[spx['val_date']== mes]).reset_index()
        ret_mes_spx = (ret_mes_spx.iloc[0]['SPX Index']/ret_mes_spx.iloc[1]['SPX Index']-1)*peso_spx
        ret_ytd_spx = dia_spx.append(spx[spx['val_date']== ytd1]).reset_index()
        ret_ytd_spx = (ret_ytd_spx.iloc[0]['SPX Index']/ret_ytd_spx.iloc[1]['SPX Index']-1)*peso_spx
        ret_mtd_spx = dia_spx.append(spx[spx['val_date']== mtd1]).reset_index()
        ret_mtd_spx = (ret_mtd_spx.iloc[0]['SPX Index']/ret_mtd_spx.iloc[1]['SPX Index']-1)*peso_spx
       
        dia_ibov = ibov[ibov['val_date']==data]
        
        ret_dia_ibov = dia_ibov.append(ibov[ibov['val_date']== data - BDay(1)]).reset_index()
        ret_dia_ibov = (ret_dia_ibov.iloc[0]['IBOV Index']/ret_dia_ibov.iloc[1]['IBOV Index']-1)*peso_ibov
        ret_sem_ibov = dia_ibov.append(ibov[ibov['val_date']== sem]).reset_index()
        ret_sem_ibov = (ret_sem_ibov.iloc[0]['IBOV Index']/ret_sem_ibov.iloc[1]['IBOV Index']-1)*peso_ibov
        ret_mes_ibov = dia_ibov.append(ibov[ibov['val_date']== mes]).reset_index()
        ret_mes_ibov = (ret_mes_ibov.iloc[0]['IBOV Index']/ret_mes_ibov.iloc[1]['IBOV Index']-1)*peso_ibov
        ret_ytd_ibov = dia_ibov.append(ibov[ibov['val_date']== ytd2]).reset_index()
        ret_ytd_ibov = (ret_ytd_ibov.iloc[0]['IBOV Index']/ret_ytd_ibov.iloc[1]['IBOV Index']-1)*peso_ibov
        ret_mtd_ibov = dia_ibov.append(ibov[ibov['val_date']== mtd1]).reset_index()
        ret_mtd_ibov = (ret_mtd_ibov.iloc[0]['IBOV Index']/ret_mtd_ibov.iloc[1]['IBOV Index']-1)*peso_ibov
        
        dia_spx = pd.DataFrame([[ret_dia_spx,'indice_spx','indice_spx','equity','dia']], columns = ['product_pl_pct','Book','Product','estrategia1','janela'])
        sem_spx = pd.DataFrame([[ret_sem_spx,'indice_spx','indice_spx','equity','semana']], columns = ['product_pl_pct','Book','Product','estrategia1','janela'])
        mes_spx = pd.DataFrame([[ret_mes_spx,'indice_spx','indice_spx','equity','mes']], columns = ['product_pl_pct','Book','Product','estrategia1','janela'])
        ytd_spx = pd.DataFrame([[ret_ytd_spx,'indice_spx','indice_spx','equity','ytd']], columns = ['product_pl_pct','Book','Product','estrategia1','janela'])
        mtd_spx = pd.DataFrame([[ret_mtd_spx,'indice_spx','indice_spx','equity','mtd']], columns = ['product_pl_pct','Book','Product','estrategia1','janela'])

        dia_ibov = pd.DataFrame([[ret_dia_ibov,'indice_ibov','indice_ibov','equity','dia']], columns = ['product_pl_pct','Book','Product','estrategia1','janela'])
        sem_ibov = pd.DataFrame([[ret_sem_ibov,'indice_ibov','indice_ibov','equity','semana']], columns = ['product_pl_pct','Book','Product','estrategia1','janela'])
        mes_ibov = pd.DataFrame([[ret_mes_ibov,'indice_ibov','indice_ibov','equity','mes']], columns = ['product_pl_pct','Book','Product','estrategia1','janela'])
        ytd_ibov = pd.DataFrame([[ret_ytd_ibov,'indice_ibov','indice_ibov','equity','ytd']], columns = ['product_pl_pct','Book','Product','estrategia1','janela'])
        mtd_ibov = pd.DataFrame([[ret_mtd_ibov,'indice_ibov','indice_ibov','equity','mtd']], columns = ['product_pl_pct','Book','Product','estrategia1','janela'])
        
        spx_ret = dia_spx.append([sem_spx,mes_spx,ytd_spx,mtd_spx])
        spx_ret['estrategia2'] = 'dm'
        spx_ret['estrategia3'] = 'America'
        spx_ret['estrategia4'] = 'indice_spx'
        ibov_ret = dia_ibov.append([sem_ibov,mes_ibov,ytd_ibov,mtd_ibov])
        ibov_ret['estrategia2'] = 'em'
        ibov_ret['estrategia3'] = 'BZ'
        ibov_ret['estrategia4'] = 'indice_ibov'
        final = spx_ret.append(ibov_ret)
        return(final)
    
    
    ytd_sptr = dt.datetime(2020,12,31)
    ytd_ib= dt.datetime(2020,12,30)
    
    spx_ibov_ret = spx_ibov(spx,ibov,yesterday,semana,mes,ytd_sptr,ytd_ib,mtd)
    spx_ret = spx_ibov_ret[spx_ibov_ret['Book'] == 'indice_spx']
    ibov_ret = spx_ibov_ret[spx_ibov_ret['Book'] == 'indice_ibov'] 
    
    tgc_off_spx = spx_ret.copy()
    tgc_off_spx['product_pl_pct'] = - tgc_off_spx['product_pl_pct']
    tgc_off_spx['pm'] = 'tgc_off'
    tgc_off_spx['trading_desk'] = 'rgq'
    tgc_off_spx['on_off'] = 'off'
    
    tgc_on_ibov = ibov_ret.copy()
    tgc_on_ibov['product_pl_pct'] = - tgc_on_ibov['product_pl_pct']
    tgc_on_ibov['pm'] = 'tgc_local'
    tgc_on_ibov['trading_desk'] = 'rgq'
    tgc_on_ibov['on_off'] = 'on'
    
    tdm_spx = spx_ret.copy()
    tdm_spx['pm'] = 'tdm'
    tdm_spx['trading_desk'] = 'rgq'
    tdm_spx['on_off'] = 'off'
    
    tdm_ibov = ibov_ret.copy()
    tdm_ibov['pm'] = 'tdm'
    tdm_ibov['trading_desk'] = 'rgq'
    tdm_ibov['on_off'] = 'on'
    
    pnl3 = pnl2.append([tgc_off_spx,tgc_on_ibov,tdm_spx,tdm_ibov],ignore_index=True )
    
    tgc_off_spx = spx_ret.copy()
    tgc_off_spx['product_pl_pct'] = - tgc_off_spx['product_pl_pct']
    tgc_off_spx['pm'] = 'tgc_off'
    tgc_off_spx['trading_desk'] = 'rgm'
    tgc_off_spx['on_off'] = 'off'
    
    tgc_on_ibov = ibov_ret.copy()
    tgc_on_ibov['product_pl_pct'] = - tgc_on_ibov['product_pl_pct']
    tgc_on_ibov['pm'] = 'tgc_local'
    tgc_on_ibov['trading_desk'] = 'rgm'
    tgc_on_ibov['on_off'] = 'on'
    
    tdm_spx = spx_ret.copy()
    tdm_spx['pm'] = 'tdm'
    tdm_spx['trading_desk'] = 'rgm'
    tdm_spx['on_off'] = 'off'
    
    tdm_ibov = ibov_ret.copy()
    tdm_ibov['pm'] = 'tdm'
    tdm_ibov['trading_desk'] = 'rgm'
    tdm_ibov['on_off'] = 'on'
    
    pnl3 = pnl3.append([tgc_off_spx,tgc_on_ibov,tdm_spx,tdm_ibov],ignore_index=True )
    #-----------------------------------perf fee-----------------------------------
    
    pl_fundos = pd.DataFrame()
    for i in fundos:
        if i == 'glf': 
            pl = hbp[hbp['trading_desk'] == i] 
            pl = pl[pl['book']!=('Default')]
            pl = pl[pl['book']!=('O3')]
            pl = pl[['val_date','position_close']].groupby('val_date').sum().reset_index()
            #pl = pl[pl['val_date']==yesterday]
            pl['trading_desk']=i
            pl_fundos = pl_fundos.append(pl)
        else:
            pl = hbp[hbp['trading_desk'] == i] 
            pl = pl[['val_date','position_close']].groupby('val_date').sum().reset_index()
            #pl = pl[pl['val_date']==yesterday]
            pl['trading_desk']=i
            pl_fundos = pl_fundos.append(pl)
    pl_fundos['val_date'] = pl_fundos['val_date'].dt.date
    #atualizando perf fee no bd
    refresh_fee = refresh_excel('I:/Riscos/PnL','nova_perfee.xlsx')
    nova_perfee = pd.read_excel('I:/Riscos/PnL/nova_perfee.xlsx' )
    nova_perfee['val_date'] = yesterday.date()
    #######################################################
    
    #######################################################atualiza = to_alchemy_append_n(nova_perfee,'perf_fee')
    #######################################################
    #puxando e calculando as perf fees
    perf_fee = recupera_bd('perf_fee')
    perf_fee['val_date'] = perf_fee['val_date'].dt.date
    perf_fee_q = perf_fee[perf_fee['trading_desk']=='rgq']
    perf_fee_g = perf_fee[perf_fee['trading_desk']=='rgm']
    perf_fee_tg = perf_fee[perf_fee['trading_desk']=='tge']
    perf_fee_tq = perf_fee[perf_fee['trading_desk']=='tgq']
    
    perf_fee_q['perf_fee'] = np.where(perf_fee_q['val_date']>dt.datetime(2021,6,30).date(),perf_fee_q['perf_fee'] + 2027639.95,perf_fee_q['perf_fee'])
    perf_fee_g['perf_fee'] = np.where(perf_fee_g['val_date']>dt.datetime(2021,6,30).date(),perf_fee_g['perf_fee']+ 735822.72,perf_fee_g['perf_fee'])
    perf_fee = perf_fee_q.append([perf_fee_g,perf_fee_tg,perf_fee_tq])
    fundos_perfee = ['rgq','rgm','tge','tgq']
    perf_fee_ontem = pd.DataFrame()
    for i in fundos_perfee:
        if i == 'tgq':
            fundo = perf_fee[perf_fee['trading_desk'] ==i].sort_values('val_date')
            pl_fundo = pl_fundos[pl_fundos['trading_desk'] ==i]
            fundo = fundo.merge(pl_fundo,how='outer').sort_values('val_date')
            fundo['aux1'] = fundo['perf_fee'].shift(1)-fundo['perf_fee'] 
            fundo = fundo.fillna(-fundo['perf_fee'][0])
            fundo['aux2'] = (fundo['aux1']/fundo['position_close'].shift(1))+1
            a = min(5,len(fundo['trading_desk']))
            b = min(21,len(fundo['trading_desk']))
            ytd2 = fundo['aux2'].prod()-1
            dia = fundo['aux2'].tail(1).prod()-1
            mtd2 = fundo['aux2'].tail(mtd_n).prod()-1
            semana = fundo['aux2'].tail(a).prod()-1
            mes = fundo['aux2'].tail(b).prod()-1
            perf_fee_ontem = perf_fee_ontem.append(pd.DataFrame([[i,dia,semana,mes,ytd2,mtd2]],columns=['trading_desk','dia','semana','mes','ytd','mtd']))
    
        else:
            fundo = perf_fee[perf_fee['trading_desk'] ==i].sort_values('val_date')
            pl_fundo = pl_fundos[pl_fundos['trading_desk'] ==i]
            fundo = fundo.merge(pl_fundo,how='outer').sort_values('val_date')
            fundo['aux1'] = fundo['perf_fee'].shift(1)-fundo['perf_fee'] 
            fundo = fundo.fillna(-fundo['perf_fee'][0])
            fundo['aux2'] = (fundo['aux1']/fundo['position_close'].shift(1))+1
            ytd2 = fundo['aux2'].prod()-1
            dia = fundo['aux2'].tail(1).prod()-1
            semana = fundo['aux2'].tail(5).prod()-1
            mes = fundo['aux2'].tail(21).prod()-1
            mtd2 = fundo['aux2'].tail(mtd_n).prod()-1
            perf_fee_ontem = perf_fee_ontem.append(pd.DataFrame([[i,dia,semana,mes,ytd2,mtd2]],columns=['trading_desk','dia','semana','mes','ytd','mtd']))
    
    
    
    pl_quali = pl_fundos[pl_fundos['trading_desk'] == 'rgq'].iloc[0]['position_close']
    pl_geral = pl_fundos[pl_fundos['trading_desk'] == 'rgm'].iloc[0]['position_close']
    pl_tendencias = pl_fundos[pl_fundos['trading_desk'] == 'tge'].iloc[0]['position_close']
    pl_tendencias_q = pl_fundos[pl_fundos['trading_desk'] == 'tgq'].iloc[0]['position_close']
    
    
    perf_fee_quali = perf_fee_ontem.iloc[0]['ytd']
    perf_fee_geral = perf_fee_ontem.iloc[1]['ytd']
    perf_fee_tendencias = perf_fee_ontem.iloc[2]['ytd']
    perf_fee_tendencias_q = perf_fee_ontem.iloc[3]['ytd']
    
    
    perf_fee_quali_dia = perf_fee_ontem.iloc[0]['dia']
    perf_fee_geral_dia = perf_fee_ontem.iloc[1]['dia']
    perf_fee_tendencias_dia = perf_fee_ontem.iloc[2]['dia']
    perf_fee_tendencias_q_dia = perf_fee_ontem.iloc[3]['dia']
    
    
    perf_fee_quali_semana = perf_fee_ontem.iloc[0]['semana']
    perf_fee_geral_semana = perf_fee_ontem.iloc[1]['semana']
    perf_fee_tendencias_semana = perf_fee_ontem.iloc[2]['semana']
    perf_fee_tendencias_q_semana = perf_fee_ontem.iloc[3]['semana']
    
    perf_fee_quali_mes = perf_fee_ontem.iloc[0]['mes']
    perf_fee_geral_mes = perf_fee_ontem.iloc[1]['mes']
    perf_fee_tendencias_mes = perf_fee_ontem.iloc[2]['mes']
    perf_fee_tendencias_q_mes = perf_fee_ontem.iloc[3]['mes']
    
    perf_fee_quali_mtd = perf_fee_ontem.iloc[0]['mtd']
    perf_fee_geral_mtd = perf_fee_ontem.iloc[1]['mtd']
    perf_fee_tendencias_mtd = perf_fee_ontem.iloc[2]['mtd']
    perf_fee_tendencias_q_mtd = perf_fee_ontem.iloc[3]['mtd']
    
    feriados = 4
    dias_ytd = np.busday_count(ytd.date(),yesterday.date())-feriados
    
    dias_ytd_geral = np.busday_count(ytd_geral.date(),yesterday.date())-feriados
    dias_ytd_tendencias = np.busday_count(ytd_tendencias.date(),yesterday.date())-feriados
    dias_ytd_tendencias_q = np.busday_count(ytd_tendencias_q.date(),yesterday.date())-1
    
    
    taxa_adm = 1.0195
    taxa_dia_quali = -(taxa_adm**(1/252)-1)+perf_fee_quali_dia
    taxa_sem_quali = -(taxa_adm**(5/252)-1)+perf_fee_quali_semana
    taxa_mes_quali = -(taxa_adm**(21/252)-1)+perf_fee_quali_mes
    taxa_ytd_quali = -(taxa_adm**(dias_ytd/252)-1)+perf_fee_quali
    taxa_mtd_quali = -(taxa_adm**(mtd_n/252)-1)+perf_fee_quali_mtd
    
    taxa_dia_geral = -(taxa_adm**(1/252)-1)+perf_fee_geral_dia
    taxa_sem_geral = -(taxa_adm**(5/252)-1)+perf_fee_geral_semana
    taxa_mes_geral = -(taxa_adm**(21/252)-1)+perf_fee_geral_mes
    taxa_ytd_geral = -(taxa_adm**(dias_ytd_geral/252)-1)+perf_fee_geral
    taxa_mtd_geral = -(taxa_adm**(mtd_n/252)-1)+perf_fee_geral_mtd
    
    taxa_dia_tendencias = -(taxa_adm**(1/252)-1)+perf_fee_tendencias_dia
    taxa_sem_tendencias = -(taxa_adm**(5/252)-1)+perf_fee_tendencias_semana
    taxa_mes_tendencias = -(taxa_adm**(21/252)-1)+perf_fee_tendencias_mes
    taxa_ytd_tendencias = -(taxa_adm**(dias_ytd_tendencias/252)-1)+perf_fee_tendencias
    taxa_mtd_tendencias = -(taxa_adm**(mtd_n/252)-1)+perf_fee_tendencias_mtd
    
    
    taxa_dia_tendencias_q = -(taxa_adm**(1/252)-1)+perf_fee_tendencias_q_dia
    taxa_sem_tendencias_q = -(taxa_adm**(5/252)-1)+perf_fee_tendencias_q_semana
    taxa_mes_tendencias_q = -(taxa_adm**(21/252)-1)+perf_fee_tendencias_q_mes
    taxa_ytd_tendencias_q = -(taxa_adm**(dias_ytd_tendencias_q/252)-1)+perf_fee_tendencias_q
    taxa_mtd_tendencias_q = -(taxa_adm**(mtd_n/252)-1)+perf_fee_tendencias_q_mtd
    
    custos_dia_quali = pd.DataFrame([[taxa_dia_quali,'custos','custos','custos','dia','rgq']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    custos_sem_quali = pd.DataFrame([[taxa_sem_quali,'custos','custos','custos','semana','rgq']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    custos_mes_quali = pd.DataFrame([[taxa_mes_quali,'custos','custos','custos','mes','rgq']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    custos_ytd_quali = pd.DataFrame([[taxa_ytd_quali,'custos','custos','custos','ytd','rgq']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    custos_mtd_quali = pd.DataFrame([[taxa_mtd_quali,'custos','custos','custos','mtd','rgq']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    
    custos_quali = custos_dia_quali.append([custos_sem_quali,custos_mes_quali,custos_ytd_quali,custos_mtd_quali])
    custos_quali['Book'] = 'custos'
    custos_quali['Product'] = 'custos'
    custos_quali['estrategia2'] = 'custos'
    custos_quali['estrategia3'] = 'custos'
    custos_quali['estrategia4'] = 'custos'
    
    custos_dia_geral = pd.DataFrame([[taxa_dia_geral,'custos','custos','custos','dia','rgm']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    custos_sem_geral = pd.DataFrame([[taxa_sem_geral,'custos','custos','custos','semana','rgm']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    custos_mes_geral = pd.DataFrame([[taxa_mes_geral,'custos','custos','custos','mes','rgm']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    custos_ytd_geral = pd.DataFrame([[taxa_ytd_geral,'custos','custos','custos','ytd','rgm']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    custos_mtd_geral = pd.DataFrame([[taxa_mtd_geral,'custos','custos','custos','mtd','rgm']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    
    custos_geral = custos_dia_geral.append([custos_sem_geral,custos_mes_geral,custos_ytd_geral,custos_mtd_geral])
    custos_geral['Book'] = 'custos'
    custos_geral['Product'] = 'custos'
    custos_geral['estrategia2'] = 'custos'
    custos_geral['estrategia3'] = 'custos'
    custos_geral['estrategia4'] = 'custos'
    
    
    custos_dia_tendencias = pd.DataFrame([[taxa_dia_tendencias,'custos','custos','custos','dia','tge']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    custos_sem_tendencias = pd.DataFrame([[taxa_sem_tendencias,'custos','custos','custos','semana','tge']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    custos_mes_tendencias = pd.DataFrame([[taxa_mes_tendencias,'custos','custos','custos','mes','tge']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    custos_ytd_tendencias = pd.DataFrame([[taxa_ytd_tendencias,'custos','custos','custos','ytd','tge']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    custos_mtd_tendencias = pd.DataFrame([[taxa_mtd_tendencias,'custos','custos','custos','mtd','tge']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    
    custos_tendencias = custos_dia_tendencias.append([custos_sem_tendencias,custos_mes_tendencias,custos_ytd_tendencias,custos_mtd_tendencias])
    custos_tendencias['Book'] = 'custos'
    custos_tendencias['Product'] = 'custos'
    custos_tendencias['estrategia2'] = 'custos'
    custos_tendencias['estrategia3'] = 'custos'
    custos_tendencias['estrategia4'] = 'custos'
    
    custos_dia_tendencias_q = pd.DataFrame([[taxa_dia_tendencias_q,'custos','custos','custos','dia','tgq']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    custos_sem_tendencias_q = pd.DataFrame([[taxa_sem_tendencias_q,'custos','custos','custos','semana','tgq']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    custos_mes_tendencias_q = pd.DataFrame([[taxa_mes_tendencias_q,'custos','custos','custos','mes','tgq']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    custos_ytd_tendencias_q = pd.DataFrame([[taxa_ytd_tendencias_q,'custos','custos','custos','ytd','tgq']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    custos_mtd_tendencias_q = pd.DataFrame([[taxa_mtd_tendencias_q,'custos','custos','custos','mtd','tgq']],columns = ['product_pl_pct','pm','on_off','estrategia1','janela','trading_desk'])
    custos_tendencias_q = custos_dia_tendencias_q.append([custos_sem_tendencias_q,custos_mes_tendencias_q,custos_ytd_tendencias_q,custos_mtd_tendencias_q])
    custos_tendencias_q['Book'] = 'custos'
    custos_tendencias_q['Product'] = 'custos'
    custos_tendencias_q['estrategia2'] = 'custos'
    custos_tendencias_q['estrategia3'] = 'custos'
    custos_tendencias_q['estrategia4'] = 'custos'
    
    pnl_final = pnl3.append([custos_quali,custos_geral,custos_tendencias,custos_tendencias_q], ignore_index = True)
    
    #ajustes
    dic = {'Book':'book',
           'FxPL':'fx_pnl',
           'FxPL Pct':'fx_pl_pct',
           'PositionPL': 'position_pl',
           'PositionPL Pct':'position_pl_pct',
           'Product':'product',
           'Product Pct':'product_pct',
           'Product Pos Pct':'product_pos_pct',
           'ProductClass':'product_class',
           'TradesPL':'trades_pl',
           'TradesPL Pct':'trades_pl_pct' 
           }
    pnl_final = pnl_final.rename(columns = dic)
    pnl_final['val_date'] = yesterday.date()
    
    to_alchemy_append(pnl_final,'perf_attribution')
    to_alchemy_append_n(pnl_final,'perf_attribution')
    
    print('Performance Attribution OK.')
    return (pnl_final)


def tgc_off():

    yesterday_arq = yesterday.strftime("%Y%m%d")
    fundos = ['rgq']
    #-------------------------------Calculo PL------------------------------------
    hbp = recupera_bd2('select * from consolidado_pnl')
    de_para_fundos = pd.read_excel('I:/Riscos/Ludmilla/de_para_fundos.xlsx')
    #de_para_fundos = de_para_fundos.rename(columns={'TradingDesk': 'trading_desk'})
    hbp = hbp.merge(de_para_fundos,left_on = 'trading_desk', right_on = 'TradingDesk')
    hbp['val_date'] = pd.to_datetime(hbp['val_date'])
    hbp =hbp.drop(columns = 'trading_desk_x') 
    hbp = hbp.rename(columns = {'trading_desk_y':'trading_desk' })  
    
    
    pl_fundos = pd.DataFrame()
    for i in fundos:
        if i == 'glf': 
            pl = hbp[hbp['trading_desk'] == i] 
            pl = pl[pl['book']!=('Default')]
            pl = pl[pl['book']!=('O3')]
            pl = pl[['val_date','position_close']].groupby('val_date').sum().reset_index()
            pl = pl[pl['val_date']==yesterday]
            pl['trading_desk']=i
            pl_fundos = pl_fundos.append(pl)
        else:
            pl = hbp[hbp['trading_desk'] == i] 
            pl = pl[['val_date','position_close']].groupby('val_date').sum().reset_index()
            pl = pl[pl['val_date']==yesterday]
            pl['trading_desk']=i
            pl_fundos = pl_fundos.append(pl)

    #-------------------------------Importacao de dados----------------------------    
    
    def le_dados(fundo,nivel):
        if 'param' in fundo:
            tabela = pd.read_csv('I:/Riscos/TotalVaR/historico/' + fundo + '_nivel' + nivel +'_' + yesterday_arq + '.txt', sep='\t',parse_dates=['ValDate'])
            tabela = tabela.rename(columns = {'ValDate' : 'val_date', 'ParametricVaR' : 'param_var'})
            tabela['val_date'] = tabela['val_date'].dt.date 
        elif 'stress' in fundo:
            tabela = pd.read_csv('I:/Riscos/TotalVaR/historico/' + fundo + '_nivel' + nivel +'_' + yesterday_arq + '.txt', sep='\t')
        else:
            tabela = pd.read_csv('I:/Riscos/TotalVaR/historico/' + fundo + '_nivel' + nivel +'_' + yesterday_arq + '.txt', sep='\t',parse_dates=['Date'])
            tabela = tabela.rename(columns = {'Date' : 'val_date', 'TotalVar' : 'total_var'})
            tabela['val_date'] = tabela['val_date'].dt.date 
        return (tabela)  
    
    #-------------------------------Funcoes Rotina---------------------------------
    
    def resultados_book2(df,data,fundo,book):   
        df = df.sort_values('val_date')
        vol_24 = abs(np.std(df['total_var'].tail(504))*math.sqrt(252))
        vol_60 = abs(np.std(df['total_var'].tail(1260))*math.sqrt(252))
        percentil = np.percentile(df['total_var'],1)
        var_1pct = abs(percentil)
        filtro_es = df[df['total_var']<percentil]
        es_pct = abs(np.mean(filtro_es['total_var'])*math.sqrt(21))
        ewma = list()
        ewma.append(0)
        tv = list(df['total_var'])
        for i in range(1,len(df)):
            ewma_nova =math.sqrt( 0.94*ewma[i-1]*ewma[i-1]+(1-0.94)*tv[i]*tv[i])
            ewma.append(ewma_nova)
        ewma_ano = pd.DataFrame(ewma)*math.sqrt(252)
        vol_ewma = ewma_ano.tail(1)
        vol_ewma = abs(vol_ewma.iloc[0][0])
        resultados = pd.DataFrame([[data,fundo,book,'te_equities',vol_24,vol_60,vol_ewma,es_pct,var_1pct]],columns=['val_date','trading_desk','estrategia','tipo','vol_24_meses','vol_60_meses','vol_ewma','es_21du','var_1du'])
        return(resultados)  
      
    #Books TGC e TML
    ibov = recupera_bd('ibov')
    ibov['ibov'] = ibov['IBOV Index']/ibov['IBOV Index'].shift(1)-1 
    spx = recupera_bd('spx')
    spx['spx'] = spx['SPX Index']/spx['SPX Index'].shift(1)-1 
    
    pct_tgc_off = 0.10
    pct_tgc_on = 0.03
    
    pl_quali = pl_fundos[pl_fundos['trading_desk'] == 'rgq'].iloc[0]['position_close']
    
    #pl_geral = pl_fundos[pl_fundos['trading_desk'] == 'rgm'].iloc[0]['position_close']
    
    # leitura de dados

    tgc_off_quali = pd.read_csv('I:/Riscos/TotalVaR/historico/usd_brl/tgc_off_quali_' + yesterday_arq + '.txt', sep='\t',parse_dates=['Date'])
    tgc_off_quali = tgc_off_quali.rename(columns={'Date':'val_date'})

    # ajustes off
    tgc_off_quali['aux'] = (tgc_off_quali['TotalVar']-tgc_off_quali['Total USDBRLSpot'])/(pl_quali*pct_tgc_off)   
    tgc_off_quali = tgc_off_quali.merge(spx,how = 'left')
    
    tgc_off_quali['dif_off_spx'] =  tgc_off_quali['aux'] - tgc_off_quali['spx']
    
    tgc_off_quali = tgc_off_quali[['val_date','dif_off_spx']].fillna(0)
    tgc_off_quali = tgc_off_quali.rename(columns = {'dif_off_spx':'total_var'})
    
    #--------------------------- inicio ---------------------------
    tgc_off_quali = tgc_off_quali.sort_values('val_date')
    
    def multi_period_return(period_returns):
        return np.prod(period_returns + 1) - 1
    
    tgc_off_quali ['rolling_ret_3m'] = tgc_off_quali ['total_var'].rolling(63).apply(multi_period_return)
    tgc_off_quali ['rolling_ret_6m'] = tgc_off_quali ['total_var'].rolling(126).apply(multi_period_return)
    tgc_off_quali ['rolling_ret_12m'] = tgc_off_quali ['total_var'].rolling(252).apply(multi_period_return)
    
    tgc_off_quali ['rolling_vol_3m'] = abs(tgc_off_quali ['total_var'].rolling(63).std()*math.sqrt(63))
    tgc_off_quali ['rolling_vol_6m'] = abs(tgc_off_quali ['total_var'].rolling(126).std()*math.sqrt(126))
    tgc_off_quali ['rolling_vol_12m'] = abs(tgc_off_quali ['total_var'].rolling(252).std()*math.sqrt(252))
    
    
    tgc_off_quali ['rolling_vol_3m_aa'] = abs(tgc_off_quali ['total_var'].rolling(63).std()*math.sqrt(252))
    tgc_off_quali ['rolling_vol_6m_aa'] = abs(tgc_off_quali ['total_var'].rolling(126).std()*math.sqrt(252))
    tgc_off_quali ['rolling_vol_12m_aa'] = abs(tgc_off_quali ['total_var'].rolling(252).std()*math.sqrt(252))
    tgc_off_quali ['rolling_vol_24m_aa'] = abs(tgc_off_quali ['total_var'].rolling(504).std()*math.sqrt(252))
    
    tgc_off_quali ['rolling_sharpe_3m'] = tgc_off_quali ['rolling_ret_3m']/tgc_off_quali ['rolling_vol_3m']
    tgc_off_quali ['rolling_sharpe_6m'] = tgc_off_quali ['rolling_ret_6m']/tgc_off_quali ['rolling_vol_6m']
    tgc_off_quali ['rolling_sharpe_12m'] = tgc_off_quali ['rolling_ret_12m']/tgc_off_quali ['rolling_vol_12m']
    
    
    ten_year = pd.read_csv('I:/Riscos/Maxdrawdown/10yr_treasury.csv')
    ten_year.columns = ['val_date','10_yr_index']
    ten_year['val_date'] = pd.to_datetime(ten_year['val_date']) 
    ten_year['10_yr_returns'] =  ten_year['10_yr_index']-ten_year['10_yr_index'].shift(1)
    tgc_off_quali = tgc_off_quali.merge(ten_year)
    
    tgc_off_quali['cor_10yr_treas_1m'] = tgc_off_quali['total_var'].rolling(21).corr(tgc_off_quali['10_yr_returns'])
    tgc_off_quali['cor_10yr_treas_6m'] = tgc_off_quali['total_var'].rolling(126).corr(tgc_off_quali['10_yr_returns'])
    tgc_off_quali['cor_10yr_treas_12m'] = tgc_off_quali['total_var'].rolling(252).corr(tgc_off_quali['10_yr_returns'])
    tgc_off_quali['ret_acumulado'] = (tgc_off_quali['total_var']+1).cumprod() -1
    tgc_off_quali = tgc_off_quali.rename(columns = {'total_var':'retorno_tgc'})
    
    
    to_alchemy_replace(tgc_off_quali,'tgc_off_rolling')
    to_alchemy_replace_n(tgc_off_quali,'tgc_off_rolling')

    print ('Backtest TGC Off Ok!')
    return(0)

class janela:
  
    def __init__(self, raiz):
      
        self.fr1 = Frame(raiz)
        self.fr1.pack()
        self.lb1 = Label(self.fr1,text = 'Rotinas da Manhã',font=('bold'),bg='white',height = 1, width = 20)
        self.lb1.pack()

        self.b15 = Button(self.fr1,text = 'Atualizar Cambio',command = atualiza_cambio,height = 1, width = 20)
        self.b15.pack()
        self.b12 = Button(self.fr1,text = 'Atualizar Fatores',command = atualiza_fatores,height = 1, width = 20)
        self.b12.pack()
        self.b4 = Button(self.fr1,text = 'VaR',command = var,height = 1, width = 20)
        self.b4.pack()
        self.b11 = Button(self.fr1,text = 'Backtest TGC Off',command = tgc_off,height = 1, width = 20)
        self.b11.pack()
        self.b9 = Button(self.fr1,text = 'Performance Attribution',command = perf_attrib,height = 1, width = 20)
        self.b9.pack()
        self.lb1 = Label(self.fr1,text = 'Rotinas da Tarde',font=('bold'),bg='white',height = 1, width = 20)
        self.lb1.pack()
        self.b13 = Button(self.fr1,text = 'Atualizar Obs',command = atualiza_obs,height = 1, width = 20)
        self.b13.pack()  
  
raiz = Tk()
janela (raiz)
raiz.mainloop()
