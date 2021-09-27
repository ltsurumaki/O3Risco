# -*- coding: utf-8 -*-
"""
Created on Thu Jun 17 16:06:26 2021

@author: ludmilla.tsurumaki
"""
import os
import pandas as pd
import pickle
import numpy as np
import math
import datetime as dt
from pandas.tseries.offsets import BDay
from sqlalchemy import create_engine
import telegram_send

lastBusDay = dt.datetime.today()
yesterday = lastBusDay - BDay(1)
yesterday = dt.datetime(yesterday.year,yesterday.month,yesterday.day)
data_csv = yesterday.strftime("%d%b%Y")
yesterday_bbg = yesterday.strftime("%m/%d/%Y")
yesterday_2 = yesterday.strftime("%Y%m%d")


#------------------------------FUNCOES AUXILIARES------------------------------


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

#################################### Dolar ###############################
dolar = recupera_bd('cambio')
dolar = dolar.iloc[0][0]                                  
data_csv = yesterday.strftime("%d%b%Y")
################################# proporcao ##############################
#proporcao_fundos = recupera_bd2('proporcao_fundos')
proporcao_fundos = pd.read_excel(r'I:\Riscos\Maxdrawdown\prop.xlsx')

prop_quali = proporcao_fundos[proporcao_fundos['Origin']=='O3 MACRO INTERNATIONAL FUND']
prop_quali = prop_quali[prop_quali['Destination']=='O3 RETORNO GLOBAL QUALIFICADO MASTER FIM']
prop_quali = prop_quali[['Date','Amount']]
prop_quali = prop_quali.rename(columns={'Date':'val_date'})
prop_quali ['Amount'] =prop_quali ['Amount']/100
'''
prop_geral = proporcao_fundos[proporcao_fundos['fundo_origem']=='O3 MACRO INTERNATIONAL FUND']
prop_geral = prop_geral[prop_geral['fundo_destino']=='O3 RETORNO GLOBAL MASTER FIM']
prop_geral = prop_geral[['data_ref','proporcao']]
prop_geral = prop_geral.rename(columns={'data_ref':'val_date'})
prop_geral ['proporcao'] =prop_geral ['proporcao']/100
'''
#-----------Atualizando a Historical Book Performance--------------------------
hbp = recupera_bd2('select * from consolidado_pnl')

#de_para_fundos = pd.read_excel('C:/Users/o3capital/Documents/Ludmilla/de_para_fundos.xls')

de_para_fundos = pd.read_excel('I:/Riscos/Ludmilla/de_para_fundos.xls')


#de_para_fundos = de_para_fundos.rename(columns={'TradingDesk': 'trading_desk'})
hbp = hbp.merge(de_para_fundos,left_on = 'trading_desk', right_on = 'TradingDesk')
hbp['val_date'] = pd.to_datetime(hbp['val_date'])
hbp =hbp.drop(columns = 'trading_desk_x') 
hbp = hbp.rename(columns = {'trading_desk_y':'trading_desk' })  
    
quali = hbp[hbp['trading_desk'] =='rgq']
geral = hbp[hbp['trading_desk'] =='rgm']
macro_int = hbp[hbp['trading_desk'] =='mif']
multi = hbp[hbp['trading_desk'] =='ms1']
tendencias = hbp[hbp['trading_desk'] =='tge']
tendencias_q = hbp[hbp['trading_desk'] =='tgq']
multi2 = hbp[hbp['trading_desk'] =='ms2']
global_fund2 = hbp[hbp['trading_desk'] =='glf']
global_fund = hbp[hbp['trading_desk'] =='glf']
hedge = hbp[hbp['trading_desk'] =='hdf']

global_fund = global_fund[global_fund['book']!=('Default')]
global_fund = global_fund[global_fund['book']!=('O3')]
##############################################################################

position = quali[['val_date','position_close']].groupby('val_date').sum().reset_index()
position_geral = geral[['val_date','position_close']].groupby('val_date').sum().reset_index()
position_multi = multi[['val_date','position_close']].groupby('val_date').sum().reset_index()
position_multi2 = multi2[['val_date','position_close']].groupby('val_date').sum().reset_index()
position_tendencias = tendencias[['val_date','position_close']].groupby('val_date').sum().reset_index()
position_tendencias_q = tendencias_q[['val_date','position_close']].groupby('val_date').sum().reset_index()


#PL Global Fund
position_global_fund = global_fund[['val_date','position_close']].groupby('val_date').sum().reset_index()
position_hedge = hedge[['val_date','position_close']].groupby('val_date').sum().reset_index()
#dolar =5.40650044844538
sta_rita = 66325534.8
sta_rita2 = 48268813.44
position_hedge['position_close2'] = np.where(position_hedge['val_date']<dt.datetime.strptime('2021-03-17', '%Y-%m-%d'),(position_hedge['position_close'] + sta_rita) ,position_hedge['position_close'])
position_hedge['position_close3'] = np.where(position_hedge['val_date']>dt.datetime.strptime('2021-03-16', '%Y-%m-%d'),(position_hedge['position_close'] + sta_rita2) ,0)
position_hedge['position_close3'] = np.where(position_hedge['val_date']>=dt.datetime.strptime('2021-04-16', '%Y-%m-%d'), 0,position_hedge['position_close3'])

position_hedge['position_close4'] = np.where(position_hedge['position_close3']== 0,position_hedge['position_close2'],position_hedge['position_close3'])
position_hedge['position_close4'] = np.where(position_hedge['val_date']>dt.datetime.strptime('2021-04-15', '%Y-%m-%d'),position_hedge['position_close'],position_hedge['position_close4'])
position_hedge = position_hedge.rename(columns={'position_close4':'position_close_hd'})
position_hedge = position_hedge[['val_date','position_close_hd']]

position_global_fund = position_global_fund.merge(position_hedge)
position_global_fund['position_close'] = position_global_fund['position_close'] + position_global_fund['position_close_hd']/dolar
position_global_fund = position_global_fund.drop(columns = 'position_close_hd')


position_global_fund2 = global_fund2[['val_date','position_close']].groupby('val_date').sum().reset_index()
position_global_fund2 = position_global_fund2.merge(position_hedge, on ='val_date')
position_global_fund2['position_close'] = position_global_fund2['position_close']*dolar + position_global_fund2['position_close_hd'] 
position_global_fund2['position_close'] = np.where(position_global_fund2['val_date']<dt.datetime.strptime('2021-04-07', '%Y-%m-%d'),0,position_global_fund2['position_close'])

fundo = quali[['val_date','pl_total']].groupby('val_date').sum().reset_index()
fundo_geral = geral[['val_date','pl_total']].groupby('val_date').sum().reset_index()
fundo_multi = multi[['val_date','pl_total']].groupby('val_date').sum().reset_index()
fundo_multi_2 = multi2[['val_date','pl_total']].groupby('val_date').sum().reset_index()
fundo_tendencias = tendencias[['val_date','pl_total']].groupby('val_date').sum().reset_index()
fundo_tendencias_q = tendencias_q[['val_date','pl_total']].groupby('val_date').sum().reset_index()
fundo_global_fund = global_fund[['val_date','pl_total']].groupby('val_date').sum().reset_index()

#lista de datas para filtra dados importados do BBG
datas = fundo['val_date']
#-----------------------------------Import BBG---------------------------------
cdi = recupera_bd('cdi')
cdi['cdi']= cdi['BZACCETP Index']/cdi['BZACCETP Index'].shift(1)-1
cdi.columns = ['val_date','BZACCETP Index','cdi']
cdi = cdi.merge(datas)

ibov = recupera_bd('ibov')
ibov.columns = ['val_date','Ibov Index']
ibov['ibov'] = ibov['Ibov Index']/ibov['Ibov Index'].shift(1)-1 
ibov = ibov.merge(datas)


brl = recupera_bd('brl')
brl.columns = ['val_date','brl_curncy'] 
brl['brl'] = brl['brl_curncy']/brl['brl_curncy'].shift(1)-1 
brl = brl.merge(datas)


spx = recupera_bd('spx')
spx.columns = ['val_date','spx Index']
spx['spx'] = spx['spx Index']/spx['spx Index'].shift(1)-1 
spx = spx.merge(datas)


sptr = recupera_bd('sptr')
sptr.columns = ['val_date','sptr Index']
sptr = sptr.merge(datas,how='outer').sort_values('val_date')
sptr = sptr.ffill().bfill()

##############################################################################
#------------------------Maxdrawdown Quali------------------------------------

fundo = fundo.merge(cdi)
#Subtraindo o CDI 
fundo['fundo'] = fundo['pl_total']/position['position_close'].shift(1)-fundo['cdi']
fundo = fundo.drop(columns = ['pl_total','cdi','BZACCETP Index'])
#############
fundo['fundo'] = fundo['fundo']+1.00
fundo = fundo.fillna(100.00)
fundo['fundo'] = fundo['fundo'].cumprod()
#############
#fundo2 = Maxdrawdown
fundo2 = fundo.copy()
fundo2['aux'] = np.where(fundo['fundo']>100,fundo['fundo'],100.00)
fundo2['aux'] = fundo2['aux'].cummax()
fundo2['fundo'] = fundo2['fundo']/fundo2['aux']-1
fundo2 = fundo2.drop(columns = ['aux'])
fundo['fundo'][0] = math.nan
fundo2['fundo'][0] = math.nan
#fundo3 = YtD PNL
fundo3 = fundo.copy()
fundo3['fundo'] = fundo3['fundo']/100.00-1.00

#------------------------Maxdrawdown Geral------------------------------------
fundo_geral = fundo_geral.merge(cdi)
#Subtraindo o CDI 
fundo_geral['fundo_geral'] = fundo_geral['pl_total']/position_geral['position_close'].shift(1)-fundo_geral['cdi']
fundo_geral = fundo_geral.drop(columns = ['pl_total','cdi','BZACCETP Index'])
#############
fundo_geral['fundo_geral'] = fundo_geral['fundo_geral']+1.00
fundo_geral = fundo_geral.fillna(100.00)
fundo_geral['fundo_geral'] = fundo_geral['fundo_geral'].cumprod()
#############
#fundo_geral2 = Maxdrawdown
fundo_geral2 = fundo_geral.copy()
fundo_geral2['aux'] = np.where(fundo_geral['fundo_geral']>100,fundo_geral['fundo_geral'],100.00)
fundo_geral2['aux'] = fundo_geral2['aux'].cummax()
fundo_geral2['fundo_geral'] = fundo_geral2['fundo_geral']/fundo_geral2['aux']-1
fundo_geral2 = fundo_geral2.drop(columns = ['aux'])
fundo_geral['fundo_geral'][0] = math.nan
fundo_geral2['fundo_geral'][0] = math.nan
#fundo_geral3 = YtD PNL
fundo_geral3 = fundo_geral.copy()
fundo_geral3['fundo_geral'] = fundo_geral3['fundo_geral']/100.00-1.00
#------------------------Maxdrawdown global_fund------------------------------------
fundo_global_fund = fundo_global_fund.merge(cdi)
#Subtraindo o CDI 
fundo_global_fund['fundo_global_fund'] = fundo_global_fund['pl_total']/position_global_fund['position_close'].shift(1)-fundo_global_fund['cdi']
fundo_global_fund = fundo_global_fund.drop(columns = ['pl_total','cdi','BZACCETP Index'])
#############
fundo_global_fund['fundo_global_fund'] = fundo_global_fund['fundo_global_fund']+1.00
fundo_global_fund = fundo_global_fund.fillna(100.00)
fundo_global_fund['fundo_global_fund'] = fundo_global_fund['fundo_global_fund'].cumprod()
#############
#fundo_global_fund2 = Maxdrawdown
fundo_global_fund2 = fundo_global_fund.copy()
fundo_global_fund2['aux'] = np.where(fundo_global_fund['fundo_global_fund']>100,fundo_global_fund['fundo_global_fund'],100.00)
fundo_global_fund2['aux'] = fundo_global_fund2['aux'].cummax()
fundo_global_fund2['fundo_global_fund'] = fundo_global_fund2['fundo_global_fund']/fundo_global_fund2['aux']-1
fundo_global_fund2 = fundo_global_fund2.drop(columns = ['aux'])
fundo_global_fund['fundo_global_fund'][0] = math.nan
fundo_global_fund2['fundo_global_fund'][0] = math.nan
#fundo_global_fund3 = YtD PNL
fundo_global_fund3 = fundo_global_fund.copy()
fundo_global_fund3['fundo_global_fund'] = fundo_global_fund3['fundo_global_fund']/100.00-1.00


##############################################################################
#-----------Porcentagens-------------
pct_tdm = 0.6
pct_tnk = 0.1
pct_tml = 0.1
pct_trm = 0.05
pct_tgc_local = 0.03 
pct_tgc_off = 0.1
pct_tdd = 0.1

#------------------------Maxdrawdown TDM Quali---------------------------------
tdm = quali[quali['book']=='TDM']
tdm = tdm[['val_date','pnl_ex_fx']].groupby('val_date').sum().reset_index()
tdm['tdm'] = tdm['pnl_ex_fx']/(position['position_close'].shift(1)*pct_tdm)
tdm = tdm.drop(columns = ['pnl_ex_fx'])

tdm['tdm'] = tdm['tdm']+1
tdm = tdm.fillna(100)
tdm['tdm'] = tdm['tdm'].cumprod()
#tdm2 = Maxdrawdown
tdm2 = tdm.copy()
tdm2['aux'] = np.where(tdm['tdm']>100,tdm['tdm'],100)
tdm2['aux'] = tdm2['aux'].cummax()
tdm2['tdm'] = tdm2['tdm']/tdm2['aux']-1
tdm2 = tdm2.drop(columns = ['aux'])
tdm['tdm'][0] = math.nan
tdm2['tdm'][0] = math.nan
#tdm3 = YtD pnl
tdm3 = tdm.copy()
tdm3['tdm'] = tdm3['tdm']/100-1
#------------------------Maxdrawdown TDM Geral---------------------------------
tdm_geral = geral[geral['book']=='TDM']
tdm_geral = tdm_geral[['val_date','pnl_ex_fx']].groupby('val_date').sum().reset_index()
tdm_geral['tdm'] = tdm_geral['pnl_ex_fx']/(position_geral['position_close'].shift(1)*pct_tdm)
tdm_geral = tdm_geral.drop(columns = ['pnl_ex_fx'])

tdm_geral['tdm'] = tdm_geral['tdm']+1
tdm_geral = tdm_geral.fillna(100)
tdm_geral['tdm'] = tdm_geral['tdm'].cumprod()
#tdm_geral2 = Maxdrawdown
tdm_geral2 = tdm_geral.copy()
tdm_geral2['aux'] = np.where(tdm['tdm']>100,tdm_geral['tdm'],100)
tdm_geral2['aux'] = tdm_geral2['aux'].cummax()
tdm_geral2['tdm'] = tdm_geral2['tdm']/tdm_geral2['aux']-1
tdm_geral2 = tdm_geral2.drop(columns = ['aux'])
tdm_geral['tdm'][0] = math.nan
tdm_geral2['tdm'][0] = math.nan
#tdm_geral3 = YtD pnl
tdm_geral3 = tdm_geral.copy()
tdm_geral3['tdm'] = tdm_geral3['tdm']/100-1
##############################################################################
#------------------------Maxdrawdown TNK Quali---------------------------------
tnk = quali[quali['book']=='TNK']
tnk = tnk[tnk['val_date']>= dt.datetime(2021,8,1)]
tnk = tnk[['val_date','pnl_ex_fx']].groupby('val_date').sum().reset_index()
tnk = tnk.merge(position)
tnk['tnk'] = tnk['pnl_ex_fx']/(tnk['position_close'].shift(1)*pct_tnk)
tnk = tnk.drop(columns = ['pnl_ex_fx'])

tnk['tnk'] = tnk['tnk']+1
tnk = tnk.fillna(100)
tnk['tnk'] = tnk['tnk'].cumprod()
#tnk2 = Maxdrawdown
tnk2 = tnk.copy()
tnk2['aux'] = np.where(tnk['tnk']>100,tnk['tnk'],100)
tnk2['aux'] = tnk2['aux'].cummax()
tnk2['tnk'] = tnk2['tnk']/tnk2['aux']-1
tnk2 = tnk2.drop(columns = ['aux'])
tnk['tnk'][0] = math.nan
tnk2['tnk'][0] = math.nan
#tnk3 = YtD pnl
tnk3 = tnk.copy()
tnk3['tnk'] = tnk3['tnk']/100-1
#------------------------Maxdrawdown TNK Geral---------------------------------
tnk_geral = geral[geral['book']=='TNK']
tnk_geral = tnk_geral[tnk_geral['val_date']>= dt.datetime(2021,8,1)]
tnk_geral = tnk_geral[['val_date','pnl_ex_fx']].groupby('val_date').sum().reset_index()
tnk_geral = tnk_geral.merge(position_geral)
tnk_geral['tnk'] = tnk_geral['pnl_ex_fx']/(tnk['position_close'].shift(1)*pct_tnk)
tnk_geral = tnk_geral.drop(columns = ['pnl_ex_fx'])

tnk_geral['tnk'] = tnk_geral['tnk']+1
tnk_geral = tnk_geral.fillna(100)
tnk_geral['tnk'] = tnk_geral['tnk'].cumprod()
#tnk_geral2 = Maxdrawdown
tnk_geral2 = tnk_geral.copy()
tnk_geral2['aux'] = np.where(tnk['tnk']>100,tnk_geral['tnk'],100)
tnk_geral2['aux'] = tnk_geral2['aux'].cummax()
tnk_geral2['tnk'] = tnk_geral2['tnk']/tnk_geral2['aux']-1
tnk_geral2 = tnk_geral2.drop(columns = ['aux'])
tnk_geral['tnk'][0] = math.nan
tnk_geral2['tnk'][0] = math.nan
#tnk_geral3 = YtD pnl
tnk_geral3 = tnk_geral.copy()
tnk_geral3['tnk'] = tnk_geral3['tnk']/100-1
##############################################################################

#--------------------------Maxdrawdown TML Quali-------------------------------
tml = quali[quali['book']=='TML']
tml = tml[tml['val_date']>= dt.datetime(2021,8,1)]
tml = tml[['val_date','pnl_ex_fx']].groupby('val_date').sum().reset_index()
tml = tml.merge(position)
tml['tml'] = tml['pnl_ex_fx']/(tml['position_close'].shift(1)*pct_tml)
tml = tml.drop(columns = ['pnl_ex_fx'])

tml['tml'] = tml['tml']+1
tml = tml.fillna(100)
tml['tml'] = tml['tml'].cumprod()

#tml2 = Maxdrawdown
tml2 = tml.copy()
tml2['aux'] = np.where(tml['tml']>100,tml['tml'],100)
tml2['aux'] = tml2['aux'].cummax()
tml2['tml'] = tml2['tml']/tml2['aux']-1
tml2 = tml2.drop(columns = ['aux'])
tml['tml'][0] = math.nan
tml2['tml'][0] = math.nan
#tml3 = YtD pnl
tml3 = tml.copy()
tml3['tml'] = tml3['tml']/100-1
#--------------------------Maxdrawdown TML geral------------------------------
tml_geral = geral[geral['book']=='TML']
tml_geral = tml_geral[tml_geral['val_date']> dt.datetime(2021,8,1)]
tml_geral = tml_geral[['val_date','pnl_ex_fx']].groupby('val_date').sum().reset_index()
tml_geral = tml_geral.merge(position_geral)
tml_geral['tml'] = tml_geral['pnl_ex_fx']/(tml_geral['position_close'].shift(1)*pct_tml)
tml_geral = tml_geral.drop(columns = ['pnl_ex_fx'])

tml_geral['tml'] = tml_geral['tml']+1
tml_geral = tml_geral.fillna(100)
tml_geral['tml'] = tml_geral['tml'].cumprod()
#tml_geral2 = Maxdrawdown
tml_geral2 = tml_geral.copy()
tml_geral2['aux'] = np.where(tml_geral['tml']>100,tml_geral['tml'],100)
tml_geral2['aux'] = tml_geral2['aux'].cummax()
tml_geral2['tml'] = tml_geral2['tml']/tml_geral2['aux']-1
tml_geral2 = tml_geral2.drop(columns = ['aux'])
tml_geral['tml'][0] = math.nan
tml_geral2['tml'][0] = math.nan
#tml_geral3 = YtD pnl
tml_geral3 = tml_geral.copy()
tml_geral3['tml'] = tml_geral3['tml']/100-1
##############################################################################
#------------------------Maxdrawdown TRM Quali---------------------------------
trm = quali[quali['book']=='TRM']
trm = trm[['val_date','pnl_ex_fx']].groupby('val_date').sum().reset_index()
trm['trm'] = trm['pnl_ex_fx']/(position['position_close'].shift(1)*pct_trm)
trm = trm.drop(columns = ['pnl_ex_fx'])

trm['trm'] = trm['trm']+1
trm = trm.fillna(100)
trm['trm'] = trm['trm'].cumprod()
#trm2 = Maxdrawdown
trm2 = trm.copy()
trm2['aux'] = np.where(trm['trm']>100,trm['trm'],100)
trm2['aux'] = trm2['aux'].cummax()
trm2['trm'] = trm2['trm']/trm2['aux']-1
trm2 = trm2.drop(columns = ['aux'])
trm['trm'][0] = math.nan
trm2['trm'][0] = math.nan
#trm3 = YtD pnl
trm3 = trm.copy()
trm3['trm'] = trm3['trm']/100-1
#------------------------Maxdrawdown TRM Geral---------------------------------
trm_geral = geral[geral['book']=='TRM']
trm_geral = trm_geral[['val_date','pnl_ex_fx']].groupby('val_date').sum().reset_index()
trm_geral['trm'] = trm_geral['pnl_ex_fx']/(position_geral['position_close'].shift(1)*pct_trm)
trm_geral = trm_geral.drop(columns = ['pnl_ex_fx'])

trm_geral['trm'] = trm_geral['trm']+1
trm_geral = trm_geral.fillna(100)
trm_geral['trm'] = trm_geral['trm'].cumprod()
#trm_geral2 = Maxdrawdown
trm_geral2 = trm_geral.copy()
trm_geral2['aux'] = np.where(trm_geral['trm']>100,trm_geral['trm'],100)
trm_geral2['aux'] = trm_geral2['aux'].cummax()
trm_geral2['trm'] = trm_geral2['trm']/trm_geral2['aux']-1
trm_geral2 = trm_geral2.drop(columns = ['aux'])
trm_geral['trm'][0] = math.nan
trm_geral2['trm'][0] = math.nan
#trm_geral3 = YtD pnl
trm_geral3 = trm_geral.copy()
trm_geral3['trm'] = trm_geral3['trm']/100-1

trm = quali[quali['book']=='TRM']
trm = trm[['val_date','pnl_ex_fx']].groupby('val_date').sum().reset_index()
trm['trm'] = trm['pnl_ex_fx']/(position['position_close'].shift(1)*pct_trm)
trm = trm.drop(columns = ['pnl_ex_fx'])
#------------------------Maxdrawdown TRM Multi---------------------------------
trm_multi = multi[multi['book']=='TRM']
trm_multi = trm_multi[['val_date','pnl_ex_fx']].groupby('val_date').sum().reset_index()
trm_multi['trm_multi'] = trm_multi['pnl_ex_fx']/(position_multi['position_close'].shift(1)
                                                 )
trm_multi = trm_multi.drop(columns = ['pnl_ex_fx'])
trm_multi['trm_multi'] = trm_multi['trm_multi']+1
trm_multi = trm_multi.fillna(100)
trm_multi['trm_multi'] = trm_multi['trm_multi'].cumprod()
#trm_multi2 = Maxdrawdown
trm_multi2 = trm_multi.copy()
trm_multi2['aux'] = np.where(trm_multi['trm_multi']>100,trm_multi['trm_multi'],100)
trm_multi2['aux'] = trm_multi2['aux'].cummax()
trm_multi2['trm_multi'] = trm_multi2['trm_multi']/trm_multi2['aux']-1
trm_multi2 = trm_multi2.drop(columns = ['aux'])
trm_multi['trm_multi'][0] = math.nan
trm_multi2['trm_multi'][0] = math.nan
#trm_multi3 = YtD pnl
trm_multi3 = trm_multi.copy()
trm_multi3['trm_multi'] = trm_multi3['trm_multi']/100-1

##############################################################################
#------------------------Maxdrawdown TGC Local Quali---------------------------

tgc_local = quali[quali['book']=='TGC LOCAL']
tgc_local = tgc_local[['val_date','pl_total']].groupby('val_date').sum().reset_index()
tgc_local['tgc_local'] = tgc_local['pl_total']/(position['position_close'].shift(1)*pct_tgc_local)
tgc_local = tgc_local.drop(columns = ['pl_total'])
#subtrair IBOV =(tgc+1-ibov)*100
tgc_local = tgc_local.merge(ibov, how = 'outer')
tgc_local  = tgc_local.drop(columns='Ibov Index')
tgc_local['ibov'][0] = 0
tgc_local['ibov'] = tgc_local['ibov'].fillna(method='ffill')
tgc_local['tgc_local'] = tgc_local['tgc_local']+1-tgc_local['ibov']
tgc_local['tgc_local'] = tgc_local['tgc_local'].fillna(100)
tgc_local['tgc_local'] = tgc_local['tgc_local'].cumprod()
tgc_local = tgc_local.drop(columns = 'ibov')
#tgc_local2 = Maxdrawdown
tgc_local2 = tgc_local.copy()
tgc_local2['aux'] = np.where(tgc_local['tgc_local']>100,tgc_local['tgc_local'],100)
tgc_local2['aux'] = tgc_local2['aux'].cummax()
tgc_local2['tgc_local'] = tgc_local2['tgc_local']/tgc_local2['aux']-1
tgc_local2 = tgc_local2.drop(columns = ['aux'])
tgc_local['tgc_local'][0] = math.nan
tgc_local2['tgc_local'][0] = math.nan
#tgc_local3 = YtD PNL
tgc_local3 = tgc_local.copy()
tgc_local3['tgc_local'] = tgc_local3['tgc_local']/100-1
#------------------------Maxdrawdown TGC Local Geral---------------------------
tgc_local_geral = geral[geral['book']=='TGC LOCAL']
tgc_local_geral = tgc_local_geral[['val_date','pl_total']].groupby('val_date').sum().reset_index()
tgc_local_geral['tgc_local'] = tgc_local_geral['pl_total']/(position_geral['position_close'].shift(1)*pct_tgc_local)
tgc_local_geral = tgc_local_geral.drop(columns = ['pl_total'])
#subtrair IBOV =(tgc+1-ibov)*100
tgc_local_geral = tgc_local_geral.merge(ibov, how = 'outer')
tgc_local_geral  = tgc_local_geral.drop(columns='Ibov Index')
tgc_local_geral['ibov'][0] = 0
tgc_local_geral['ibov'] = tgc_local_geral['ibov'].fillna(method='ffill')
tgc_local_geral['tgc_local'] = tgc_local_geral['tgc_local']+1-tgc_local_geral['ibov']
tgc_local_geral['tgc_local'] = tgc_local_geral['tgc_local'].fillna(100)
tgc_local_geral['tgc_local'] = tgc_local_geral['tgc_local'].cumprod()
tgc_local_geral = tgc_local_geral.drop(columns = 'ibov')
#tgc_local_geral2 = Maxdrawdown
tgc_local_geral2 = tgc_local_geral.copy()
tgc_local_geral2['aux'] = np.where(tgc_local_geral['tgc_local']>100,tgc_local_geral['tgc_local'],100)
tgc_local_geral2['aux'] = tgc_local_geral2['aux'].cummax()
tgc_local_geral2['tgc_local'] = tgc_local_geral2['tgc_local']/tgc_local_geral2['aux']-1
tgc_local_geral2 = tgc_local_geral2.drop(columns = ['aux'])
tgc_local_geral['tgc_local'][0] = math.nan
tgc_local_geral2['tgc_local'][0] = math.nan
#tgc_local_geral3 = YtD PNL
tgc_local_geral3 = tgc_local_geral.copy()
tgc_local_geral3['tgc_local'] = tgc_local_geral3['tgc_local']/100-1

###################################################################################################################################################

###################################################################################################################################################
#------------------------Maxdrawdown TGC Off-----------------------------------

tgc_off = macro_int[macro_int['book']=='TGC OFF']
tgc_off = tgc_off[['val_date','pl_total']].groupby('val_date').sum().reset_index()

 
#ajuste tiradentes
pnl_tiradentes = tgc_off[tgc_off['val_date'] == dt.datetime(2021,4,21)]
pnl_tiradentes = pnl_tiradentes.iloc[0]['pl_total']
tgc_off['pl_total'] = np.where(tgc_off['val_date'] == dt.datetime(2021,4,22), tgc_off['pl_total'] + pnl_tiradentes,tgc_off['pl_total']) 
#ajuste carnaval
pnl_carnaval = 15028.1752357808
tgc_off['pl_total'] = np.where(tgc_off['val_date'] == dt.datetime(2021,2,17), tgc_off['pl_total'] + pnl_carnaval,tgc_off['pl_total']) 

#ajuste 7set
pnl_7set = tgc_off[tgc_off['val_date'] == dt.datetime(2021,9,7)]
pnl_7set = pnl_7set.iloc[0]['pl_total']
tgc_off['pl_total'] = np.where(tgc_off['val_date'] == dt.datetime(2021,9,8), tgc_off['pl_total'] + pnl_7set,tgc_off['pl_total'])

tgc_off = tgc_off.merge(position)

tgc_off = tgc_off.merge(prop_quali,how='outer')
tgc_off = tgc_off.sort_values('val_date').reset_index()
tgc_off['Amount'] = tgc_off['Amount'].ffill()
tgc_off = tgc_off.dropna()
tgc_off = tgc_off.drop(columns='index')

#Dividindo pelo dolar
tgc_off = tgc_off.merge(brl,how = 'outer')
tgc_off['brl_curncy'] = tgc_off['brl_curncy'].ffill().bfill()
tgc_off['tgc_off'] = (tgc_off['pl_total']*(tgc_off['Amount'].shift(-1)))/(tgc_off['position_close'].shift(1)/tgc_off['brl_curncy'].shift(1)*pct_tgc_off)

#tgc_off['tgc_off'] = tgc_off['pl_total']/(tgc_off['position_close'].shift(1)*(tgc_off['proporcao'].shift(1)/100)*pct_tgc_off/tgc_off['brl_curncy'].shift(1))
#tgc_off = tgc_off.drop(columns = ['pl_total','brl'])
#tgc_off = tgc_off.drop(columns=['brl_curncy','position_close'])

#subtrair SPX =(tgc+1-spx)

tgc_off = tgc_off.merge(sptr)

tgc_off['sptr'] = tgc_off['sptr Index']/tgc_off['sptr Index'].shift(1)-1

primeiro = (tgc_off.loc[1]['tgc_off']+1)/(tgc_off.loc[1]['sptr']+1)*100 

tgc_off['tgc_off'] = (tgc_off['tgc_off']+1)/(tgc_off['sptr']+1)
tgc_off = tgc_off.replace(to_replace =tgc_off.loc[1]['tgc_off'],value = primeiro)
tgc_off['tgc_off'] = tgc_off['tgc_off'].cumprod()
tgc_off = tgc_off.drop(columns=['sptr','sptr Index'])


#tgc_off2 = Maxdrawdown
tgc_off2 = tgc_off.copy()
tgc_off2['aux'] = np.where(tgc_off['tgc_off']>100,tgc_off['tgc_off'],100)
tgc_off2['aux'] = tgc_off2['aux'].cummax()
tgc_off2['tgc_off'] = tgc_off2['tgc_off']/tgc_off2['aux']-1
tgc_off2 = tgc_off2.drop(columns = ['aux'])
tgc_off['tgc_off'][0] = math.nan
tgc_off2['tgc_off'][0] = math.nan
#tgc_off3 = YtD PNL
tgc_off3 = tgc_off.copy()
tgc_off3['tgc_off'] = tgc_off3['tgc_off']/100-1

'''
#------------------------Maxdrawdown TGC Off-----------------------------------
tgc_off_geral = macro_int[macro_int['book']=='TGC OFF']
tgc_off_geral = tgc_off_geral[['val_date','pl_total']].groupby('val_date').sum().reset_index()
 
#ajuste tiradentes
pnl_tiradentes = tgc_off_geral[tgc_off_geral['val_date'] == dt.datetime(2021,4,21)]
pnl_tiradentes = pnl_tiradentes.iloc[0]['pl_total']
tgc_off_geral['pl_total'] = np.where(tgc_off_geral['val_date'] == dt.datetime(2021,4,22), tgc_off_geral['pl_total'] + pnl_tiradentes,tgc_off_geral['pl_total']) 
#ajuste carnaval
pnl_carnaval = 15028.1752357808
tgc_off_geral['pl_total'] = np.where(tgc_off_geral['val_date'] == dt.datetime(2021,2,17), tgc_off_geral['pl_total'] + pnl_carnaval,tgc_off_geral['pl_total']) 

#ajuste 7set
pnl_7set = tgc_off_geral[tgc_off_geral['val_date'] == dt.datetime(2021,9,7)]
pnl_7set = pnl_7set.iloc[0]['pl_total']
tgc_off_geral['pl_total'] = np.where(tgc_off_geral['val_date'] == dt.datetime(2021,9,8), tgc_off_geral['pl_total'] + pnl_7set,tgc_off_geral['pl_total'])

tgc_off_geral = tgc_off_geral.merge(position_geral)

tgc_off_geral = tgc_off_geral.merge(prop_geral,how='outer')
tgc_off_geral = tgc_off_geral.sort_values('val_date').reset_index()
tgc_off_geral['proporcao'] = tgc_off_geral['proporcao'].ffill()
tgc_off_geral = tgc_off_geral.dropna()
tgc_off_geral = tgc_off_geral.drop(columns='index')

#Dividindo pelo dolar
tgc_off_geral = tgc_off_geral.merge(brl,how = 'outer')
tgc_off_geral['brl_curncy'] = tgc_off_geral['brl_curncy'].ffill().bfill()
tgc_off_geral['tgc_off_geral'] = (tgc_off_geral['pl_total']*(tgc_off_geral['proporcao'].shift(1)))/(tgc_off_geral['position_close'].shift(1)/tgc_off_geral['brl_curncy'].shift(1)*pct_tgc_off)

#tgc_off_geral['tgc_off_geral'] = tgc_off_geral['pl_total']/(tgc_off_geral['position_close'].shift(1)*(tgc_off_geral['proporcao'].shift(1)/100)*pct_tgc_off_geral/tgc_off_geral['brl_curncy'].shift(1))
#tgc_off_geral = tgc_off_geral.drop(columns = ['pl_total','brl'])
#tgc_off_geral = tgc_off_geral.drop(columns=['brl_curncy','position_close'])

#subtrair SPX =(tgc+1-spx)

tgc_off_geral = tgc_off_geral.merge(sptr)
tgc_off_geral['sptr'] = tgc_off_geral['sptr Index']/tgc_off_geral['sptr Index'].shift(1)-1

primeiro = (tgc_off_geral.loc[1]['tgc_off_geral']+1)/(tgc_off_geral.loc[1]['sptr']+1)*100 

tgc_off_geral['tgc_off_geral'] = (tgc_off_geral['tgc_off_geral']+1)/(tgc_off_geral['sptr']+1)
tgc_off_geral = tgc_off_geral.replace(to_replace =tgc_off_geral.loc[1]['tgc_off_geral'],value = primeiro)
tgc_off_geral['tgc_off_geral'] = tgc_off_geral['tgc_off_geral'].cumprod()
tgc_off_geral = tgc_off_geral.drop(columns=['sptr','sptr Index'])


#tgc_off_geral2 = Maxdrawdown
tgc_off_geral2 = tgc_off_geral.copy()
tgc_off_geral2['aux'] = np.where(tgc_off_geral['tgc_off_geral']>100,tgc_off_geral['tgc_off_geral'],100)
tgc_off_geral2['aux'] = tgc_off_geral2['aux'].cummax()
tgc_off_geral2['tgc_off_geral'] = tgc_off_geral2['tgc_off_geral']/tgc_off_geral2['aux']-1
tgc_off_geral2 = tgc_off_geral2.drop(columns = ['aux'])
tgc_off_geral['tgc_off_geral'][0] = math.nan
tgc_off_geral2['tgc_off_geral'][0] = math.nan
#tgc_off_geral3 = YtD PNL
tgc_off_geral3 = tgc_off_geral.copy()
tgc_off_geral3['tgc_off_geral'] = tgc_off_geral3['tgc_off_geral']/100-1

'''