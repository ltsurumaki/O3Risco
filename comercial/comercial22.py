# -*- coding: utf-8 -*-
"""
Created on Fri May 21 11:19:36 2021

@author: ludmilla.tsurumaki
v16: salva no novo BD
v17:inclusao da janela YtD
v18: janela 63du betas
v20: vol janela ytd 
v21: random forest
"""

import os
import glob
import pandas as pd
import math as ma
import datetime as dt
import wget
from pandas.tseries.offsets import BDay
import statsmodels.api as sm
from sqlalchemy import create_engine
from statsmodels import regression
import time
import threading
import numpy as np
import telegram_send
#biblioteca betas
from pyfinance.ols import OLS, RollingOLS, PandasRollingOLS 
#bibliotecas random forest
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from lightgbm import LGBMRegressor
telegram_send.send(messages=['Inicio Concorrencia'])
inicio = time.time()
os.chdir("I:\Riscos\Comercial\Base\Historico\Recentes")

hoje = dt.datetime.today()
ytd = dt.datetime(2021, 1, 1)
data_filtro = hoje - BDay(4)
dia = hoje.day
mes = hoje.month
ano = hoje.year
urls=list()
path = 'I:\Riscos\Comercial\Base\Historico\Recentes'
ano_str = str(ano)
#---------------------------------- funcoes -----------------------------------
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

def to_alchemy(df,tablename,tipo,user):
    param_dic = {
    "host"      : "54.207.157.118",
    "database"  : "o3risco",
    "user"      : user,
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
                  if_exists = tipo,
                  chunksize = 5000,
                  method = 'multi') 
    return 0

#---------------------------- download dos arquivos ---------------------------
if dia in [1,2,3,4]:
    mes_ant = mes - 1
    ano_str = str(ano)
    if mes <10:
        mes_str = str(0) + str(mes)    
    else:
        mes_str = str(mes)
    url = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_'+ ano_str + mes_str +'.csv'
    urls.append(url)
    if mes_ant <10:
        mes_str = str(0) + str(mes_ant)    
    else:
        mes_str = str(mes_ant)
    url_ant = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_'+ ano_str + mes_str +'.csv'
    urls.append(url_ant)
else:
    ano_str = str(ano)
    if mes <10:
        mes_str = str(0) + str(mes)    
    else:
        mes_str = str(mes)
    url = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_'+ ano_str + mes_str +'.csv'
    urls.append(url)
    
path = 'I:\Riscos\Comercial\Base\Historico\Recentes'

for url in urls:
    filename = path + '/' + os.path.basename(url) # get the full path of the file
    if os.path.exists(filename):
        os.remove(filename) # if exist, remove it directly
    try:
        wget.download(url, out=filename) # download it to the specific path.
    except:
        pass
#----------------------------- leitura dos arquivos ---------------------------

os.chdir("I:\Riscos\Comercial\Base\Historico\Recentes")

extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

#combine all files in the list
bd1 = pd.concat([pd.read_csv(f, sep=';') for f in all_filenames ])
#------------------------------------------------------------------------------
flag_ships = pd.read_csv('I:/Riscos/Comercial/flagships.csv')
flag_ships_cnpj = list(flag_ships['CNPJ_FUNDO'])

bd1['Fluxo_DIA'] = bd1['CAPTC_DIA'] - bd1['RESG_DIA']

cadastro1 = pd.read_csv('I:\Riscos\Comercial\cadastrov4.csv')

cadastro1 = cadastro1.drop(['DT_REG', 'DT_CONST', 'DT_CANCEL', 'SIT', 'DT_INI_SIT', 'DT_INI_ATIV', 'DT_FIM_EXERC', \
                'DT_INI_CLASSE','RENTAB_FUNDO','TRIB_LPRAZO','TAXA_PERFM','INF_TAXA_PERFM','TAXA_ADM', 'INF_TAXA_ADM', \
                'DIRETOR','ADMIN','CPF_CNPJ_GESTOR','CNPJ_AUDITOR','AUDITOR','CNPJ_CUSTODIANTE','CUSTODIANTE', \
                'CNPJ_CONTROLADOR','CONTROLADOR'], axis=1)

                           
bd2 = bd1.merge(cadastro1,how='inner',left_on=['CNPJ_FUNDO'],right_on=['CNPJ_FUNDO'])

bd2['flag_ship'] = bd2.apply(lambda x: "S" if x['CNPJ_FUNDO'] in flag_ships_cnpj else "N",axis=1)


bd2 = bd2[bd2['FUNDO_EXCLUSIVO'] == 'N']
bd2 = bd2[bd2['CONDOM'] == 'Aberto']
bd2 = bd2[(bd2['FUNDO_COTAS'] == 'N') | (bd2['flag_ship'] == 'S')]
bd2 = bd2[bd2['PF_PJ_GESTOR'] == 'PJ'].reset_index()

bd2.sort_values(by=['CNPJ_FUNDO','DT_COMPTC'])

bd2.reset_index()
 
ytd = np.busday_count(ytd.date(),hoje.date())

bd2['resg_5'] = bd2.groupby('CNPJ_FUNDO')['RESG_DIA'].transform(lambda x: x.rolling(5, center=False).sum())
bd2['resg_21'] = bd2.groupby('CNPJ_FUNDO')['RESG_DIA'].transform(lambda x: x.rolling(21, center=False).sum())
bd2['resg_63'] = bd2.groupby('CNPJ_FUNDO')['RESG_DIA'].transform(lambda x: x.rolling(63, center=False).sum())
bd2['resg_126'] = bd2.groupby('CNPJ_FUNDO')['RESG_DIA'].transform(lambda x: x.rolling(126, center=False).sum())
bd2['resg_252'] = bd2.groupby('CNPJ_FUNDO')['RESG_DIA'].transform(lambda x: x.rolling(252, center=False).sum())
bd2['resg_ytd'] = bd2.groupby('CNPJ_FUNDO')['RESG_DIA'].transform(lambda x: x.rolling(ytd, center=False).sum())


bd2['apli_5'] = bd2.groupby('CNPJ_FUNDO')['CAPTC_DIA'].transform(lambda x: x.rolling(5, center=False).sum())
bd2['apli_21'] = bd2.groupby('CNPJ_FUNDO')['CAPTC_DIA'].transform(lambda x: x.rolling(21, center=False).sum())
bd2['apli_63'] = bd2.groupby('CNPJ_FUNDO')['CAPTC_DIA'].transform(lambda x: x.rolling(63, center=False).sum())
bd2['apli_126'] = bd2.groupby('CNPJ_FUNDO')['CAPTC_DIA'].transform(lambda x: x.rolling(126, center=False).sum())
bd2['apli_252'] = bd2.groupby('CNPJ_FUNDO')['CAPTC_DIA'].transform(lambda x: x.rolling(252, center=False).sum())
bd2['apli_ytd'] = bd2.groupby('CNPJ_FUNDO')['CAPTC_DIA'].transform(lambda x: x.rolling(ytd, center=False).sum())


bd2['flux_5'] = bd2.groupby('CNPJ_FUNDO')['Fluxo_DIA'].transform(lambda x: x.rolling(5, center=False).sum())
bd2['flux_21'] = bd2.groupby('CNPJ_FUNDO')['Fluxo_DIA'].transform(lambda x: x.rolling(21, center=False).sum())
bd2['flux_63'] = bd2.groupby('CNPJ_FUNDO')['Fluxo_DIA'].transform(lambda x: x.rolling(63, center=False).sum())
bd2['flux_126'] = bd2.groupby('CNPJ_FUNDO')['Fluxo_DIA'].transform(lambda x: x.rolling(126, center=False).sum())
bd2['flux_252'] = bd2.groupby('CNPJ_FUNDO')['Fluxo_DIA'].transform(lambda x: x.rolling(252, center=False).sum())
bd2['flux_ytd'] = bd2.groupby('CNPJ_FUNDO')['Fluxo_DIA'].transform(lambda x: x.rolling(ytd, center=False).sum())

bd2['MMovel'] = bd2.groupby(['CNPJ_FUNDO'])['Fluxo_DIA'].transform(lambda x: x.rolling(42, center=False).mean())

bd2['ret_1'] = bd2.groupby('CNPJ_FUNDO')['VL_QUOTA'].transform(lambda x: x.pct_change(periods=1))
bd2['ret_5'] = bd2.groupby('CNPJ_FUNDO')['VL_QUOTA'].transform(lambda x: x.pct_change(periods=5))
bd2['ret_21'] = bd2.groupby('CNPJ_FUNDO')['VL_QUOTA'].transform(lambda x: x.pct_change(periods=21))
bd2['ret_63'] = bd2.groupby('CNPJ_FUNDO')['VL_QUOTA'].transform(lambda x: x.pct_change(periods=63))
bd2['ret_126'] = bd2.groupby('CNPJ_FUNDO')['VL_QUOTA'].transform(lambda x: x.pct_change(periods=126))
bd2['ret_252'] = bd2.groupby('CNPJ_FUNDO')['VL_QUOTA'].transform(lambda x: x.pct_change(periods=252))
bd2['ret_ytd'] = bd2.groupby('CNPJ_FUNDO')['VL_QUOTA'].transform(lambda x: x.pct_change(periods=ytd))

bd2.sort_values(by=['CNPJ_FUNDO','DT_COMPTC'])

cdi = recupera_bd("SELECT val_date,px_last FROM precos WHERE ticker='BZACCETP Index'")
cdi = cdi.rename(columns={'val_date':'DT_COMPTC','px_last':'BZACCETP Index'})
cdi ['DT_COMPTC'] = cdi['DT_COMPTC'].astype(str)
#cdi = cdi.drop(['Unnamed: 0'], axis=1)

cdi['ret_cdi_1'] = cdi['BZACCETP Index'].pct_change(periods=1)
cdi['ret_cdi_5'] = cdi['BZACCETP Index'].pct_change(periods=5)
cdi['ret_cdi_21'] = cdi['BZACCETP Index'].pct_change(periods=21)
cdi['ret_cdi_63'] = cdi['BZACCETP Index'].pct_change(periods=63)
cdi['ret_cdi_126'] = cdi['BZACCETP Index'].pct_change(periods=126)
cdi['ret_cdi_252'] = cdi['BZACCETP Index'].pct_change(periods=252)
cdi['ret_cdi_ytd'] = cdi['BZACCETP Index'].pct_change(periods=ytd)


bd3 = bd2.merge(cdi,how='left',left_on=['DT_COMPTC'],right_on=['DT_COMPTC'])

bd3['ret_cdimais_1'] = (((1 + bd3['ret_1']) / (1 + bd3['ret_cdi_1'])) - 1)
bd3['ret_cdimais_5'] = (((1 + bd3['ret_5']) / (1 + bd3['ret_cdi_5'])) - 1)
bd3['ret_cdimais_21'] = (((1 + bd3['ret_21']) / (1 + bd3['ret_cdi_21'])) - 1)
bd3['ret_cdimais_63'] = (((1 + bd3['ret_63']) / (1 + bd3['ret_cdi_63'])) - 1)
bd3['ret_cdimais_126'] = (((1 + bd3['ret_126']) / (1 + bd3['ret_cdi_126'])) - 1)
bd3['ret_cdimais_252'] = (((1 + bd3['ret_252']) / (1 + bd3['ret_cdi_252'])) - 1)
bd3['ret_cdimais_ytd'] = (((1 + bd3['ret_ytd']) / (1 + bd3['ret_cdi_ytd'])) - 1)

bd3['std_126'] = bd3.groupby('CNPJ_FUNDO')['ret_cdimais_1'].transform(lambda x: x.rolling(126, center=False).std()) * ma.sqrt(126)

bd3['Vol_126'] = bd3.groupby('CNPJ_FUNDO')['ret_cdimais_1'].transform(lambda x: x.rolling(126, center=False).std()) * ma.sqrt(252)
bd3['Vol_252'] = bd3.groupby('CNPJ_FUNDO')['ret_cdimais_1'].transform(lambda x: x.rolling(252, center=False).std()) * ma.sqrt(252)
bd3['Vol_ytd'] = bd3.groupby('CNPJ_FUNDO')['ret_cdimais_1'].transform(lambda x: x.rolling(ytd, center=False).std()) * ma.sqrt(252)

bd3['sharp_126'] = bd3['ret_cdimais_126'] / bd3['std_126']


bd4 = bd3.drop(['ret_cdi_1','ret_cdi_5', 'ret_cdi_21', 'ret_cdi_63', 'ret_cdi_126', 'ret_cdi_252', 'BZACCETP Index','VL_PATRIM_LIQ_y', \
               'resg_5','resg_21','resg_63','resg_126','resg_252','apli_5','apli_21','apli_63','apli_126','apli_252','ret_5', \
               'ret_21','ret_63','ret_126','ret_252','PF_PJ_GESTOR','CNPJ_ADMIN','DT_PATRIM_LIQ','FUNDO_EXCLUSIVO', \
               'CONDOM','DT_INI_EXERC','VL_TOTAL','FUNDO_COTAS'], axis=1)


bd4['ret_pond_1'] = bd4['VL_PATRIM_LIQ_x'] * bd4['ret_cdimais_1']
bd4['ret_pond_5'] = bd4['VL_PATRIM_LIQ_x'] * bd4['ret_cdimais_5']
bd4['ret_pond_21'] = bd4['VL_PATRIM_LIQ_x'] * bd4['ret_cdimais_21']
bd4['ret_pond_63'] = bd4['VL_PATRIM_LIQ_x'] * bd4['ret_cdimais_63']
bd4['ret_pond_126'] = bd4['VL_PATRIM_LIQ_x'] * bd4['ret_cdimais_126']
bd4['ret_pond_252'] = bd4['VL_PATRIM_LIQ_x'] * bd4['ret_cdimais_252']
bd4['ret_pond_ytd'] = bd4['VL_PATRIM_LIQ_x'] * bd4['ret_cdimais_ytd']

bd4['vol_pond_126'] = bd4['VL_PATRIM_LIQ_x'] * bd4['Vol_126']
bd4['vol_pond_252'] = bd4['VL_PATRIM_LIQ_x'] * bd4['Vol_252']
bd4['vol_pond_ytd'] = bd4['VL_PATRIM_LIQ_x'] * bd4['Vol_ytd']

bd4['sharpe_pond_126'] = bd4['VL_PATRIM_LIQ_x'] * bd4['sharp_126']


bd4['CLASSE'] = bd4.apply(lambda x: "Ref_DI" if 'REFERENCIADO' in x['DENOM_SOCIAL'] \
                                                else x['CLASSE'],axis=1)

bd4['CLASSE'] = bd4.apply(lambda x: "balanceado" if 'BALANCEADO' in x['DENOM_SOCIAL'] \
                                                else x['CLASSE'],axis=1)


bd4['ESG'] = bd4.apply(lambda x: "S" if 'ESG ' in x['DENOM_SOCIAL'] \
                                                else "N",axis=1)
cnpj_esg = ['37.703.644/0001-56',
'38.389.079/0001-67',
'38.306.228/0001-87',
'35.399.404/0001-84',
'35.916.390/0001-29',
'36.874.443/0001-59',
'38.489.470/0001-33',
'37.592.098/0001-23',
'37.985.879/0001-88',
'37.985.829/0001-09',
'37.248.043/0001-09',
'38.440.349/0001-17',
'37.802.092/0001-33',
'38.401.374/0001-91',
'38.428.104/0001-74',
'38.230.040/0001-00',
'35.806.221/0001-36',
'36.352.709/0001-01',
'35.400.868/0001-63',
'35.704.492/0001-80',
'40.054.305/0001-09',
'33.701.828/0001-26',
'34.258.351/0001-19',
'37.108.062/0001-21',
'21.470.644/0001-13',
'37.396.815/0001-41',
'38.173.769/0001-84',
'07.686.680/0001-98',
'37.843.293/0001-89',
'35.956.743/0001-14',
'36.521.818/0001-05',
'12.984.444/0001-98',
'05.775.731/0001-22',
'06.069.957/0001-70',
'08.070.838/0001-63',
'09.087.500/0001-87',
'37.553.529/0001-42',
'04.736.006/0001-82',
'10.638.510/0001-42',
'17.797.426/0001-10',
'39.227.315/0001-01',
'10.991.914/0001-15',
'39.603.028/0001-59',
'39.227.519/0001-42',
'39.723.229/0001-90',
'07.187.751/0001-08',
'10.418.335/0001-88',
'13.083.185/0001-97',
'35.030.809/0001-40',
'36.401.557/0001-81',
'37.887.464/0001-71',
'39.285.822/0001-00',
'39.568.954/0001-30',
'38.597.751/0001-00',
'05.775.731/0001-22',
'06.069.957/0001-70',
'09.441.424/0001-66',
'37.308.317/0001-08',
'26.470.606/0001-84',
'37.895.491/0001-96',
'36.976.907/0001-38']

bd4['ESG'] = np.where(bd4['CNPJ_FUNDO'].isin(cnpj_esg),'S',bd4['ESG'])

bd4['PREV'] = bd4.apply(lambda x: "S" if 'PREV' in x['DENOM_SOCIAL'] else "N",axis=1)

mylist = [' IE' , 'EXTERIOR']
#any(i in x for i in searchfor)

bd4['EXTERIOR'] = bd4.apply(lambda x: "S" if any(i in x['DENOM_SOCIAL'] for i in mylist) \
                                                else "N",axis=1)

mylist = [' CRED' , 'CRÉDITO', ' CRÃ']
bd4['CREDITO'] = bd4.apply(lambda x: "S" if any(i in x['DENOM_SOCIAL'] for i in mylist) \
                                                else "N",axis=1)

bd4 = bd4[(bd4['ret_cdimais_252'] < 1) | (bd4['ret_cdimais_252'].isna())]
bd4 = bd4[(bd4['ret_cdimais_252'] > -1) | (bd4['ret_cdimais_252'].isna())]

bd4 = bd4[(bd4['ret_cdimais_ytd'] < 1) | (bd4['ret_cdimais_ytd'].isna())]
bd4 = bd4[(bd4['ret_cdimais_ytd'] > -1) | (bd4['ret_cdimais_ytd'].isna())]

bd4 = bd4[(bd4['ret_cdimais_126'] < 1) | (bd4['ret_cdimais_126'].isna())]
bd4 = bd4[(bd4['ret_cdimais_126'] > -1) | (bd4['ret_cdimais_126'].isna())]

bd4 = bd4[(bd4['ret_cdimais_63'] < 1) | (bd4['ret_cdimais_63'].isna())]
bd4 = bd4[(bd4['ret_cdimais_63'] > -1) | (bd4['ret_cdimais_63'].isna())]

bd4 = bd4[(bd4['ret_cdimais_21'] < 1) | (bd4['ret_cdimais_21'].isna())]
bd4 = bd4[(bd4['ret_cdimais_21'] > -1) | (bd4['ret_cdimais_21'].isna())]

bd4 = bd4[(bd4['ret_cdimais_5'] < 1) | (bd4['ret_cdimais_5'].isna())]
bd4 = bd4[(bd4['ret_cdimais_5'] > -1) | (bd4['ret_cdimais_5'].isna())]

bd4 = bd4[(bd4['ret_cdimais_1'] < 0.3) | (bd4['ret_cdimais_1'].isna())]
bd4 = bd4[(bd4['ret_cdimais_1'] > -0.3) | (bd4['ret_cdimais_1'].isna())]
bd_esg = bd4[bd4['ESG'] == 'S']

bd4 = bd4[bd4['ESG'] == 'N']
bd5 = bd4[bd4['VL_PATRIM_LIQ_x'] > 10000000]
bd5 = bd5.append(bd_esg)

################################## matriz correl##############################
spx = recupera_bd("SELECT * FROM precos where ticker = 'SPX Index'")
spx['ret_cdimais_1'] = spx['px_last']/spx['px_last'].shift(1)-1 
spx['nome_resumido']='S&P'
spx['CNPJ_FUNDO']='S&P'
spx = spx.rename(columns={'val_date':'DT_COMPTC'})
spx = spx[['DT_COMPTC','nome_resumido','ret_cdimais_1']].sort_values('DT_COMPTC',ascending=False).head(300)

bd_flag_ship = bd5[bd5['flag_ship']=='S']
bd_flag_ship2 = bd_flag_ship.merge(flag_ships,on='CNPJ_FUNDO')
bd_flag_ship2 = bd_flag_ship2 [['DT_COMPTC','CNPJ_FUNDO','DENOM_SOCIAL_y','nome_resumido','tipo_concorrente','ret_cdimais_1']]
bd_flag_ship2['DT_COMPTC'] = bd_flag_ship2['DT_COMPTC'].astype('datetime64[ns]')
bd_flag_ship3 = bd_flag_ship2 [['DT_COMPTC','nome_resumido','ret_cdimais_1']]

mercado = bd_flag_ship3.groupby("DT_COMPTC")["DT_COMPTC","ret_cdimais_1"].transform('mean').reset_index()
mercado['nome_resumido'] = 'mercado'
mercado= mercado[['DT_COMPTC','nome_resumido','ret_cdimais_1']]
bd_flag_ship3 = bd_flag_ship3.append(mercado)
bd_flag_ship3['DT_COMPTC'] = bd_flag_ship3['DT_COMPTC'].dt.date
bd_flag_ship3 = bd_flag_ship3.append(spx)

bd_flag_ship4 = bd_flag_ship3.pivot_table(values='ret_cdimais_1', index = bd_flag_ship3.DT_COMPTC,columns= 'nome_resumido',aggfunc='first').reset_index()


corr_21 = bd_flag_ship4.sort_values('DT_COMPTC',ascending=False).head(21).corr()
#corr_21 = corr_21.where(np.triu(np.ones(corr_21 .shape)).astype(np.bool))
corr_21.index.name = None
corr_21 = corr_21.stack().reset_index()

corr_63 = bd_flag_ship4.sort_values('DT_COMPTC',ascending=False).head(63).corr()
#corr_63 = corr_63.where(np.triu(np.ones(corr_63 .shape)).astype(np.bool))
corr_63.index.name = None
corr_63 = corr_63.stack().reset_index()

corr_126 = bd_flag_ship4.sort_values('DT_COMPTC',ascending=False).head(126).corr()
#corr_126 = corr_126.where(np.triu(np.ones(corr_126 .shape)).astype(np.bool))
corr_126.index.name = None
corr_126 = corr_126.stack().reset_index()

corr_252 = bd_flag_ship4.sort_values('DT_COMPTC',ascending=False).head(252).corr()
#corr_252 = corr_252.where(np.triu(np.ones(corr_252 .shape)).astype(np.bool))
corr_252.index.name = None
corr_252 = corr_252.stack().reset_index()


corr_21 ['janela'] = '21du'
corr_63 ['janela'] = '63du'
corr_126 ['janela'] = '126du'
corr_252 ['janela'] = '252du'

matriz_final = corr_21.append([corr_63,corr_126,corr_252])
matriz_final = matriz_final.rename(columns={'level_0':'nome_resumido2',0:'correl'})

flag_ships = flag_ships.append(pd.DataFrame([['mercado','mercado','mercado','mercado','mercado',27]],
                               columns=['CNPJ_FUNDO','DENOM_SOCIAL','GESTOR','nome_resumido','tipo_concorrente','sort']))
flag_ships = flag_ships.append(pd.DataFrame([['S&P','S&P','S&P','S&P','S&P',28]],
                               columns=['CNPJ_FUNDO','DENOM_SOCIAL','GESTOR','nome_resumido','tipo_concorrente','sort']))

matriz_final= matriz_final.merge(flag_ships,left_on = 'nome_resumido2',right_on = 'nome_resumido').drop_duplicates()
matriz_final = matriz_final.drop(columns=['nome_resumido_y'])
matriz_final = matriz_final.rename(columns={'nome_resumido_x':'nome_resumido'})
data_base = bd_flag_ship4['DT_COMPTC'].sort_values().tail(1).reset_index()
data_base = data_base.iloc[0]['DT_COMPTC']
#data_base  = data_base.date()
matriz_final ['data_base'] = data_base  


##############################################################################
#----------------------------------Betas--------------------------------------
bd6 = bd5[bd5['CLASSE'] == 'Fundo Multimercado']

#bd5 = bd5.drop(['index','level_0','sharp_126','Vol_126','ret_cdimais_1','ret_cdimais_5','ret_cdimais_21','ret_cdimais_63','ret_cdimais_126','ret_cdimais_252'], axis=1)
bd5 = bd5.drop(['index','sharp_126','Vol_126','ret_cdimais_1','ret_cdimais_5','ret_cdimais_21','ret_cdimais_63','ret_cdimais_126','ret_cdimais_252','ret_cdimais_ytd'], axis=1)
bd5.reset_index()

#-------------------------- leitura dos benchmarks ---------------------------

ibov = recupera_bd("SELECT val_date,px_last FROM precos WHERE ticker='IBOV Index'")
ibov = ibov.rename(columns={'px_last':'IBOV Index'})
ibov['ret_ibov_1'] = ibov['IBOV Index']/ibov['IBOV Index'].shift(1)-1 

sptr = recupera_bd("SELECT val_date,px_last FROM precos WHERE ticker='SPTR500N Index'")
sptr= sptr.rename(columns={'px_last':'SPTR500N Index'})
sptr['ret_sptr_1'] = sptr['SPTR500N Index']/sptr['SPTR500N Index'].shift(1)-1 

dolar_real = recupera_bd("SELECT val_date,px_last FROM precos WHERE ticker='USDBRL REGN Curncy'")
dolar_real= dolar_real.rename(columns={'px_last':'USDBRL REGN Curncy'})
dolar_real['ret_dolar_real_1'] = dolar_real['USDBRL REGN Curncy']/dolar_real['USDBRL REGN Curncy'].shift(1)-1 

dxy = recupera_bd("SELECT val_date,px_last FROM precos WHERE ticker='DXY Curncy'")
dxy= dxy.rename(columns={'px_last':'DXY Curncy'})
dxy['ret_dxy_1'] = dxy['DXY Curncy']/dxy['DXY Curncy'].shift(1)-1 

ten_yr_treasury = recupera_bd("SELECT val_date,px_last FROM precos WHERE ticker='USGG10YR Index'")
ten_yr_treasury = ten_yr_treasury.rename(columns={'px_last':'USGG10YR Index'})
ten_yr_treasury['ret_10yr_treas_1'] = (ten_yr_treasury['USGG10YR Index']-ten_yr_treasury['USGG10YR Index'].shift(1))*10/100

pre_3_anos =recupera_bd("SELECT val_date,px_last FROM precos WHERE ticker='BZAD3Y Index'")
pre_3_anos = pre_3_anos.rename(columns={'px_last':'BZAD3Y Index'})
pre_3_anos['ret_pre_3a_1'] = (pre_3_anos['BZAD3Y Index']-pre_3_anos['BZAD3Y Index'].shift(1))*3/100

mxcn =recupera_bd("SELECT val_date,px_last FROM precos WHERE ticker='MXCN Index'")
mxcn = mxcn.rename(columns={'px_last':'MXCN Index'})
mxcn['ret_mxcn_1'] = (mxcn['MXCN Index']/mxcn['MXCN Index'].shift(1))-1


benchmarks = ibov.merge(sptr,how = 'outer').merge(dolar_real,how = 'outer').merge(dxy,how='outer').merge(ten_yr_treasury,how='outer').merge(pre_3_anos,how='outer').merge(mxcn,how='outer')
benchmarks['val_date'] = pd.to_datetime(benchmarks['val_date'])
benchmarks = benchmarks[['val_date','ret_ibov_1','ret_sptr_1','ret_dolar_real_1','ret_dxy_1','ret_10yr_treas_1','ret_pre_3a_1','ret_mxcn_1']]
inicio_base =  dt.datetime(2017,1,1)
benchmarks = benchmarks[benchmarks['val_date']>inicio_base] 
benchmarks['val_date'] = (benchmarks['val_date'].dt.date).apply(str)
benchmarks = benchmarks.rename(columns={'val_date':'DT_COMPTC'})
benchmarks = benchmarks.fillna(0)

#-----------------------------------------------------------------------------

betas = benchmarks.merge(bd6, how='outer')
betas = betas[['DT_COMPTC','CNPJ_FUNDO','VL_PATRIM_LIQ_x','ret_ibov_1','ret_sptr_1','ret_dolar_real_1','ret_dxy_1','ret_10yr_treas_1','ret_pre_3a_1','ret_mxcn_1','ret_cdimais_1']]
betas = betas.sort_values(['CNPJ_FUNDO','DT_COMPTC'])
betas = betas.set_index('DT_COMPTC')
    
fundos = bd6['CNPJ_FUNDO'].drop_duplicates()
betas_final = pd.DataFrame()
for i in fundos:
    betas3 = betas[betas['CNPJ_FUNDO']==i]
    
    y = betas3[['ret_cdimais_1']]
    x = betas3[['ret_ibov_1','ret_sptr_1','ret_dolar_real_1','ret_dxy_1','ret_10yr_treas_1','ret_pre_3a_1','ret_mxcn_1']]
    window = 21 
    try:
        model = PandasRollingOLS(y=y, x=x, window=window) 
        modelo = model.beta.reset_index()
        r_quad = model.rsq.reset_index()
        fim = modelo.merge(r_quad)
        fim['CNPJ_FUNDO'] = i
        betas_final = betas_final.append(fim)
    except ValueError:
        pass
#betas_final.to_csv(r'I:\Riscos\Ludmilla\semple.csv')
ajuste = bd6[['CNPJ_FUNDO','VL_PATRIM_LIQ_x','DENOM_SOCIAL','GESTOR','PREV','CREDITO','EXTERIOR','flag_ship']].drop_duplicates('CNPJ_FUNDO')
betas_final2 = betas_final.merge(ajuste)
betas_final2['beta_ibov_21'] = betas_final2['ret_ibov_1']*betas_final2['VL_PATRIM_LIQ_x']   
betas_final2['beta_sptr_21'] = betas_final2['ret_sptr_1']*betas_final2['VL_PATRIM_LIQ_x']   
betas_final2['beta_dolar_real_21'] = betas_final2['ret_dolar_real_1']*betas_final2['VL_PATRIM_LIQ_x']   
betas_final2['beta_dxy_21'] = betas_final2['ret_dxy_1']*betas_final2['VL_PATRIM_LIQ_x']   
betas_final2['beta_10yr_treas_21'] = betas_final2['ret_10yr_treas_1']*betas_final2['VL_PATRIM_LIQ_x']   
betas_final2['beta_pre_3a_21'] = betas_final2['ret_pre_3a_1']*betas_final2['VL_PATRIM_LIQ_x']   
betas_final2['beta_mxcn_21'] = betas_final2['ret_mxcn_1']*betas_final2['VL_PATRIM_LIQ_x']   

betas_final2 = betas_final2.drop(['ret_ibov_1','ret_sptr_1','ret_dolar_real_1','ret_dxy_1','ret_10yr_treas_1','ret_pre_3a_1','ret_mxcn_1'], axis=1)
betas_final2 ['flag'] = '21du'

betas_final_63 = pd.DataFrame()
for i in fundos:
    betas3 = betas[betas['CNPJ_FUNDO']==i]
    
    y = betas3[['ret_cdimais_1']]
    x = betas3[['ret_ibov_1','ret_sptr_1','ret_dolar_real_1','ret_dxy_1','ret_10yr_treas_1','ret_pre_3a_1','ret_mxcn_1']]
    window = 63 
    try:
        model = PandasRollingOLS(y=y, x=x, window=window) 
        modelo = model.beta.reset_index()
        r_quad = model.rsq.reset_index()
        fim = modelo.merge(r_quad)
        fim['CNPJ_FUNDO'] = i
        betas_final_63 = betas_final_63.append(fim)
    except ValueError:
        pass
    
#betas_final_63.to_csv(r'I:\Riscos\Ludmilla\semple.csv')
ajuste = bd6[['CNPJ_FUNDO','VL_PATRIM_LIQ_x','DENOM_SOCIAL','GESTOR','PREV','CREDITO','EXTERIOR','flag_ship']].drop_duplicates('CNPJ_FUNDO')
betas_final_632 = betas_final_63.merge(ajuste)
betas_final_632['beta_ibov_63'] = betas_final_632['ret_ibov_1']*betas_final_632['VL_PATRIM_LIQ_x']   
betas_final_632['beta_sptr_63'] = betas_final_632['ret_sptr_1']*betas_final_632['VL_PATRIM_LIQ_x']   
betas_final_632['beta_dolar_real_63'] = betas_final_632['ret_dolar_real_1']*betas_final_632['VL_PATRIM_LIQ_x']   
betas_final_632['beta_dxy_63'] = betas_final_632['ret_dxy_1']*betas_final_632['VL_PATRIM_LIQ_x']   
betas_final_632['beta_10yr_treas_63'] = betas_final_632['ret_10yr_treas_1']*betas_final_632['VL_PATRIM_LIQ_x']   
betas_final_632['beta_pre_3a_63'] = betas_final_632['ret_pre_3a_1']*betas_final_632['VL_PATRIM_LIQ_x']   
betas_final_632['beta_mxcn_63'] = betas_final_632['ret_mxcn_1']*betas_final_632['VL_PATRIM_LIQ_x']   
betas_final_632 = betas_final_632.drop(['ret_ibov_1','ret_sptr_1','ret_dolar_real_1','ret_dxy_1','ret_10yr_treas_1','ret_pre_3a_1','ret_mxcn_1'], axis=1)
betas_final_632 ['flag'] ='63du'

betas_21e63 = betas_final2.append(betas_final_632)
##############################################################################
#------------------------------Random Forest-----------------------------------
bd_random= bd5.copy()

bd_random.sort_values(by=['CNPJ_FUNDO','DT_COMPTC'])

bd_random['Fluxo_Def_5'] = bd_random.groupby('CNPJ_FUNDO')['flux_5'].shift(5)
bd_random['Fluxo_Def_21'] = bd_random.groupby('CNPJ_FUNDO')['flux_21'].shift(21)
bd_random['Fluxo_Def_63'] = bd_random.groupby('CNPJ_FUNDO')['flux_63'].shift(63)
bd_random['Fluxo_Def_126'] = bd_random.groupby('CNPJ_FUNDO')['flux_126'].shift(126)


def trata_classe(valor):
    if valor == 'Fundo de Renda Fixa':
        return 1
    elif valor == 'Fundo Multimercado':
        return 2
    elif valor == 'Ref_DI':
        return 3
    elif valor == 'Fundo Cambial':
        return 4
    elif valor == 'Fundo de AÃƒÂ§ÃƒÂµes':
        return 5
    elif valor == 'balaceado':
        return 6

bd_random['CLASSE'] = bd_random['CLASSE'].apply(trata_classe)

#Treinamento 63
bd_random_63 = bd_random.drop(['CNPJ_FUNDO','DT_COMPTC','VL_QUOTA','CAPTC_DIA','RESG_DIA','TP_FUNDO','Fluxo_DIA','DENOM_SOCIAL',
                                'INVEST_QUALIF','GESTOR','flag_ship','flux_5','flux_21','flux_126','flux_252','MMovel','ret_1',
                                'std_126','Vol_252','ret_pond_1','ret_pond_5','ret_pond_21','ret_pond_63','ret_pond_126',
                                'ret_pond_252','sharpe_pond_126','ESG','PREV','EXTERIOR','CREDITO',
                                'vol_pond_126','vol_pond_252'], axis=1)

bd_random_63 = bd_random_63.dropna(axis=0)

train_63, test_63 = train_test_split(bd_random_63, test_size = 0.2)

x_63 = bd_random_63.drop(['flux_63'], axis=1)
y_63 = bd_random_63['flux_63']

np.where(x_63.values >= np.finfo(np.float64).max)

x1_63 = train_63.drop(['flux_63'], axis=1)
y1_63 = train_63['flux_63']

x2_63 = test_63.drop(['flux_63'], axis=1)
y2_63 = test_63['flux_63']


modelo_63 = LGBMRegressor(max_depth=49)
#modelo_21 = RandomForestRegressor(max_depth=10)

modelo_63.fit(x1_63, y1_63)
result1_63 = modelo_63.score(x2_63, y2_63)

print(result1_63)

#treinamento 126
bd_random_126 = bd_random.drop(['CNPJ_FUNDO','DT_COMPTC','VL_QUOTA','CAPTC_DIA','RESG_DIA','TP_FUNDO','Fluxo_DIA','DENOM_SOCIAL',
                                 'INVEST_QUALIF','GESTOR','flag_ship','flux_5','flux_21','flux_63','flux_252','MMovel','ret_1',
                                 'std_126','Vol_252','ret_pond_1','ret_pond_5','ret_pond_21','ret_pond_63','ret_pond_126',
                                 'ret_pond_252','sharpe_pond_126','ESG','PREV','EXTERIOR','CREDITO',
                                 'vol_pond_126','vol_pond_252'], axis=1)



bd_random_126 = bd_random_126.dropna(axis=0)

train_126, test_126 = train_test_split(bd_random_126, test_size = 0.2)

x_126 = bd_random_126.drop(['flux_126'], axis=1)
y_126 = bd_random_126['flux_126']

np.where(x_126.values >= np.finfo(np.float64).max)

x1_126 = train_126.drop(['flux_126'], axis=1)
y1_126 = train_126['flux_126']

x2_126 = test_126.drop(['flux_126'], axis=1)
y2_126 = test_126['flux_126']

modelo_126 = LGBMRegressor(max_depth=49)

modelo_126.fit(x1_126, y1_126)
result1_126 = modelo_126.score(x2_126, y2_126)

print(result1_126)

#resultado 63
bd6_63 = bd_random.drop(['VL_QUOTA','CAPTC_DIA','RESG_DIA','TP_FUNDO','Fluxo_DIA','DENOM_SOCIAL',
                         'INVEST_QUALIF','GESTOR','flag_ship','flux_5','flux_21','flux_126','flux_252','MMovel','ret_1',
                         'std_126','Vol_252','ret_pond_1','ret_pond_5','ret_pond_21','ret_pond_63','ret_pond_126',
                         'ret_pond_252','sharpe_pond_126','ESG','PREV','EXTERIOR','CREDITO',
                         'vol_pond_126','vol_pond_252'], axis=1)


bd7_63 = bd6_63.groupby('CNPJ_FUNDO')['DT_COMPTC'].max().reset_index()

bd7_63 = bd7_63.merge(bd6_63,how='left',left_on=['CNPJ_FUNDO','DT_COMPTC'],right_on=['CNPJ_FUNDO','DT_COMPTC'])

def modelo1(fundo):
    df = bd7_63[bd7_63['CNPJ_FUNDO'] == fundo]
    df = df.drop(['flux_63','CNPJ_FUNDO','DT_COMPTC'], axis=1)
    return (modelo_63.predict(df.values.tolist()))

resultado= pd.DataFrame()

data = bd7_63['DT_COMPTC'].drop_duplicates()

for i in bd7_63['CNPJ_FUNDO'].drop_duplicates():
    resultado1 = pd.DataFrame(modelo1(i))
    resultado1['CNPJ_FUNDO'] = i
    resultado = resultado.append(resultado1)
    resultado['DT_COMPTC'] = data

resultado_63 = resultado.rename(columns={0:'modelo_63'})

#resultado 126
bd6_126 = bd_random.drop(['VL_QUOTA','CAPTC_DIA','RESG_DIA','TP_FUNDO','Fluxo_DIA','DENOM_SOCIAL',
                          'INVEST_QUALIF','GESTOR','flag_ship','flux_5','flux_21','flux_63','flux_252','MMovel','ret_1',
                          'std_126','Vol_252','ret_pond_1','ret_pond_5','ret_pond_21','ret_pond_63','ret_pond_126',
                          'ret_pond_252','sharpe_pond_126','ESG','PREV','EXTERIOR','CREDITO',
                          'vol_pond_126','vol_pond_252'], axis=1)


bd7_126 = bd6_126.groupby('CNPJ_FUNDO')['DT_COMPTC'].max().reset_index()

bd7_126 = bd7_126.merge(bd6_126,how='left',left_on=['CNPJ_FUNDO','DT_COMPTC'],right_on=['CNPJ_FUNDO','DT_COMPTC'])

def modelo1(fundo):
    df = bd7_126[bd7_126['CNPJ_FUNDO'] == fundo]
    df = df.drop(['flux_126','CNPJ_FUNDO','DT_COMPTC'], axis=1)
    return (modelo_126.predict(df.values.tolist()))

resultado= pd.DataFrame()

data = bd7_126['DT_COMPTC'].drop_duplicates()

for i in bd7_126['CNPJ_FUNDO'].drop_duplicates():
    resultado1 = pd.DataFrame(modelo1(i))
    resultado1['CNPJ_FUNDO'] = i
    resultado = resultado.append(resultado1)
    resultado['DT_COMPTC'] = data

resultado_126 = resultado.rename(columns={0:'modelo_126'})

#ajustes nos resultados
resultado1 = resultado_63.merge(resultado_126,how='left',left_on=['CNPJ_FUNDO'],right_on=['CNPJ_FUNDO'])
resultado1 = resultado1.drop(['DT_COMPTC_y'], axis=1)
#scores
hoje2 = hoje.date()
scores = pd.DataFrame([[result1_126, result1_63, hoje2]], columns = ['126_du','63_du','val_date'])
################################### Peers ####################################
bd_peers = bd5.copy()
bd_peers['DT_COMPTC'] = bd_peers['DT_COMPTC'].astype('datetime64[ns]')
bd_peers = bd_peers.sort_values('DT_COMPTC')
bd_peers = bd_peers[bd_peers['DT_COMPTC']==bd_peers.iloc[-1]['DT_COMPTC']]
bd_peers['DT_COMPTC'] = bd_peers['DT_COMPTC'].dt.strftime('%Y/%m/%d')
'''
##############################################################################
#------------------------------Subindo no BD-----------------------------------
'''
inicio = time.time()

threading.Thread(target=to_alchemy(betas_21e63,'betas_concorrencia','replace','ltsurumaki')).start()
threading.Thread(target=to_alchemy(bd5,'concorrencia','replace','rloures')).start()
threading.Thread(target=to_alchemy(resultado1,'modelo_concorrencia','append','rloures')).start()
threading.Thread(target=to_alchemy(scores,'scores_modelo_concorrencia','append','rloures')).start()
threading.Thread(target=to_alchemy(matriz_final,'concorrencia_matriz_correl','replace','ltsurumaki')).start()
to_alchemy(bd_peers,'concorrencia_peers','replace','rloures')

fim = time.time()
tempo = round((fim - inicio)/60, 1)
mens = "Concorrencia subiu em " + str(tempo) +'m!!'
telegram_send.send(messages=[mens])

