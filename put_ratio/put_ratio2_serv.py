# -*- coding: utf-8 -*-
"""
Created on Thu Jul 29 15:52:43 2021

@author: pedro.silva
"""

import pandas as pd
import numpy as np
import os
from scipy.stats import norm
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import datetime as dt
from pandas.tseries.offsets import BDay
import telegram_send

param_dic = {
   "host"      : "localhost",
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
#-------------------------------- Datas --------------------------------------
hoje =  dt.date.today()
ontem = hoje - BDay(1)
ontem = ontem.date()
hoje_str = hoje.strftime("%Y-%m-%d")
hoje_str2 = hoje.strftime("%Y%m%d")

#cambio
dolar = pd.read_sql('cambio', con=engine)
dolar = dolar.iloc[0][0]

#------------------------------------ RPM -------------------------------------
def export_txt(rpm_exe, t, o, p, data, nome):
        '''
        Exporta o report do RPM em txt e salva em uma pasta, retorna o caminho do arquivo salvo
            
            Parameters:
                rpm_exe: caminho completo até o executável do RPM
                t: caminho completo do arquivo de template do RPM
                o: pasta de saida do arquivo a ser gerado (o nome do arquivo sera igual ao do template + tradingdesk + data + .txt)
                p: parâmetros do template
                tradingdesk: Criar a coluna Trading desk? Se Falso não criar, caso contrário informe o códio da tradringdesk
                data: Criar uma coluna de data? Se Falso não criar, caso contrário informe a data no formato YY YY-MM-DD  
        '''
        
        o = o + t.split('/')[-1].replace('.snx', '')
        o = o + nome
        o = o + data.replace('-', '') + '.txt'
        
        cmd_line_template = r'{} t:"{}" o:"{}" p:"{}"'.format(rpm_exe, t, o, p)
        os.system(cmd_line_template)     
        
        return o

fundos_dic = {'rgq':'O3 RETORNO GLOBAL QUAIFICADO MASTER FIM','rgm':'O3 RETORNO GLOBAL MASTER FIM','glf':'O3 GLOBAL FUND','hdf':'TANTALO FIM CP IE'}

t_mif = 'C:/Users/o3capital/Documents/Ludmilla/put_ratio/options_price.snx' 
rpm_exe = 'C:/Users/o3capital/AppData/Local/Apps/2.0/ZQDYDTA0.V2R/T63G9M52.XPV/repo..tion_46f8e9fc3ea7ba85_0000.0000_3e79f2123ac79929/ReportsPortfolioManager5.exe' 
o = 'C:/Users/o3capital/Documents/Ludmilla/put_ratio/historico/'

p_mif = 'valdate='+ hoje_str + ',BookReportLevel=10,ReportParent=Main,TradingDesk=O3 MACRO INTERNATIONAL FUND'

################################ RPM #####################################

mif = export_txt(rpm_exe, t_mif, o, p_mif, hoje_str,'_nivel10_')

for i in fundos_dic:
    t_nav = 'C:/Users/o3capital/Documents/Ludmilla/put_ratio/nav_'+ i +'.snx'
    p = 'valdate='+ hoje_str + ',BookReportLevel=1,ReportParent=Main,TradingDesk=' + fundos_dic[i]
    export_txt(rpm_exe, t_nav, o, p, hoje_str,'_nivel1_')
PL = pd.DataFrame()
for i in fundos_dic:
    df = pd.read_csv('C:/Users/o3capital/Documents/Ludmilla/put_ratio/historico/nav_'+ i +'_nivel1_'+ hoje_str2 +'.txt', sep='\t')
    df = df[['ValDate','NAV']]
    df ['fundo'] = i
    df = df.head(1)
    PL = PL.append(df)
    
PL['NAV'] = np.where(PL['fundo'] == 'glf',PL['NAV']*dolar,PL['NAV'])
PL_tot = PL['NAV'].sum()

products = pd.read_csv('C:/Users/o3capital/Documents/Ludmilla/put_ratio/historico/options_price_nivel10_' + hoje_str2 +'.txt', sep='\t')
products = products[['Product','YestPrice','Amount']]
products = products[products['Product'].str.contains('SPX ')]
products['YestPrice'] = products['YestPrice'].astype(float)
products = products.rename(columns={'Product':'option'})
#------------------------------------- Dados ----------------------------------

spx_ontem = pd.read_sql('spx', con=engine)
spx_ontem = spx_ontem.sort_values('val_date')
spx_ontem = spx_ontem ['SPX Index'].iloc[-1]

ff_ontem = pd.read_sql('ff', con=engine)
ff_ontem = ff_ontem.sort_values('val_date')
ff_ontem = ff_ontem ['FF1 Comdty'].iloc[-1]

risk_free = (100-ff_ontem)/100
#put_ratio1
options_list = pd.read_excel('C:/Users/o3capital/Documents/Ludmilla/put_ratio\options_list.xls')
options_list ['end'] = options_list ['option'].str[7:15]
options_list ['strike'] = options_list ['option'].str[-4:]
options_list ['tipo'] = options_list ['option'].str[16:17]
options_list ['end'] = pd.to_datetime(options_list ['end']).dt.date
options_list ['strike'] = options_list ['strike'].astype(float)
options_list ['tempo'] = options_list ['end'] - hoje
options_list ['tempo'] = options_list ['tempo'] /np.timedelta64(1,'D')/365

#preco e amount ontem
options_list = options_list.merge(products)
options_list['quantidade'] = -options_list['Amount']/options_list.iloc[0]['Amount']

options_list = options_list.to_dict('index')
n_options = len(options_list)

#putratio2
options_list2 = pd.read_excel('C:/Users/o3capital/Documents/Ludmilla/put_ratio\options_list2.xls')
options_list2 ['end'] = options_list2 ['option'].str[7:15]
options_list2 ['strike'] = options_list2 ['option'].str[-4:]
options_list2 ['tipo'] = options_list2 ['option'].str[16:17]
options_list2 ['end'] = pd.to_datetime(options_list2 ['end']).dt.date
options_list2 ['strike'] = options_list2 ['strike'].astype(float)
options_list2 ['tempo'] = options_list2 ['end'] - hoje
options_list2 ['tempo'] = options_list2 ['tempo'] /np.timedelta64(1,'D')/365

#preco e amount ontem
options_list2 = options_list2.merge(products)
options_list2['quantidade'] = -options_list2['Amount']/options_list2.iloc[0]['Amount']

options_list2 = options_list2.to_dict('index')
n_options2 = len(options_list2)

'''

Option price -> price per share
S -> price per share (spot price)
K -> price per share (strike price)
r -> per year (risk free rate)
t -> Years (time)
sigma -> per sqrt(year)

'''

class opt:
    def __init__(self,contract_type='U',Spot=0, Strike=0, r=0, t=0,p=0,sigma=0):
        self.type = contract_type
        self.premium = p
        self.vol = sigma
        self.spot = Spot
        self.strike = Strike
        self.rate = r
        self.time = t
                
        if (self.vol == 0 ) and (contract_type != 'U' ):
            self.vol = get_vol(self)
        if (self.premium == 0) and (contract_type != 'U' ):
            self.premium = price(self)
        
    # def upload_d(self):
    #     self.d1,self.d2 = d(self.sigma, self.spot, self.strike, self.rate, self.time)

    
    
def get_vol(self):
    '''
    Recebe um objeto do tipo opt e retorna a vol dele se todas as outras informações forem dadas
    Calcula pelo metodo de Newton Raphson
    '''
    tol = 1e-3
    
    count = 0
    max_iter = 1000
    epsilon = 1
    
    vol = 0.15# initial guess
    
    self.vol = vol
    
    while epsilon>tol:
        count = count + 1
        if count>max_iter:
            break
        
        orig_vol = vol    
        #d1,d2 = d(self)
        
        f_value = price(self)-self.premium
        v = vega(self)
        vol = -f_value/ v + vol
        
        self.vol = vol
        
        epsilon = abs((vol-orig_vol)/orig_vol)   
    
    return vol
        
def price(self):
        '''
        Recebe um objeto do tipo opt e retorna o preço dele se todas as outras informações forem dadas
        '''
        
        if (self.type.lower() == "p"):
            return put_price(self)
        elif (self.type.lower() == "c"):
            return call_price(self)   
        elif (self.type.lower() == 'u'):
            return self.premium
            
def d(self):
    sigma = self.vol
    S = self.spot
    K = self.strike
    r = self.rate
    t = self.time
    d1 = 1 / (sigma * np.sqrt(t)) * ( np.log(S/K) + (r + sigma**2/2) * t)
    d2 = d1 - sigma * np.sqrt(t)
    return d1, d2

def call_price(self):
    S = self.spot
    K = self.strike
    r = self.rate
    t = self.time
    d1,d2 = d(self)
    
    C = norm.cdf(d1) * S - norm.cdf(d2) * K * np.exp(-r * t)
    return C

def put_price(self):
    S = self.spot
    K = self.strike
    r = self.rate
    t = self.time
    d1,d2 = d(self)
    P = -norm.cdf(-d1) * S + norm.cdf(-d2) * K * np.exp(-r * t)
    return P


def delta(self):
    '''
    

    Returns delta
    -------
    Float
    
    Given a option and all its caracteristics returns its delta

    '''
    contract_type = self.type
    if contract_type.lower() == 'u':
        return 1
    d1,d2 = d(self)
    
    if contract_type.lower() == 'c':
        return norm.cdf(d1)
    elif contract_type.lower() == 'p':
        return -norm.cdf(-d1)
    
    
def gamma(self):
    '''
    

    Returns gamma
    -------
    Float
    
    Given a option and all its caracteristics returns its gammma

    '''
    sigma = self.vol
    S = self.spot
    K = self.strike
    r = self.rate
    t = self.time
    if self.type.lower() == 'u':
        return 0
    d1,d2 = d(self)
    
    return( K * np.exp(-r * t) * (norm.pdf(d2) / (S**2 * sigma * np.sqrt(t) ))) 

def theta(self):
    '''
    

    Returns theta
    -------
    Float
    
    Given a option and all its caracteristics returns its theta

    '''
    sigma = self.vol
    S = self.spot
    K = self.strike
    r = self.rate
    t = self.time
    if self.type.lower() == 'u':
        return 0
    d1,d2 = d(self)
    contract_type = self.type
    
    if contract_type.lower() == 'c':
        theta = (-S * sigma * norm.pdf(d1) / (2 * np.sqrt(t)) )- r * K * np.exp(-r * t) * norm.cdf(d2)
    elif contract_type.lower() == 'p':
        theta = (-S * sigma * norm.pdf(-d1) / (2 * np.sqrt(t)) )+ r * K * np.exp(-r * t) * norm.cdf(-d2)
    
    return theta

def vega(self):
    '''
    

    Returns vega
    -------
    Float
    
    Given a option and all its caracteristics returns its vega

    '''
    S = self.spot
    t = self.time
    if self.type.lower() == 'u':
        return 0
    d1,d2 = d(self)
    v = S * norm.pdf(d1) * np.sqrt(t)
    
    
    
    return v


if __name__ == "__main__":
    
    #df = investpy.get_stock_recent_data(stock='GLD', country='united states')
    #df['dif'] = df["Close"].pct_change(periods=1)
    
    
    
    #sigma = 0.2214
    #p = 0
    #S = 183.355
    #K = 3450
    #K1 = 3200
    #r = 0.0115/100
    #t = 133/365
    contract_type = 'P'
    
    # perna1  = opt(contract_type,S,K,r,t,sigma = sigma)
    # perna2 = opt(contract_type,S,K1,r,t,sigma = sigma)
    # perna3 
    spx = np.arange(spx_ontem - 1500, spx_ontem +1501,100)
    vol = np.arange(17-16,17+70,1)
    vol =vol[::-1]
    #tempo = list(range (50,51,1))
    #tempo = [50]# quanto tempo dc até vencimento
    #tempo = [x/365 for x in tempo]

    
    #det = np.zeros((len(tempo),len(spx)))
    #veg = np.zeros((len(tempo),len(spx)))
    #gam = np.zeros((len(tempo),len(spx)))
    pay = np.zeros((len(vol),len(spx)))
    #tht = np.zeros((len(tempo),len(spx)))
    '''
    Parametrizacao da vol
    '''
    perna1_param = opt(options_list[0]['tipo'],Spot = spx_ontem,Strike = options_list[0]['strike'],r = risk_free,t = options_list[0]['tempo'],p=options_list[0]['YestPrice'])
    perna2_param = opt(options_list[1]['tipo'],Spot = spx_ontem,Strike = options_list[1]['strike'],r = risk_free,t = options_list[1]['tempo'],p=options_list[1]['YestPrice'])
    
    vol1 = perna1_param.vol
    vol2 = perna2_param.vol
    ratio_vol = vol2/vol1
    
    if n_options>2:
        perna3_param = opt('P',Spot = spx_ontem,Strike = 3900,r = risk_free,t = 90/365,p=62.7)
        vol3 = perna3_param.vol
        ratio_vol2 = vol3/vol1
        
    
    #t = tempo[0]
    pay_2 = pd.DataFrame()
    delta_df = pd.DataFrame()
    for v in range(len(vol)):
        for p in range(len(spx)):      
            perna1 = opt('P',Spot = spx[p],Strike = options_list[0]['strike'],r = risk_free,t = options_list[0]['tempo'],sigma = 1*vol[v]/100)
            perna2 = opt('P',Spot = spx[p],Strike = options_list[1]['strike'],r = risk_free,t = options_list[1]['tempo'],sigma = ratio_vol *vol[v]/100)
            if n_options > 2:
               perna3 = opt('P',Spot = spx[p],Strike = options_list[2]['strike'],r = risk_free,t = options_list[2]['tempo'],sigma = ratio_vol2 *vol[v]/100)
               px = options_list[0]['quantidade']*perna1.premium +options_list[1]['quantidade']*perna2.premium + options_list[2]['quantidade'] * perna3.premium
            else:
                px = options_list[0]['quantidade']*perna1.premium +options_list[1]['quantidade']*perna2.premium
                dt = options_list[0]['Amount']*delta(perna1) + options_list[1]['Amount']*delta(perna2)
                dt = dt*spx[p]*100/(PL_tot/dolar)
            df = pd.DataFrame([[vol[v],spx[p],px]],columns=['vol','spot','px'])
            df1 = pd.DataFrame([[vol[v],spx[p],dt]],columns=['vol','spot','delta'])
            pay_2 = pay_2.append(df)
            delta_df = delta_df.append(df1)


#sns.heatmap(pay,xticklabels = spx,yticklabels = vol,cmap="viridis" )
amount = - options_list[0]['Amount']

put_ratio_2 = pay_2.copy()  

put_ratio_2 ['px'] = put_ratio_2['px']*amount*100*dolar/PL_tot 
put_ratio_2 ['val_date'] = ontem
put_ratio_2['spot_ontem'] = spx_ontem
spx_arred = np.round(spx,0)

put_ratio_2.to_sql('put_ratio', 
                  con=engine, 
                  index=False, 
                  if_exists='append')


delta_df['val_date'] = ontem

delta_df.to_sql('put_ratio_delta', 
                  con=engine, 
                  index=False, 
                  if_exists='append')

telegram_send.send(messages = ['Put Ratio OK!!'])

