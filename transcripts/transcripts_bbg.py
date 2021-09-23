# -*- coding: utf-8 -*-
"""
Created on Tue Sep 14 11:29:44 2021

@author: ludmilla.tsurumaki
"""

import os
import pandas as pd
import pickle
import datetime as dt
from dateutil.relativedelta import relativedelta
os.chdir(r'I:\Riscos\Codigos Automacao')
from wraper import *


'''
msft= BBG.fetch_contract_parameter('MSFT US Equity','EARN_CONF__FINAL_TRANSCRIPT_EXP')
msft.to_csv('I:/Riscos/msft.csv')
'''
from xbbg import blp
a=blp.bdp('MSFT US Equity', 'DT656')
