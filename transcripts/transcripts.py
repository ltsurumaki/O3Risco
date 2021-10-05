# -*- coding: utf-8 -*-
"""
Created on Tue Sep 28 13:24:03 2021

@author: ludmilla.tsurumaki
"""
import pandas as pd
import os
import glob
import fitz
import re
import nltk
import string
nltk.download('punkt')
nltk.download('wordnet')
nltk.download('averaged_perceptron_tagger')
from nltk.tokenize import sent_tokenize
from nltk.tokenize import word_tokenize
from nltk.probability import FreqDist
from nltk.corpus import stopwords

from sqlalchemy import create_engine
#-------------------------------- Connection ---------------------------------
    
#import win32com.client
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

#--------------------------------- Arquivos ----------------------------------                        
pasta = 'I:/Riscos/Earnings call/MSFT US/'
os.chdir(pasta)
extension = 'pdf'
docs = [i for i in glob.glob('*.{}'.format(extension))]
#----------------------------- Limpeza dos textos ----------------------------
stop_words= set(stopwords.words("english"))

msft = pd.DataFrame()
for i in docs:
    doc = fitz.open(pasta + i)
    n_pages = doc.page_count
    text  = str()
    for pg in range(n_pages):
        page = doc[pg]
        new_text = page.getText("text")
        text = text + new_text.lower()
    #frases    
    frases = sent_tokenize(text)
    frases2 = [s.translate(str.maketrans('', '', string.punctuation)) for s in frases]
    
    #palavras
    words = word_tokenize(text.translate(str.maketrans('', '', string.punctuation)))
    words_df = pd.DataFrame(words)
    
    #remove stop words
    no_stop_words = words_df[~words_df.isin(stop_words)].dropna()
    no_stop_words = list(no_stop_words[0])
    
    
    #Lexicon Normalization
    #performing stemming and Lemmatization
    '''
    # Stemming
    from nltk.stem import PorterStemmer
    from nltk.tokenize import sent_tokenize, word_tokenize
    
    ps = PorterStemmer()
    stemmed_words=[]
    for w in no_stop_words:
        stemmed_words.append(ps.stem(w))
    '''
    
    from nltk.stem.wordnet import WordNetLemmatizer
    
    lem = WordNetLemmatizer()
    lemmatized_words=[]
    for w in no_stop_words:
        lemmatized_words.append(lem.lemmatize(w))
        
    #pos tagging
    pos_tagged = nltk.pos_tag(lemmatized_words)
        
    fdist = FreqDist(lemmatized_words)
    fdist_df = pd.DataFrame(list(fdist.items()), columns = ["word","frequency"]) 
    fdist_df['flag'] = i[0:7]
    fdist_df['ticker'] = 'msft' 
    msft = msft.append(fdist_df)
    '''
    import matplotlib.pyplot as plt
    fdist.plot(30,cumulative=False)
    plt.show()
    '''
#------------------------------- Subindo no BD  ------------------------------
msft.to_sql('transcripts_msft',con = engine, index=False, if_exists='replace')
