# -*- coding: utf-8 -*-
"""
Created on Tue Sep 28 13:24:03 2021

@author: ludmilla.tsurumaki
"""
import pandas as pd
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

stop_words= set(stopwords.words("english"))

doc = fitz.open(r"I:\Riscos\Earnings call\20200429_Microsoft_Corp-_Earnings_Call_2020-4-29_DN000000002830540408.pdf")
n_pages = 20
text  = str()
for i in range(n_pages):
    page = doc[i]
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

# Stemming
from nltk.stem import PorterStemmer
from nltk.tokenize import sent_tokenize, word_tokenize

ps = PorterStemmer()
stemmed_words=[]
for w in no_stop_words:
    stemmed_words.append(ps.stem(w))


from nltk.stem.wordnet import WordNetLemmatizer

lem = WordNetLemmatizer()
lemmatized_words=[]
for w in stemmed_words:
    lemmatized_words.append(lem.lemmatize(w))
    
#pos tagging
pos_tagged = nltk.pos_tag(lemmatized_words)
    
fdist = FreqDist(lemmatized_words)
import matplotlib.pyplot as plt
fdist.plot(30,cumulative=False)
plt.show()