# -*- coding: utf-8 -*-
"""
Created on Fri Oct  7 13:52:41 2016

@author: Jonny
"""

import sys
import os
import pandas as pd
from pandas.io import sql
import sqlite3
#import subprocess
#from sklearn.ensemble import RandomForestClassifier
#from sklearn.cross_validation import train_test_split
#from sklearn.metrics import accuracy_score
import random

os.chdir(r"C:\Users\Jonny\production")

csv = 'train_numeric.csv'
out_sqlite = 'prod.sqlite'

table_name = 'numeric'
chunksize = 10000
nlines = 1183748

conn = sqlite3.connect(out_sqlite)

numerical = pd.read_csv("train_numeric.csv", nrows = 1000)

numerical.drop_duplicates()
numerical.dropna(axis = 1, how='all')
columns = numerical.columns.unique()
    
for i in range(0, nlines, chunksize):
    df = pd.read_csv(csv, 
                     header = None,
                     nrows = chunksize,
                     skiprows = i
                     )
    df.columns = columns
    
    sql.to_sql(
                df,
               name = table_name,
               con = conn,
               index = False,
               index_label = 'Id',
               if_exists = 'append'
               )
conn.close()

conn = sqlite3.connect(out_sqlite)

responders = pd.read_sql(sql = '''SELECT * FROM numeric WHERE Response = 1;''',
                         con = conn)
row_count = len(responders.index)

non_responder_sample = random.sample(range(1,1183748 - row_count), row_count)

query = 'SELECT Id, (select count(*) from numeric b  where Response = 0 and a.Id >= b.Id) as cnt from numeric a WHERE cnt IN (' + ','.join(map(str, non_responder_sample)) + ')'

non_response_count = pd.read_sql(sql =
                                 query,
                                 con = conn)


query = 'SELECT * FROM numeric WHERE Id IN (SELECT Id FROM numeric WHERE Response = 0 ORDER BY RANDOM() LIMIT (%s));' % row_count

non_responder = pd.read_sql(sql = query,
                            con = conn)


csv = 'train_categorical.csv'
out_sqlite = 'prod.sqlite'

table_name = 'categorical'
chunksize = 10000
nlines = 1183748

conn = sqlite3.connect(out_sqlite)

for i in range(0, nlines, chunksize):
    df = pd.read_csv(csv, 
                     nrows = chunksize,
                     skiprows = i
                     )
    columns = df.columns.unique()
    
    df.to_sql(
                #df,
               name = table_name,
               con = conn,
               index = columns,
               #index_label = 'Id',
               if_exists = 'replace'
               )
conn.close()

categorical = pd.read_csv("train_categorical.csv", nrows = 1000)
numerical = pd.read_csv("train_numeric.csv", nrows = 1000)

date = pd.read_csv("train_date.csv")