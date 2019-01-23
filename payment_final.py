#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 21 12:00:19 2019

@author: partha
"""
import time
import pandas as pd
#import numpy as np
import datetime 
from pymongo import MongoClient
start = time.time()

######### Connect with DataBase #####################
client = MongoClient()


db = client['spectaData']
#collection = db.test_payment
collection_dhaka = db.test_payment_Dhaka
collection_Sylhet = db.test_payment_Sylhet
collection_CTG = db.test_payment_CTG
collection_Khulna = db.test_payment_Khulna
collection_Rajshahi = db.test_payment_Rajshahi

######## Load Data File ############################
#xls = pd.ExcelFile("/home/partha/PAYMENTS Site.xlsx")
### Now you can list all sheets in the file
#xls.sheet_names
data = pd.read_excel("/home/partha/PAYMENTS Site.xlsx", sheet_name = None, index_col = "site")
len(data)
payment = pd.concat([data[frame] for frame in data.keys()], sort = False, axis = 0)
len(payment)

#payment = pd.read_excel("/home/partha/PAYMENTS Site.xlsx",  index_col = "site")
payment.drop(['Bogra','Barishal','Benapole','Comilla','Coxbazar','Jessore','Mouluvibazar','Mymenshing','Narayangonj','Nator','Sirajgonj'], inplace= True)
#tabnames = payment.sheet_names
#i = 0
#df = payment.parse(sheetname=tabnames[i], skiprows=0)
#df.head(2)
#for sheet in xls:
#    print(sheet)
#    payment = pd.read_csv("/home/partha/PAYMENTS Site.xlsx",  index_col = "site")
#    payment.isnull().sum()
#    payment = payment.dropna()
payment['paymentdate'] = payment['paymentdate'].astype(str)
payment['paymentdate'] = payment['paymentdate'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
payment['paymentdesc'] = payment['paymentdesc'].apply( lambda x: x.upper())
## replacing na values 
payment['mode'] = payment['mode'].fillna("UNKNOWN", inplace = False)      
df_payment = payment.rename_axis('site').reset_index()
df_payment['site'] = df_payment['site'].fillna("UNKNOWN", inplace = False)
df_payment['recordDate'] = datetime.datetime.now()
#df_payment = df_payment[['subscriberid','recordDate','mode','paymentdesc','amt','site']]
df_payment = df_payment[['subscriberid','paymentdate','mode','paymentdesc','amt','site','recordDate']]
payment_site = df_payment.groupby('site')
multi_df = {}
for name, group in payment_site:
    multi_df[name] = df_payment[(df_payment.site == name)]

Dhaka = multi_df['Dhaka']
Sylhet = multi_df['SYLHET']
CTG = multi_df['CTG']
khulna = multi_df['KHULNA']
Rajshahi = multi_df['Rajshahi']




###########Insert Data to DataBase #################
##### Dhaka###########
Dhaka_list = []
for i in Dhaka.index:
  data_dict = {}
  for column in Dhaka.columns:
    data_dict[column] = Dhaka[column][i]
  Dhaka_list.append(data_dict)
payment_record = collection_dhaka.insert_many(Dhaka_list)
##### Syleht #############
Sylhet_list = []  
for i in Sylhet.index:
  data_dict = {}
  for column in Sylhet.columns:
    data_dict[column] = Sylhet[column][i]
  Sylhet_list.append(data_dict)
payment_record = collection_Sylhet.insert_many(Sylhet_list)

##### CTG #############
CTG_list = []
for i in CTG.index:
  data_dict = {}
  for column in CTG.columns:
    data_dict[column] = CTG[column][i]
  CTG_list.append(data_dict)
payment_record = collection_CTG.insert_many(CTG_list)

##### Khulna ###########
khulna_list = []
for i in khulna.index:
  data_dict = {}
  for column in khulna.columns:
    data_dict[column] = khulna[column][i]
  khulna_list.append(data_dict)
payment_record = collection_Khulna.insert_many(khulna_list) 

##### Rajshahi #########
Rajshahi_list = []
#post_data_list = []
for i in Rajshahi.index:
  data_dict = {}
  for column in Rajshahi.columns:
    data_dict[column] = Rajshahi[column][i]
  Rajshahi_list.append(data_dict)
payment_record = collection_Rajshahi.insert_many(Rajshahi_list)

end = time.time()
print('Time: ',end - start)