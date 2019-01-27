#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 23 11:29:06 2019

@author: partha
"""

import time
import pandas as pd
#import numpy as np
import datetime 
import json
from pymongo import MongoClient
start = time.time()
client = MongoClient()


db = client['spectaData']
#collection = db.test_payment
collection_dhaka = db.test_invoice_Dhaka
collection_Sylhet = db.test_invoice_Sylhet
collection_CTG = db.test_invoice_CTG
collection_Khulna = db.test_invoice_Khulna
collection_Rajshahi = db.test_invoice_Rajshahi

cols=['Invoice Number','MQ ID','Name','Operational Entity','reqdate','Description','Net Sale','VAT','Gross Sale','VAT Rate']

#xls = pd.ExcelFile("/home/partha/INVOICE.XLS", names = cols)
# Now you can list all sheets in the file
#tabnames = xls.sheet_names
data = pd.read_excel("/home/partha/INVOICE.XLS",sheet_name = None, names=cols )
#len(data)
invoice = pd.concat([data[frame] for frame in data.keys()], sort = False, axis = 0)
len(invoice)
invoice.columns
invoice.dtypes
invoice.head()
invoice['reqdate'] = invoice['reqdate'].astype(str)
invoice['reqdate'] = invoice['reqdate'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
invoice['reqdate'] = invoice['reqdate'].apply(lambda x: x.isoformat())
invoice['Name'] = invoice['Name'].apply( lambda x: x.upper())
invoice['Description'] = invoice['Description'].apply( lambda x: x.upper())
invoice['plan'] = "UNKNOWN"
invoice['Invoice Number'] = invoice['Invoice Number'].apply( lambda x: x.upper())
#invoice['Operational Entity'].nunique()
#invoice['Operational Entity'].value_counts()
invoice['recordDate']=datetime.datetime.now()
invoice['recordDate'] = invoice['recordDate'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
invoice['recordDate'] = invoice['recordDate'].astype(str)
invoice['recordDate'] = invoice['recordDate'].apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S' ))
invoice['recordDate'] = invoice['recordDate'].apply(lambda x: x.isoformat())
# new data frame with split value columns 
new = invoice["Operational Entity"].str.split(" ", n = 1, expand = True) 
# making seperate first name column from new data frame 
invoice["site"]= new[0] 
invoice['site'] = invoice['site'].apply( lambda x: x.upper())
  # making seperate last name column from new data frame 
invoice["area"]= new[1] 
invoice['area'] = invoice['area'].apply( lambda x: x.upper())
# Dropping old Name columns 
invoice.drop(columns =["Operational Entity"], inplace = True) 
## replacing na values 
#payment['mode'] = payment['mode'].fillna("UNKNOWN", inplace = False)      
#df_payment = payment.rename_axis('site').reset_index()
#df_payment['site'] = df_payment['site'].fillna("UNKNOWN", inplace = False)
#df_payment = df_payment[['subscriberid','recordDate','mode','paymentdesc','amt','site']]
invoice_site = invoice.groupby('site')
multi_df = {}
for name, group in invoice_site:
    multi_df[name] = invoice[(invoice.site == name)]
    
Dhaka = multi_df['DHAKA']
Sylhet = multi_df['SYLHET']
CTG = multi_df['CHITTAGONG']
khulna = multi_df['KHULNA']
Rajshahi = multi_df['RAJSHAHI']

def date_hook(json_dict):
    for (key, value) in json_dict.items():
        try:
            json_dict[key] = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
        except:
            pass
    return json_dict

Dhaka_json = json.loads(Dhaka.to_json(orient='records'),object_hook= date_hook)
collection_dhaka.insert_many(Dhaka_json)

Syleht_json = json.loads(Sylhet.to_json(orient='records'),object_hook= date_hook)
collection_Sylhet.insert_many(Syleht_json)

CTG_json = json.loads(CTG.to_json(orient='records'),object_hook= date_hook)
collection_CTG.insert_many(CTG_json)

Khulna_json = json.loads(khulna.to_json(orient='records'),object_hook= date_hook)
collection_Khulna.insert_many(Khulna_json)

Rajshahi_json = json.loads(Rajshahi.to_json(orient='records'),object_hook= date_hook)
collection_Rajshahi.insert_many(Rajshahi_json)

end = time.time()
print('Time: ',end - start)