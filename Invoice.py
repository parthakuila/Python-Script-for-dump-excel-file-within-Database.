# -*- coding: utf-8 -*-
"""
Created on Fri Jan 18 14:38:39 2019

@author: Partha Kuila
"""

import pandas as pd 
#import numpy as np
import datetime
from pymongo import MongoClient
#import pprint
#import time


#Client = MongoClient('mongodb://10.0.0.14:27017',
#                     username = 'spectauser',
#                     password = 'spectaDb#011',
#                     authSource = 'spectaData',
#                     authMechanism = 'SCRAM-SHA-1')
#
#
##Database  Creation/Access 
#db = Client.spectaData
#
##Creating Table or collection in mongoDb
#sylhet = db.sylhet_invoice_list
#CTG    = db.CTG_invoice_list 
#Dhaka  = db.Dhaka_invoice_list

#list declaration
sylhet_list = []
CTG_list    = []
Dhaka_list  = []



#record to store individual record
#record = {}

#excel_sheet = ['Sheet1','Sheet2','Sheet3','Sheet4','Sheet5','Sheet6','Sheet7','Sheet8','Sheet10','Sheet11']


excel_sheet = ['Sheet1','Sheet2','Sheet3','Sheet4','Sheet5','Sheet6','Sheet7','Sheet8','Sheet9','Sheet10','Sheet11']

#hdr_lst=['invoiceid','subscriberid','name','optnal_entity','Date','description','net_sale','vat','gross_sale','vat_rate']
#print('h')
for sheet in excel_sheet:
    print(sheet)
    df_invoice = pd.read_excel("INVOICE.XLS", sheet_name=sheet)
    
    df_invoice.columns=['invoiceid','subscriberid','name','optnal_entity',
                        'Date','description','net_sale','vat','gross_sale','vat_rate']
    
    df_len = len(df_invoice)
    print(df_len)
    #time.sleep(20)
    
    df_invoice['recorddate'] = datetime.datetime.utcnow()
    df_invoice['plan'] = "UNKNOWN"
    
    df_invoice['reqdate'] = df_invoice['Date'].astype(str)
    
    #df_invoice['Date'] = df_invoice['Date'].apply(lambda x:x +' 12'+':00'+':00')
    df_invoice['reqdate'] = df_invoice['reqdate'].apply(lambda x:x +' 12:00:00')
    
    df_invoice['reqdate'] = df_invoice['reqdate'].apply(lambda x:datetime.datetime.strptime(x,"%Y-%m-%d %H:%M:%S"))
    df_invoice.drop(['Date'],axis = 1, inplace = True)
    
    #print('l')
    i=0
    
    # create an iterator and form index and series pair of each row
    for row_index, row in df_invoice.iterrows():
        record = {}
        
        if row['optnal_entity'].startswith('Dhaka'):
            record['gross_sale']   = row['gross_sale']
            record['net_sale']     = row['net_sale']
            record['vat']          = row['vat']
            record['name']         = row['name'].upper()
            record['subscriberid'] = row['subscriberid'].upper()
            record['description']  = row['description'].upper()
            record['invoiceid']    = row['invoiceid'].upper()
            record['reqdate']      = row['reqdate']
            record['plan']         = row['plan']
            record['vat_rate']     = row['vat_rate']
            record['recorddate']   = row['recorddate']
            #print(record)
            Dhaka_list.append(record)
            #print(i)
           # print('l')
            
        elif row['optnal_entity'].startswith('Chittagong'):
            record['gross_sale']   = row['gross_sale']
            record['net_sale']     = row['net_sale']
            record['vat']          = row['vat']
            record['name']         = row['name'].upper()
            record['subscriberid'] = row['subscriberid'].upper()
            record['description']  = row['description'].upper()
            record['invoiceid']    = row['invoiceid'].upper()
            record['reqdate']      = row['reqdate']
            record['plan']         = row['plan']
            record['vat_rate']     = row['vat_rate']
            record['recorddate']   = row['recorddate']
            #print(record)
            CTG_list.append(record)
            #print(i)
            
        elif row['optnal_entity'].startswith('Sylhet'):
            record['gross_sale']   = row['gross_sale']
            record['net_sale']     = row['net_sale']
            record['vat']          = row['vat']
            record['name']         = row['name'].upper()
            record['subscriberid'] = row['subscriberid'].upper()
            record['description']  = row['description'].upper()
            record['invoiceid']    = row['invoiceid'].upper()
            record['reqdate']      = row['reqdate']
            record['plan']         = row['plan']
            record['vat_rate']     = row['vat_rate']
            record['recorddate']   = row['recorddate']
            #print(record)
            sylhet_list.append(record)
            #print(i)
            #print('o')
        #record.clear()
        
        
   
#    if len(sylhet_list)>0:
##        print('sylhet')
##        print(sylhet_list)
#        sylhet.insert_many(sylhet_list)
#        sylhet_list.clear()
#    if len(CTG_list)>0:
##        print('CTG')
##        print(CTG_list)
#        CTG.insert_many(CTG_list)
#        CTG_list.clear()
#    if len(Dhaka_list)>0:
##        print('Dhaka')
##        print(Dhaka_list)
#        Dhaka.insert_many(Dhaka_list)
#        Dhaka_list.clear()
    #print(row_index)     
    print(sheet,"Over")
#    print(row)
#    print('')
     
print("List Printing ")    
    

print(len(sylhet_list))
print(len(CTG_list))
print(len(Dhaka_list))

print("DataBase Acess")    
Client = MongoClient('mongodb://10.0.0.14:27017',
                     username = 'spectauser',
                     password = 'spectaDb#011',
                     authSource = 'spectaData',
                     authMechanism = 'SCRAM-SHA-1')

#Database  Creation/Access 
db = Client.spectaData


#Creating Table or collection in mongoDb
sylhet = db.sylhet_lku_invoice_list
CTG    = db.CTG_lku_invoice_list 
Dhaka  = db.Dhaka_lku_invoice_list

sylhet.insert_many(sylhet_list)
CTG.insert_many(CTG_list)
Dhaka.insert_many(Dhaka_list)


Dhaka_list.clear()
sylhet_list.clear()
CTG_list.clear()
   
print('Complete Run ') 

#for x in Dhaka .find():
#    pprint.pprint(x)  
