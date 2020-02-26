# -*- coding: utf-8 -*-
"""
Created on Mon Feb 24 11:02:32 2020

@author: Utsav.minal
"""
from kafka import KafkaProducer
import pandas as pd
import json

bootstrap_servers = ['localhost:9092']
topicName = 'myTopic'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
producer = KafkaProducer()

data_frame=pd.read_csv(r'crime.csv',usecols=[8,9,14,15],encoding='mbcs',low_memory=False)

df=data_frame.dropna() #deleting all rows from dataframe conatining  NA values 
 
groupby_data=df.groupby(['YEAR','MONTH'])[['Lat','Long']] # groping by MONTH & YEAR & write Lat and Long
                                                          # to corresponding MONTH and YEAR

dict_save={}  # empty dictionary to save record {key=tuple(2015.0, 10.0),value=dataframe(YEAR,MONTH,LAT,LONG)}

#list_yr_mn=[] # empty list to save data of YEAR & MONTH explicitly (2015.0, 10.0)

dictn_of_one_groupby={} # will contain (1)List of Location, (2)int of MONTH, (3)int of YEAR

count=0  # simple verible to count how much data is passed onto kafka

                                    #accessing data in groupby_data , it has two values (1)YEAR & MONTH ON which 
for yer_mon,value in groupby_data:  #groupby action has been performed (2) filterwd dataframe which is outcome of groupby
    yer_mon=str(yer_mon)            # converting all the yer_mon(2015.0, 6.0) into string
    dict_save[yer_mon]=value        # making yer_mon as dictionary keyi.e (2015.0, 6.0) and value as its corresponding value
    #list_yr_mn.append(yer_mon)      # saving yer_mon (2015.0, 6.0) individualy in a list
    
    
                                     #accessing data from recently created dectionary
for key,value in dict_save.items():  #here keys are (2015.0, 6.0) and values is dictionary of dataframe(YEAR,MONTH,LAT,LONG)
        value=value.drop(['YEAR','MONTH'],axis=1) # as I only want Lat & Long so droping YEAR & MONTH
        #di.append(v.to_dict('records'))
        value=value.to_dict('records') #converting values into specific dictionary format.
        dictn_of_one_groupby={'YEAR':int(key[1:5]),'Location':value,'MONTH':int(key[9:len(key)-3])} #makihg dictionary in format
                                                                                                    #Year:__,Location:__,Month:__
        
        data_in_json_format=json.dumps(dictn_of_one_groupby,indent=2).encode('utf-8') #converting dictionary into json format 
        
        ack = producer.send(topicName, data_in_json_format)

        #ack = producer.send(topicName, b'Hello World!!!!!!!!')
        metadata = ack.get()
        print(metadata.topic)
        print(metadata.partition)
        print (count)
        count+=1


