# -*- coding: utf-8 -*-
"""
Created on Mon Feb 24 11:02:36 2020

@author: Utsav.minal
"""

from kafka import KafkaConsumer
import sys
import pandas as pd
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import numpy as np


        
bootstrap_servers = ['localhost:9092']
topicName = 'myTopic'
consumer = KafkaConsumer (topicName, group_id = 'group1',bootstrap_servers = bootstrap_servers,
auto_offset_reset = 'latest')

count=0


try:
    for message in consumer:
        decod_data=message.value.decode('utf-8') #receiving all message from consumer and saving it na veriable
        
        #below commented 2line code will print the  data which is being recieved by Kafka
        #print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,message.offset
        # , message.key,message.value.decode('utf-8')))
        
        print(count) #printing counter just to count the times of execution of code
        count+=1
        
        #stri=eval(a)        #  eval()-To convert a string to dictionary, we have to ensure that 
                             # the string contains a valid representation of dictionary.
                             
        data=pd.DataFrame.from_dict(eval(decod_data)) # converting dictionary into dataframe
        
        lat_long=[] # to save lat&long collectively
        lat=[]      # save latitude only
        long=[]     # save longitude only
        
        for key,value_data in data['Location'].items(): #data['Location'] contain dictionary value_data{lat,long}
            for ky,val in value_data.items():           #saving lat & long into lat_long list
                lat_long.append(val)                    #in odd location we have longotide & latitude on even 


        for iteratr in range(0,len(lat_long),2): # saving latitude data into lat list by traversing even loc of lat_long
            lat.append(lat_long[iteratr])
        for iteratr in range(1,len(lat_long),2): # saving latitude data into long list by traversing odd loc of lat_long
            long.append(lat_long[iteratr])
        fig = plt.figure(figsize=(15,9))
        
        base_map = Basemap(projection='mill', #basemap is toolkit which is being used in this program to print world map.  
                           llcrnrlat = -90,
                           urcrnrlat = 90,
                           llcrnrlon = -180,
                           urcrnrlon = 180,
                           resolution = 'l')
                
        base_map.drawcoastlines()
                
        base_map.drawparallels(np.arange(-90,90,10),labels=[True,False,False,False])
        base_map.drawmeridians(np.arange(-180,180,30),labels=[0,0,0,1])
        
        #for i in range (len(e)):
        base_map.scatter(long,lat,latlon=True,s=100 )  #passing lat & long as list in scatterfunction
        plt.title('Crime Location-'+str(data['YEAR'][0])+'-'+str(data['MONTH'][0]), fontsize=20) # printing title above every map
        plt.show()
        
except KeyboardInterrupt:
    sys.exit()