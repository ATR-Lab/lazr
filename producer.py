#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 14:29:27 2020

@author: skywalker
"""



import numpy as np
from time import sleep
from json import dumps
from kafka import KafkaProducer, KafkaConsumer
import json
import pandas as pd


print("Starting producer, please wait...")
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('ascii'))

#
#consumer = KafkaConsumer(
#    'first_topic',
#     bootstrap_servers=['localhost:9092'],
#     auto_offset_reset='latest',
#     enable_auto_commit=True,
#     value_deserializer=None) #lambda x: json.loads(x.decode('ascii'))
#
#
#
#consumer.subscribe(['first_topic'])

#for message in consumer:
#    #message = message.value
#    print("Message value:",message.value)



class Producer:
    
    def __init__(self, file):
        
        self.file = file
        
        #reading the data
        self.data_intermediate = pd.read_json(self.file) 
        
        self.data_intermediate = self.data_intermediate.drop(columns = '_id')
        
        #self.data_intermediate['started_at'] = self.data_intermediate['started_at'].astype(str)
        
        #self.data_intermediate['started_at'] = self.data_intermediate['started_at'].astype(str)
        
        self.data_intermediate['_id'] = np.arange(len(self.data_intermediate))
           
    def data_producing(self):
        
        i = 0
        
        print("PRODUCER STARTED ")
        
        self.data_details()
    
        #using time intervals to produce the data and send it over Kafka
        while(i < len(self.data_intermediate)):
        
        #for(i in range(0, len(self.data_intermediate), 2)):
            # below line works - DONOT MAKE CHANGES - line processing
            self.data_intermediate['file_name'] = "Directory/us/" + str(self.data_intermediate['started_at'][i])[:10]

            data_dict = (self.data_intermediate.iloc[i:i+1,:].to_json(orient='records'))
            #data_dict = (self.data_intermediate.iloc[i:i+1,:].astype(str)).to_json(orient='records')
            
            #data_dict = (self.data_intermediate.iloc[i:i+1,:].astype(str)).to_dict()
                                
            print(data_dict)
            print(self.data_intermediate.started_at.dtype)
            
            producer.send('astronauts_data', value=data_dict)#for thesis topic - astronauts_data debug topic-something
            
            sleep(5)
            
            i+=1
                 
        return "successfully written"
    
    
    def data_details(self):
        
        length = len(self.data_intermediate)
        
        return "Number of records: ",length
    
    
    def data_whole(self):
        
        producer.send('trails', value=self.data_intermediate.to_json())
            
        print(producer.metrics())
        
        
        return "successfully written"
    
    
    def data_testing(self):
        
        #to get a view on the data
        return (self.data_intermediate.iloc[:1,:]).to_dict('list')
    
    



producer_obj1 = Producer("/home/skywalker/Kafka/kafka_2.11-2.3.0/data/astro.json")

producer_obj1.data_producing()

#df_dict = producer_obj1.data_testing()


#producer_obj1.data_details()




data = Producer.data_intermediate




#
#post = {"author": "Mike",
#         "text": "My first blog post!",
#         "tags": ["mongodb", "python", "pymongo"],
#         "date": datetime.datetime.utcnow()}