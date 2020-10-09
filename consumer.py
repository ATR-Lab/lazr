#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 14 08:25:07 2020

@author: prade
"""

from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
import json


import pymongo



print("Consumer starting")

consumer = KafkaConsumer(
    'astronauts_data',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     value_deserializer=lambda x: json.loads(x.decode('ascii')))

print("Consumer is up!")


try:
    print("Connecting to MongoDB")
    """ Atlas """
    """for some reason choose VERSION: 3.4.0 for credentials or later on atlas"""
    #client = pymongo.MongoClient("mongodb://admin:admin@mongodb-atlas-shard-00-00-afdbx.mongodb.net:27017,mongodb-atlas-shard-00-01-afdbx.mongodb.net:27017,mongodb-atlas-shard-00-02-afdbx.mongodb.net:27017/test?ssl=true&replicaSet=MongoDB-Atlas-shard-0&authSource=admin&retryWrites=true&w=majority")
    client = MongoClient('mongodb://localhost:27017/')
    print("Connection Estabilished")

except pymongo.errors.ConnectionFailure:
    
    print("SERVER ISN'T AVAILABLE")


db_cluster = client['debugdata']

collection = db_cluster['debug5']


consumer.subscribe(['astronauts_data'])

for message in consumer:

    print("Message value:",message.value)
    
    try:
        js =  json.loads(message.value)
                
        collection.insert_many(js)
            
        print("added to database")
        
    except json.decoder.JSONDecodeError:

        print('Unable to decode: %s', js)
