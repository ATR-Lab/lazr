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

"""Discarded yet kept in case we use group of consumers"""

#consumer = KafkaConsumer(
#    'trails',
#     bootstrap_servers=['localhost:9092'],
#     auto_offset_reset='earliest',
#     enable_auto_commit=True,
#     group_id="kafka-demo-application",
#     value_deserializer=lambda x: loads(x.decode('utf-8')))



print("Consumer starting")

consumer = KafkaConsumer(
    'astronauts_data',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=True,
     value_deserializer=lambda x: json.loads(x.decode('ascii')))

print("Consumer is up!")

#following is for VERSION: 3.6.0 or later on atlas - IS NOT WORKING
#client = pymongo.MongoClient('mongodb+srv://admin:admin@mongodb-atlas-afdbx.mongodb.net/test?retryWrites=true&w=majority')
"""for some reason choose VERSION: 3.4.0 or later on atlas"""


try:
    print("Connecting to MongoDB")
    """ Atlas """
    #client = pymongo.MongoClient("mongodb://admin:admin@mongodb-atlas-shard-00-00-afdbx.mongodb.net:27017,mongodb-atlas-shard-00-01-afdbx.mongodb.net:27017,mongodb-atlas-shard-00-02-afdbx.mongodb.net:27017/test?ssl=true&replicaSet=MongoDB-Atlas-shard-0&authSource=admin&retryWrites=true&w=majority")
    client = MongoClient('mongodb://localhost:27017/')
    print("Connection Estabilished")

except pymongo.errors.ConnectionFailure:
    
    print("SERVER ISN'T AVAILABLE")



#db = client.data

db_cluster = client['debugdata']#data

collection = db_cluster['debug5']#astro-sensor


#print(db_cluster.list_collection_names())
#
#
#db = client.kaggle
#
##db_cluster = db["kaggle"]
#
#db_cluster = db
#
#collection = db_cluster["ecg"]


#db.list_collection_names()

consumer.subscribe(['astronauts_data'])

for message in consumer:
    #message = message.value
    print("Message value:",message.value)
    
    try:
        js =  json.loads(message.value)
        #collection.insert_one(js)
                
        collection.insert_many(js)
            
        print("added to database")
        
    except json.decoder.JSONDecodeError:

        print('Unable to decode: %s', js)











#collection.insert_one(df_dict).inserted_id

















#
#
#
#client
#
#db = client.test
#
#db_cluster = db['test']
#
#col = db_cluster['tesing-colle']
#
#db.list_collection_names()
#
#import datetime
#
#post = {"author": "Mike",
#         "text": "My first blog post!",
#         "tags": ["mongodb", "python", "pymongo"],
#         "date": datetime.datetime.utcnow()}
#
#
#
#posts = db.posts
#
#post_id = posts.insert_one(post).inserted_id
#
#
#db.list_collection_names()