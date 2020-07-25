#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 13:18:00 2020

@author: skywalker
"""
import io
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
import pyspark
from pyspark.sql import SparkSession, functions
import time
import plotly.offline as opy
import plotly.graph_objs as go
import flask
from flask import Flask, request, Response, redirect
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import json
#from database.db import initialize_database
#from database.models import Data_create
#import pymongo
from werkzeug.utils import secure_filename
from pyspark.sql.functions import col,sum

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext


print("Importing mymodule ...")


class PySpark_con:
    
    def __init__(self):
        
        """
                We are initializing only the Pyspark & Mongo elements
        
        """
        
        self.spark_start_time = time.time()
        
        #setting up the spark config
        self.spark = SparkSession\
            .builder\
            .appName("Mongo connector")\
            .config('spark.mongodb.input.uri','mongodb://localhost:27017/data.data.astro-sensor')\
            .config('spark.mongodb.output.uri','mongodb://localhost:27017/data.data.astro-sensor')\
            .getOrCreate()
            
        print("--- SPARK SESSION time: %s seconds ---" %(time.time() - self.spark_start_time))
        
        #reading the data
        mongo_to_spark_time = time.time()
        
        self.pyspark_dataframe = self.spark.read.format('mongo').load()
        
        

        self.mongo_load_time = (time.time()-mongo_to_spark_time)

        pass
    
    
    def querying(self, start, end): #showing filtered values
        
        self.pyspark_dataframe.filter((self.pyspark_dataframe.time>start)&((self.pyspark_dataframe.time<end))).show(n = 5)
        
        query_start_time = time.time()
        
        querying_col_time = self.pyspark_dataframe.filter((self.pyspark_dataframe.time>start)&((self.pyspark_dataframe.time<end)))
        
        query_end_time = time.time()
            
        print("--- Querying loading time: %s seconds ---" %(query_end_time-query_start_time))
            
        return querying_col_time
    
    
    """ All transformation related methods at one place """
    def data_transformations(self, option):
        
        def share_data_flask(self, option):
            
            #changing to dataframes
            
            pandas_dataframe = self.pyspark_dataframe.toPandas()
            #df_pandas = querying(self)
            
            return pandas_dataframe
        
        
        if(option == "toPandas"):
            
            pandas_dataframe = share_data_flask(self,option)
            
            return pandas_dataframe
        
        elif(option == "timeseries"):
            
            pandas_dataframe = share_data_flask(self,option)
            pandas_dataframe.index = pandas_dataframe['timer']
            
            return pandas_dataframe

        return None
    
    
    def data_details(self):
        
        schema_ = self.pyspark_dataframe.printSchema()
        
        rows, cols = ((self.pyspark_dataframe.count(), len(self.pyspark_dataframe.columns)))
        
        missing_values = self.pyspark_dataframe.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in self.pyspark_dataframe.columns)).collect() #returns a list
        
        pass



#
_spark_ = PySpark_con()

pandas_dataframe = _spark_.data_transformations(option = "timeseries")
    
print(pandas_dataframe.iloc[:11, :])




#.config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11-2.4.2')\



# To Pandas dataframes


#df.show(n=5)#to view the data

#df.printSchema() # to view the schema


#
#print("--- Quering ---")
#print(df_pandas['timer'].describe())


#start_time, end_time, option = (input("Please enter start and end time to query:  ")).split(' ')

#print(start_time,end_time, option)


#queryed_time = querying(start = 2, end = 1000, option= 'yes')



#df_query_time = df_pandas.loc[(df_pandas['time'] >= start_time) and (df_pandas['time'] <= end_time)]

#df_pandas.query('timer >= 1.005')

#print(df_query_time.head(3))


#flask_app = flask.Flask(__name__)
#
#backup_df = df 

#   backup_df.show(n=10)

#
#def missing_values():
#    
#    temp = df.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in df.columns)).collect() #returns a list
#    
#    for i in temp:
#        
#        print(i)
#
#
#print(missing_values())
#


#

"""      IMPO       """
#df.select(df['_id'], df['time'],df['heart_bpm'], df['v_fan'] ).show()
#selected_df = backup_df.select(df['_id'], df['time'],df['heart_bpm'], df['v_fan'] )
#selected_df.show()
#
##df.select(df['_id'], df['time'],df['heart_bpm'], df['v_fan'] ).show()
#selected_df = backup_df.select(df['_id'], df['time'],df['heart_bpm'], df['v_fan'] )
##selected_df.show()
#
#web_dataframe = selected_df.withColumn('time', functions.round(selected_df['time'], 2))        
#
#
#
#
#
#
#type(web_dataframe)
#
#
#selected_df = web_dataframe.select(web_dataframe['_id'], 
#                                   web_dataframe['time'],
#                                   web_dataframe['heart_bpm'],
#                                   web_dataframe['v_fan']).toPandas()
#



#
#web_dataframe = selected_df.withColumn('time', functions.round(selected_df['time'], 2))        
#
#
#
#
#
#
#type(web_dataframe)
#
#
#selected_df = web_dataframe.select(web_dataframe['_id'], 
#                                   web_dataframe['time'],
#                                   web_dataframe['heart_bpm'],
#                                   web_dataframe['v_fan']).toPandas()
#
#
#
#
#@flask_app.route('/')
#def hello():
#    
#    return("Welcome to our project")
#from matplotlib.figure import Figure
#
#@flask_app.route('/visualization1', methods=["GET","POST"])
#def chart_heart_rate():
#    
#    figure = go.Figure()
#
#    figure.add_trace(go.Scatter(y = selected_df['heart_bpm'], x = selected_df['time']))
#    
#    figure.update_layout(title_text="Heart rate against time", xaxis_title = "Heart Rate" , yaxis_title = "Time")
#    
#    output = opy.plot(figure, auto_open=True, output_type='div')
#
#    return flask.render_template("index.html", output = output)
#
#
#
##how do you scale out - containerization
#
#if __name__ == "__main__":
#    
#    flask_app.run(debug=True, use_reloader=False)
#
#
#
#
#
#
#
