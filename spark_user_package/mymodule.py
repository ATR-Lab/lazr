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



_spark_ = PySpark_con()

pandas_dataframe = _spark_.data_transformations(option = "timeseries")
    
print(pandas_dataframe.iloc[:11, :])




