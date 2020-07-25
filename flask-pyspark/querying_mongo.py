#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul  2 13:58:58 2020

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




"""============================================================================"""


conf=SparkConf()
conf.set('spark.mongodb.input.uri','mongodb://localhost:27017/debugdata.debug1')
conf.set('spark.mongodb.output.uri','mongodb://localhost:27017/debugdata.debug1')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
dfr = sqlContext.read.format("com.mongodb.spark.sql.DefaultSource")
#df = dfr.option("time", "[{ $gte: 1.0 } and {$lte: 18}]").load()

df = dfr.option({"time", { "$lte": 18 }}).load()




#{"time":{"$gte": match_time}}



print(df)







df.show()




#df.select(df['time']).show()



"""============================================================================"""
