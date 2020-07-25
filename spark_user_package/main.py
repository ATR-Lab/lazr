#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 13:19:50 2020

@author: skywalker
"""

import mymodule



from matplotlib.figure import Figure

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

from matplotlib.figure import Figure


#importing the PySpark_con class from mymodule file to _spark_
_spark_ = mymodule.PySpark_con()


flask_app = flask.Flask(__name__)
    
@flask_app.route('/testing')
def testing():
    
    return("Connection Estabilished")
    
@flask_app.route('/')
def hello():
    
    return("Welcome to our project")
    

@flask_app.route('/sample_data', methods=["GET","POST"])
def to_timeseries():
    
    pandas_dataframe = _spark_.data_transformations(option = "timeseries")
    
    pandas_dataframe.iloc[:11, :]
    
    plot_info = go.Table(
            
        header=dict(values=[i for i in pandas_dataframe.columns],
                    line = dict(color='#7D7F80'),
                    fill = dict(color='#a1c3d1'),
                    align = ['left'] * 5),
        cells=dict(values=[pandas_dataframe[i] for i in range(0, len(pandas_dataframe.columns.values))],
                   line = dict(color='#7D7F80'),
                   fill = dict(color='#EDFAFF'),
                   align = ['left'] * 5))

    plot_data = go.Data([plot_info])
    
    layout = go.Layout(autosize=True, width = 1000, height = 1000, title = "Sample data")
    
    figure = go.Figure(data = plot_data, layout = layout)
    
    output = opy.plot(figure, auto_open=True, output_type='div')
    
    return flask.render_template("heart_time_.html", output = output)


    
@flask_app.route('/heartrate', methods=["GET","POST"])
def chart_heart_rate():
    
    pandas_dataframe = _spark_.data_transformations(option = "toPandas")
    
    _spark_.querying(start=2, end = 900)
   
    figure = go.Figure()
    
    figure.add_trace(go.Scatter(y = pandas_dataframe['heart_bpm'], x = pandas_dataframe['time']))
        
    figure.update_layout(title_text="Heart rate against time", xaxis_title = "Heart Rate" , yaxis_title = "Time")
        
    output = opy.plot(figure, auto_open=True, output_type='div')
    
    return flask.render_template("heart_time_.html", output = output)
    
    
    
#how do you scale out - containerization
    
if __name__ == "__main__":
    
    flask_app.run(debug=True, use_reloader=True)







