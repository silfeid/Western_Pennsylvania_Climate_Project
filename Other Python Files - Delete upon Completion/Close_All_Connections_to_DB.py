# -*- coding: utf-8 -*-
"""
Created on Wed Aug 10 15:11:28 2022

@author: brode
"""
import os.path
#Pandas is used to create the dataframes from .csv files, to generally  manipulate the data, create new dataframes from the starting set, and to plot (via matplotlib)
import pandas as pd
#Matplotlib.pyplot is used for plotting, in some instances directly, in others Pandas 'piggybacks' on it in order to plot a dataframe.
import matplotlib.pyplot as plt
#Sys is used only for the func_quit() function, which does just what you'd think it does.  It quits.  Some say it's the best at quitting.
import sys
#This is just to allow me to adjust the plotting parameters to autolayout, below.
from matplotlib import rcParams
#datetime alows strings ina  given format to be interpreted as datetime (or here, just date) objects; although pandas has a similar function built-in, here I wanted to compare the starting and ending dates by which the user can slice the dataframes, in order to make sure that the specified end date does not precede the specified start date.  In hindsight, I could probably also have used it to control the input for those dates, instead of the exception handling function for integers that I built on, but oh well.  If it ain't broke...
from datetime import datetime as dt
#Add a real comment here
import numpy as np
#Add another real comment here
from git import Repo
import psycopg2
from psycopg2.extensions import AsIs
import psycopg2.extras
from datetime import datetime
import csv
from csv import reader
import shutil
from git import rmtree
import numpy

def close_all_connections_to_db():
	connstring="host=localhost dbname=postgres user=postgres password=boquet"
	conn=psycopg2.connect(connstring)
	cursor=conn.cursor()
	sql1 ="""SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'western_pa_climate_db'
  AND pid <> pg_backend_pid();"""
	cursor.execute(sql1)
	print('\nAll connections closed!')

close_all_connections_to_db()