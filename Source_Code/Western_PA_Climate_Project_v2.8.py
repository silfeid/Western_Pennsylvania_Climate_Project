# -*- coding: utf-8 -*-
"""
Created on Fri December 10 20:55:41 2021

@author: Adam Brode (brodeam@gmail.com) 

"""
#os.path will be used to pull files from a specified directory according to certain criteria, and to create directories for storing saved plots based on the names of the dataframes plotted.
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

#The auto-layout feature should prevent some text etc. from being cut-off; I figured better safe than sorry, and it seems to at least not have caused any problems.
rcParams.update({'figure.autolayout': True})

#Deletes the ""raw" .csv files upon exiting the program; they've been fed into the database already, and so aren't needed any longer, and this allows us to re-use the same directory when running the program next time; we can download all the csv's from Github again, and any that are new will be added to the db, while ones already in the db will be passed over.
def func_quit():
    print('\nThanks for using the Western PA Winter Weather Plotter - Have a nice day!')
    close_all_connections_to_db()
    sys.exit()
    
def set_initial_dates():

    if os.path.isfile('C:/Users/brode/Python/Western_PA_Climate_Project/Misc/ending_date.txt') is False:
        with open('C:/Users/brode/Python/Western_PA_Climate_Project/Misc/ending_date.txt', 'w') as end_date:
            end_date.write('12/31/2013')
            end_date.close()
        
    if os.path.isfile('C:/Users/brode/Python/Western_PA_Climate_Project/Misc/starting_date.txt') is False:
        with open('C:/Users/brode/Python/Western_PA_Climate_Project/Misc/starting_date.txt', 'w') as start_date:
            start_date.write('1/1/1970')
            start_date.close()
    
def make_project_directories():
    
        if os.path.isdir('C:/Users/brode/Python/Western_PA_Climate_Project') is False:
            os.mkdir('C:/Users/brode/Python/Western_PA_Climate_Project')

        if os.path.isdir('C:/Users/brode/Python/Western_PA_Climate_Project/Misc') is False:
            os.mkdir('C:/Users/brode/Python/Western_PA_Climate_Project/Misc')

        if os.path.isdir('C:/Users/brode/Python/Western_PA_Climate_Project/Plots') is False:
            os.mkdir('C:/Users/brode/Python/Western_PA_Climate_Project/Plots')

        if os.path.isdir('C:/Users/brode/Python/Western_PA_Climate_Project/Plots/Single_Stations') is False:
            os.mkdir('C:/Users/brode/Python/Western_PA_Climate_Project/Plots/Single_Stations')

        if os.path.isdir('C:/Users/brode/Python/Western_PA_Climate_Project/Plots/All_Stations') is False:
            os.mkdir('C:/Users/brode/Python/Western_PA_Climate_Project/Plots/All_Stations')
            
        if os.path.isdir('C:/Users/brode/Python/Western_PA_Climate_Project/Plots/All_Stations/Means_Comparisons') is False:
            os.mkdir('C:/Users/brode/Python/Western_PA_Climate_Project/Plots/All_Stations/Means_Comparisons')            

        if os.path.isdir('C:/Users/brode/Python/Western_PA_Climate_Project/Plots/Correlation_Plots') is False:
            os.mkdir('C:/Users/brode/Python/Western_PA_Climate_Project/Plots/Correlation_Plots')            

        if os.path.isdir('C:/Users/brode/Python/Western_PA_Climate_Project/Summary_Stats') is False:
            os.mkdir('C:/Users/brode/Python/Western_PA_Climate_Project/Summary_Stats')

def build_database():
        download_files()
        with open('C:/Users/brode/Python/Western_PA_Climate_Project/record_counter.txt', 'w') as record_counter:
            record_counter.write('0')
            record_counter.close()
        import_stations_from_csv()
        import_observations_from_csv()

def download_files():
        
        if os.path.isdir('C:/Users/brode/Python/Western_PA_Climate_Project/Raw_CSVs') is False:
                Repo.clone_from('https://github.com/silfeid/Western_PA_Climate_Project_Raw_Data.git', 'C:/Users/brode/Python/Western_PA_Climate_Project/Raw_CSVs')
                print('Climate Data in .csv format successfully downloaded!')
        
        else:
                print('\nRepository of raw climate data files in csv form already exists.')
                
def list_comparer(first_list, second_list):
    # here l1 and l2 must be lists
    if len(first_list) != len(second_list):
        return False
    first_list.sort()
    second_list.sort()
    if first_list == second_list:
        return True
    else:
        return False
    
def check_for_new_files():
    #This is the directory that we created automatically when we cloned the Git repo earlier.
    directory = 'C:/Users/brode/Python/Western_PA_Climate_Project/Raw_CSVs/'
    #We make a list of out of its contents using the os module
    first_file_list = os.listdir(directory)
    rmtree(directory)
    Repo.clone_from('https://github.com/silfeid/Western_PA_Climate_Project_Raw_Data.git', 'C:/Users/brode/Python/Western_PA_Climate_Project/Raw_CSVs')
    second_file_list = os.listdir(directory)
    same_or_no = list_comparer(first_file_list, second_file_list)
    return same_or_no
#connstring is saved to file, because we're going to need to change it once we create the db; we need to start off with a dbname that we know will exist (postgres); once we've created the western_pa_climate_db, we will write that new db name to file and read it back into Python as the connection string.

#We want to query postgres and see if the database already exists or not; I looked up the code for how to do this (obviously), but I think it's probably a useful trick to have in one's bag.
def check_db_exists():
        init_connstring="host=localhost dbname=postgres user=postgres password=boquet"
        conn=psycopg2.connect(init_connstring)
        conn.set_session(autocommit=True)
        cursor=conn.cursor()
        #The query to check if the db under the given name (this should not vary, given that its creation was automated) already exists or not.
        sql1 ="""SELECT datname FROM pg_catalog.pg_database WHERE datname = 'western_pa_climate_db'"""
        cursor.execute(sql1)
        row=cursor.fetchone();
        #If there was no such db, the results should equal None; if that's the case, we'll create the db.
        if row==None:
                print("\nNo such database - creating!")
                db_query = 0
                sql3 = """CREATE DATABASE western_pa_climate_db"""
                cursor.execute(sql3)
                cursor.close()
                conn.close()
                
                #We created the db, and since we'll want to connect directly to that db in the future, our connstring has changed.  Although we could just feed it in as a string, since we haven't given the user any choice as to the name of the db, it seems like best practice would be to write it to a .txt file, to be read by Python whenever we want to connect to the db.  That way, if we wanted to allow the user to change the db name later on, they could without messing up a mess'o'code.
                with open('C:/Users/brode/Python/Western_PA_Climate_Project/connstring.txt', 'w') as writer:
                        writer.write("host=localhost dbname=western_pa_climate_db user=postgres password=boquet")
                        writer.close()        
                #Now we use it right away again, to make sure that we're connected to the db we want to do our work in.
                connection = open('C:/Users/brode/Python/Western_PA_Climate_Project/connstring.txt', 'r')
                connstring = connection.readline()
                conn=psycopg2.connect(connstring)
                cursor=conn.cursor()
                #This will be the parent table for the rest of the db; we know that we'll need this table whether we have one or one thousand child tables, so we're going to go ahead and make it right off the bat.
                sql5 = """CREATE TABLE stations (
        station_code VARCHAR ( 50 ) PRIMARY KEY,
        station_name VARCHAR ( 50 ) NOT NULL,
        latitude NUMERIC ( 7,4 ) NOT NULL,
        longitude NUMERIC ( 7,4 ) NOT NULL,
        elevation NUMERIC ( 5,1 ) NOT NULL
);"""
                cursor.execute(sql5)
                conn.commit()
                #And here we're going to create the procedure by which we'll add stations to the stations table; they get lifted from the .csv files we downloaded earlier.  Since we only need to lift one record's worth of data from each .csv to get ALL of the required station information, it seems easiest to just use a procedure to create the new station records.  For the actual observations (see below), we might have used a procedure, but this didn't seem much easier/more efficient to me, since we have to iterate over every single record in order to add them to the db anyway; just having the built-in csv reader iterate over each line in sequence seemed just as effective.
                sql6 = """CREATE PROCEDURE add_station(
        stationcode VARCHAR(50),
        stationname VARCHAR(50),
        stationlatitude numeric(6,4),
        stationlongitude numeric(6,4),
        stationelevation numeric(7,1)
)
AS $$

BEGIN

        INSERT INTO stations(station_code, station_name, latitude, longitude, elevation) VALUES (stationcode, stationname, stationlatitude, stationlongitude, stationelevation) ON CONFLICT DO NOTHING;  

END; $$
language plpgsql;"""
                cursor.execute(sql6)
                conn.commit()
                #We have our sole parent table and the procedure we use to add to it, so we're ready to go - just need to add some child tables, which we'll get to soon.
                print('Database created!')
                
        else:
                #Row didn't equal none, which means our query to pg_catalog.pg_database returned a result, meaning the db must already exist.  So we let our user know that's the case and move on.
                print('\nDatabase found!')
                db_query = 1
        cursor.execute(sql1)
        row=cursor.fetchone();
        cursor.close
        conn.close
        db_name = str(row)
        db_name = db_name.strip('(')
        db_name = db_name.strip(')')
        db_name = db_name.strip(',')
        db_name = db_name.strip('\'')
        print('Database name: '+db_name)
        return db_query

#Simpler to use a function to grab our written-to-.txt connstring, instead of repeating the code again and again.
def get_connstring():
                connection = open('C:/Users/brode/Python/Western_PA_Climate_Project/connstring.txt', 'r')
                connstring = connection.readline()
                return connstring

#This function will use the add_station procedure we created when making our db and use it to iterate over all the .csv files we downloaded, adding the station information from each into the stations table.
def import_stations_from_csv():
        #print a new line just for aesthetics etc.
        print()
        connstring = get_connstring()
        conn=psycopg2.connect(connstring)
        cur=conn.cursor()

        #We use some handy built-in features of the os module to iterate over all the files we downloaded earlier and do stuff with 'em.
        directory = 'C:/Users/brode/Python/Western_PA_Climate_Project/Raw_CSVs/'
        for filename in os.listdir(directory):
                if filename.endswith('.csv'):
                        with open(directory+filename, 'r') as read_obj:
                                csv_reader = reader(read_obj)
                                header=next(csv_reader)
                                row=next(csv_reader)
                                station_code=row[0]
                                if station_code=='':
                                        station_code=None
                                station_name=row[1]
                                if station_name=='':
                                        station_name=None
                                station_latitude=row[2]
                                if station_latitude=='':
                                        station_latitude=None
                                station_longitude=row[3]
                                if station_longitude=='':
                                        station_longitude=None
                                station_elevation=row[4]
                                if station_elevation=='':
                                        station_elevation=None
                                #The station code will be the primary key, since it already is one for NOAA (each is unique); the station names are functionally unique too, but all the spaces make things icky, even if they are more user-friendly/memorable.  We'll take some steps later to prevent our users from having to interact with the station codes directly later on.  We can also reliably assume that if station code = none, we don't want to add any such data into our db anyway, 'cause something is messed up with that data.'
                                if station_code!=None:
                                        print("Read: " + station_code.ljust(20, ' ') + station_name.ljust(10, ' ') + station_latitude.ljust(10, ' ') + station_longitude.ljust(10, ' ') + station_elevation.ljust(10, ' '))        
                                        #And we use our handy-dandy stored procedure to add the non-None station into our db.  I don't know why there isn't a problem, but it doesn't matter how many times we try to add the same record to the stations, table, if it's already present, duplicates aren't created (I'm guessing due to the primary key situation there).  INSERT INTO...WHERE NOT EXISTS probably would have been the correct syntax to avoid any potential issues, but I've sort of run out of time, so, ain't broke, don't fix, etc. for now.
                                        cur.execute('CALL add_station(%s, %s, %s, %s, %s)', (station_code, station_name, station_latitude, station_longitude, station_elevation));
        conn.commit()
        cur.close()
        conn.close

#I created this function so that I could prevent the program from needlessly iterating over thousands and thousands of records that are already present in the db, mostly because this takes a while and is annoying for the user.  The program instead checks for all the table names in the db, and makes a list of them, which the function returns.  Later on we'll check the names of the .csv files we're reading into our db against that list, and if a file name is already present as a table (the station code), we'll just skip it.  If the user needs to update or add new values to an existing table, they have to use a separate function to do that.
def grab_db_table_list():
        connstring = get_connstring()
        conn=psycopg2.connect(connstring)
        cur=conn.cursor()
        #The table names all start with one of those sets of three letters, so probably anything along those lines will be relevant enough.  Imprecise, but I'm not sure how to do better for now.
        sql = """SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'usc%' OR table_name LIKE 'usw%';"""
        cur.execute(sql)
        row=cur.fetchone();
        table_list=[]
        while row is not None:
                row=str(row)
                row=row.replace('(','')
                row=row.replace(')','')
                row=row.replace('\'','')
                row=row.replace('\'','')
                row=row.replace(',','')
                row=row.upper()
                table_list.append(row)
                row=cur.fetchone()
        return table_list

def build_station_dict():
    
    station_dict = {}
    connstring = get_connstring()
    conn=psycopg2.connect(connstring)
    cur=conn.cursor()
    sql = """SELECT station_code, station_name FROM stations ORDER BY station_name;"""
    cur.execute(sql)
    row=cur.fetchone();
    while row is not None:        
        code = str(row[0])
        name = str(row[1])
        name = station_name_fixer(name)
        station_dict[code]=name        
        row=cur.fetchone()
    return station_dict
    
def station_name_fixer(name):

    for character in name:
            if character.isdigit():
                name=name.split(character)
                name=name[0]
            else:
                pass
            name=name.split(',')
            name=name[0]
            name=name.rstrip(' ')
    return name

#This function takes the relevant fields from the csv's and uses them to create observation tables; the ob tables drop most of the station data, only retaining the station code (so that any observation record can be linked to the stations table as needed).        
def import_observations_from_csv():
        print()

        connstring = get_connstring()
        conn=psycopg2.connect(connstring)
        cur=conn.cursor()
        
        #This function is detailed below; it returns a list of all the tables in the db, which we'll use to check against all the .csv files we have to work with (see below)
        table_list = grab_db_table_list()
        
        #This is the directory that we created automatically when we cloned the Git repo earlier.
        directory = 'C:/Users/brode/Python/Western_PA_Climate_Project/Raw_CSVs/'
        #We make a list of out of its contents using the os module
        file_list = os.listdir(directory)
        
        #Iterate through them to assess each, adding or skipping it as needed.
        for filename in file_list:
                #We're going to use this counter to keep track of how many records are being added to each table as it's created, just to let our user know.
                counter = 0
                #If it doesn't end with .csv, we don't care about it, so that filtration is easy enough.
                if filename.endswith('.csv'):
                        with open(directory+filename, 'r') as read_obj:
                                csv_reader = reader(read_obj)
                                header=next(csv_reader)
                                row = next(csv_reader)
                                #We grab the station code from the first record; this is the same for every record in the .csv, so we really only care about the first one, the others being entirely redundant for this purpose.
                                station_code=row[0]
                                #The tables that already exist in the db are listed in 'table_list'; since we're going to presume that only this program would have been used to create them, we can also assume that if they've been read in once, they don't need to be read in twice.  So if the station_code variable is congruent with an item in table_list, we can skip the .csv that contained it.
                                if station_code not in table_list:
                                
                                        sql1="""SELECT * FROM stations WHERE station_code=%s"""
                                        cur.execute(sql1, (station_code,))
                                        row=cur.fetchone();
                                        #There was no such station listed in the stations table, so we can't add it to our db yet, therefore we skip it for now.
                                        if row==None:
                                                print("Station not listed in stations table; skipping.")
                                                file_list.remove(filename)
                                                
                                        #If there is a record in the stations table, then we can go ahead and create a table with the same station code.  If somehow there was a problem with the prior step and it already existed, the IF NOT EXISTS syntax prevents a duplicate from being created, although we'd have some other troubleshooting to perform in that instance.  Note that since we do have null values occuring with lamentable frequency in our data set, we do need to permit nulls for most of the observation fields.
                                        else:
                                                cur.execute("""CREATE TABLE IF NOT EXISTS """+station_code+"""(
                                                                ob_number smallint GENERATED ALWAYS AS IDENTITY UNIQUE,
                                                                station_code VARCHAR(50) REFERENCES stations (station_code) NOT NULL,
                                                                ob_date Date NOT NULL,
                                                                snow_fall numeric(5,2) NULL,
                                                                snow_depth numeric (5,2) NULL,
                                                                temp_max numeric (5,2) NULL,
                                                                temp_min numeric (5,2) NULL);""");
                                                conn.commit()
                                                print('\nTable created for station '+station_code+';')
                                                
                                                #We use the seek functionality to return to the first line of our .csv with the reader.
                                                read_obj.seek(0)
                                                #And next to advance past the header.
                                                row = next(csv_reader)
                                                #Making sure that Python will pass None value to psql for any nulls we get.
                                                for row in csv_reader:
                                                        station_code=row[0]
                                                        if station_code=='':
                                                                station_code=None
                                                        ob_date=row[5]
                                                        if ob_date=='':
                                                                ob_date=None
                                                        snow_fall=row[6]
                                                        if snow_fall=='':
                                                                snow_fall=None
                                                        snowdepth=row[7]
                                                        if snowdepth=='':
                                                                snowdepth=None
                                                        tempmax=row[8]
                                                        if tempmax=='':
                                                                tempmax=None
                                                        tempmin=row[9]
                                                        if tempmin=='':
                                                                tempmin=None
                                                        #If station_code is null, we can't perform this step, so we'd stop there; upon verifying that it's not null, we decide to proceed (although ob_date being null would also be a real problem - I just happen to know that there are no such problems with this data set through having worked with it before)
                                                        if station_code!=None:
                                                                #And we use a simple insert into statement to add each record into our newly-created table; using the ob_date and WHERE NOT EXISTS to prevent any duplicate records, just for prudence's sake.
                                                                cur.execute("""INSERT INTO """+station_code+"""(station_code, ob_date, snow_fall, snow_depth, temp_max, temp_min) SELECT %s, %s, %s, %s, %s, %s WHERE NOT EXISTS (SELECT ob_date FROM """+station_code+""" WHERE ob_date = %s);""",(station_code, ob_date, snow_fall, snowdepth, tempmax, tempmin, ob_date));
                                                                #Upon successful execution of the last statement, we added a record to our table, so now we'll bump our counter up by one to keep tally of the total number of records added to the table.
                                                                counter += 1
                                                #And here were use the counter to print the number of records added to the table, which is also named, for our user's benefit.
                                                '''Redo this so there's just an embedded function to write the counter tally to file'''
                                                
                                                print(str(counter)+' records added to Table '+station_code+'.')
                                                with open('C:/Users/brode/Python/Western_PA_Climate_Project/record_counter.txt', 'r') as record_counter:
                                                    records_count = record_counter.read()
                                                    records_count = int(records_count)
                                                    records_count = records_count+counter
                                                    record_counter.close()
                                                    
                                                with open('C:/Users/brode/Python/Western_PA_Climate_Project/record_counter.txt', 'w') as record_counter:
                                                    records_count = str(records_count)
                                                    record_counter.write(records_count)
        
        #Honestly not sure if we need the commit for these operations, I think perhaps not?  But I've left it in out of an abundance of caution/declining to have tested it without.
        conn.commit()
        cur.close()
        conn.close

def get_record_count():
    with open('C:/Users/brode/Python/Western_PA_Climate_Project/record_counter.txt', 'r') as record_counter:
        records_count = record_counter.read()
    
    records_count = int(records_count)
    data_points = records_count*4
    records_count = "{:,}".format(records_count)
    data_points = '{:,}'.format(data_points)
    records_count = str(records_count)
    data_points = str(data_points)
    
    stations_number, station_dict = get_station_list()
    stations_number = str(stations_number)

    print('There are '+records_count+' records in the database; each record consists of 4 unique elements, meaning that there are '+data_points+' unique data points in the database. These records are drawn from '+stations_number+' distinct weather stations across Pennsylvania and West Virginia.')

def get_station_list():
    
    station_dict = build_station_dict() 
    stations_number = str(len(station_dict))    
    return stations_number, station_dict

#Pretty self-explanatory, basically just using the description function built into psycopg2 and a list comprehension to build a list of column names.
def get_column_names(table_name):
        connection = open('C:/Users/brode/Python/Western_PA_Climate_Project/connstring.txt', 'r')
        connstring = connection.readline()
        conn=psycopg2.connect(connstring)
        cur=conn.cursor()
        cur.execute("Select * FROM "+table_name+" LIMIT 0")
        column_names = [desc[0] for desc in cur.description]
        return column_names

def valiDate(date_string):
        format = "%m/%d/%Y"
        while True:
                try:
                        test_str = input(date_string)
                        result = bool(datetime.strptime(test_str, format))
                except ValueError:
                        result = False
                else:
                        return test_str

def check_on_dfs():
    df_dict = build_df_dict()
    
    for key in df_dict.keys():
        print(key)
    for value in df_dict.values():
        print(value.describe())

def build_df_dict():
    
    #An empty dictionary, to be populated with station names as keys and dataframes as values.
    df_dict = {}
    
    table_list = grab_db_table_list()
    
    connstring = get_connstring()
    conn=psycopg2.connect(connstring)
    
    for table_name in table_list:

        sql_query = pd.read_sql_query ('''
                                   SELECT
                                   *
                                   FROM '''+table_name+'''
                                   ''', conn)
    
        df = pd.DataFrame(sql_query, columns = ['station_code', 'ob_date', 'snow_fall', 'snow_depth', 'temp_max', 'temp_min'])
        
        df.set_index('ob_date', inplace = True)
    
        df_dict[table_name] = df
    
    #The next bit of code uses the os module to create new directories at a specified location (within parent_dir), one for each station location identified above.        
    parent_dir = 'C:/Users/brode/Python/Western_PA_Climate_Project/Plots/Single_Stations/'
    
    
    station_dict = build_station_dict()
    station_names = station_dict.values()
    
    for station_name in station_names:
        #the filepath for the new directory = the parentdir + the station name
        path = os.path.join(parent_dir, station_name)
        #Check to make sure that directory doesn't already exist; if it does, we simply "pass" ;this way new stations can be added to the database/program without creating a headache.
        if os.path.isdir(path) is False:
            #os.mkdir creates a directory at the specified filepath
            os.mkdir(path)
        else:
            pass
    #Return the df_dict, which we'll use throughout the rest of the program
    
    return df_dict

#The integer checker just checks input from the user and as you've already guessed, makes sure that it's an integer; if it's not, a message is displayed and the user is prompted once more. At this stage, any integer will do.
def integer_checker(slicer_datum):
  while True:
    try:
       user_input = int(input(slicer_datum))       
    except ValueError:
       print("\nInput must be an integer. Try again.")
       continue
    else:
       return user_input 
       break 
#Here, we'll use the integer checker to solicit a start date from the user.  Once we get an integer, we can perform the necessary checks to make sure that it's a valid month, valid day for that month, and valid year.
def get_start_date():
    
    print('Pick the date range for which you wish to plot data.  The first available date is January 1, 1970; the last available date is December 31, 2013.')
    #Run integer checker to get our user input.
    start_month = integer_checker('Select starting month: ')
    #Should be pretty clear; needs to be an integer that corresponds to a month.
    if start_month > 0 and start_month < 13:
        pass
    else:
        print('\nMust be integer 1-12. Try again.')
        start_month = integer_checker('Select starting month: ')
    
    #Now that the necessary operations have been performed, we convert the start_month to a string so that it can be concatenated and written to a .txt file as needed.
    start_month = str(start_month)
    
    #Pretty similar operations are performed on the start_day variable, as below, mutatis mutandis etc. etc.
    start_day = integer_checker('Select starting day: ')

    thirtiers = ['9', '4', '6', '11']
    
    if start_month in thirtiers:
        while start_day > 30 and start_day <32:
            print('\nDay does not match to month')
            start_day = integer_checker('Select starting day: ')
            
    if start_month == '2':
        print(start_month)
        while start_day > 28:
            print('\nDay does not match to month; leap days are excluded from this program.')
            start_day = integer_checker('Select starting day: ')
               
    else:
        while start_day > 31 or start_day < 0:
            print('\nNo month has that number of days in it.')
            start_day = integer_checker('Select starting day: ')

    start_day = str(start_day)
    
    #And grab the start year; here, we just make sure it's not before 1970 or after 2013.        
    start_year = integer_checker('Select starting year: ')
    
    if start_year < 1970 or start_year > 2013:
        print('\nYear must be between 1970 and 2013, inclusive. Try again.')
        start_year = integer_checker('Select starting year: ')
    
    start_year = str(start_year)
    
    #We concatenate the start month, day, and year to get our start_date, adding the slashes as needed...
    start_date = start_year+'/'+start_month+'/'+start_day
    #And write that date to a .txt file, so that it can be summoned by the program even after restarting (the last used date, i.e. the date last written to the .txt file, is the default start date used by the program; same goes for the end date, below.)
    start_file = open('Western_PA_Climate_Project/Misc/starting_date.txt', 'w')
    start_file.write(start_date)
    start_file.close()
    
    print('\nStarting date successfully chosen.  Next, pick an ending date.')

#This function works almost identically to the get_start_date() function above, so only those aspects which differ between the two are commented on below.
def get_end_date():
    
    #Read the start_date in from the .txt file, since we need to compare it to the end date to make sure that the latter doesn't come before the former, chronologically
    start_file = open('Western_PA_Climate_Project/Misc/starting_date.txt', 'r')
    start_date = start_file.read()
    #And here we use the datetime module and its strptime function to parse a string as a datetime object (here, just a date, as specified by the .date() tag at the end the function).
    check_start_date = dt.strptime(start_date, '%Y/%m/%d').date()
    
    end_month = integer_checker('Select ending month: ')
    if end_month > 0 and end_month < 13:
        pass
    else:
        print('\nMust be integer 1-12. Try again.')
        end_month = integer_checker('Select ending month: ')
    
    end_month = str(end_month)

    end_day = integer_checker('Select ending day: ')

    thirtiers = ['9', '4', '6', '11']
    
    if end_month in thirtiers:
        while end_day > 30 and end_day <32:
            print('\nDay does not match to month')
            end_day = integer_checker('Select ending day: ')
            
    if end_month == '2':
        print(end_month)
        while end_day > 28:
            print('\nDay does not match to month; leap days are excluded from this program.')
            end_day = integer_checker('Select ending day: ')
               
    else:
        while end_day > 31 or end_day < 0:
            print('\nNo month has that number of days in it.')
            end_day = integer_checker('Select ending day: ')

    end_day = str(end_day)
            
    end_year = integer_checker('Select ending year: ')
    
    if end_year < 1970 or end_year > 2013:
        print('\nYear must be between 1970 and 2013, inclusive. Try again.')
        end_year = integer_checker('Select ending year: ')
        
    end_year = str(end_year)
    
    end_date = end_year+'/'+end_month+'/'+end_day
    #Here we turn the end_date into a datetime object as well...
    check_end_date = dt.strptime(end_date, '%Y/%m/%d').date()
    #...And use a simple comparison operator to determine that it does indeed come after the start date and not before.  If it didn't, we'd just start the get_end_date function over after displaying a message telling the user what they did wrong.
    if check_end_date < check_start_date:
        print('\nEnd date must come after start date!')
        get_end_date()
    
    else:
        
        end_file = open('Western_PA_Climate_Project/Misc/ending_date.txt', 'w')
        end_file.write(end_date)
        end_file.close()
        
        print('\nEnding date successfully chosen.')

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

#This function retrieves the starting and ending dates saved to .txt via their respective functions.  Using this method allows us to simply retrieve the last dates used if the user doesn't wish to enter new ones, giving a default set of dates.
def build_slicer_date():
    
   start_file = open('C:/Users/brode/Python/Western_PA_Climate_Project/Misc/starting_date.txt', 'r')
   start_date = start_file.read()
   
   end_file = open('C:/Users/brode/Python/Western_PA_Climate_Project/Misc/ending_date.txt', 'r')
   end_date = end_file.read()
   
   return start_date, end_date

#This wee function simply prints the current start and end dates selected in case the user has forgotten them or is using the program for the first time (for them).
def show_slicer_dates():
    start_date, end_date = build_slicer_date()
    print('\nStart date: '+start_date)
    print('End date: '+end_date)

#Having built our dataframes, we now use the start and end dates arrived at above to slice each dataframe by index (which you'll remember we set to the 'ObDate' column earlier), so that each dataframe is truncated to include only those dates (ObDates) which fall in the range specified by the user.   
def slice_dfs():
    #Retrieve the dataframes
    df_dict = build_df_dict()
    #Retrieve the start and end dates.
    start, end = build_slicer_date()
    start = datetime.strptime(start, "%Y/%m/%d")
    start = start.date()
    end = datetime.strptime(end, "%Y/%m/%d")
    end = end.date()
    counter = 1    
    
    #Why is this operation running three times to do a single df plot?
    for key in df_dict.keys():
        #if not df_dict[key].index.empty:
        if start not in df_dict[key].index:
            trunc_start = min(df_dict[key].index)
            if trunc_start < start:
                index_list = df_dict[key].index.tolist()
                for item in index_list:
                    if item < start:
                        pass
                    else:
                        trunc_start = item
                        break
            else:
                trunc_start = min(df_dict[key].index)
            
        else:
            trunc_start = start

        if max(df_dict[key].index) < end:
            trunc_end = max(df_dict[key].index)
            df_dict[key] = df_dict[key].loc[trunc_start:trunc_end]
        else:
            df_dict[key] = df_dict[key].loc[trunc_start:end]
        counter += 1
        
    #We will want the start and end dates later, not to manipulate the df's (that step's done with), but to use as variables in displaying plots and in creating unique filenames for saving.  Since we're not working with any unsliced df's (the user simply picks the first and last available dates if they wish to view data for the entire date range), we'll just replace the old df_dict with this one.
    start = str(start)
    end = str(end)
    
    return df_dict, start, end

def refine_dfs_for_snow_fall():
    df_dict, start, end = slice_dfs()
    del_list = []
    
    for key, df in df_dict.items():
        
        df.index = pd.to_datetime(df.index, format='%Y-%m-%d')
        new_df= df.loc[df.index.month < 4]
        df = df.loc[df.index.month > 11]
        new_df = new_df.append(df)
        df_dict[key]=new_df
        
    for key, value in df_dict.items():
        value = value.dropna(subset=['snow_fall'])
        if len(value.index) < 60:
            del_list.append(key)

    for item in del_list:
        df_dict.pop(item)
    return df_dict, start, end

def refine_dfs_for_snow_depth():
    df_dict, start, end = slice_dfs()
    del_list = []
    
    for key, df in df_dict.items():
        
        df.index = pd.to_datetime(df.index, format='%Y-%m-%d')
        new_df= df.loc[df.index.month < 4]
        df = df.loc[df.index.month > 11]
        new_df = new_df.append(df)
        df_dict[key]=new_df
        
    for key, value in df_dict.items():
        value = value.dropna(subset=['snow_depth'])
        if len(value.index) < 60:
            del_list.append(key)

    for item in del_list:
        df_dict.pop(item)
    return df_dict, start, end

def refine_dfs_for_temp_min():
    df_dict, start, end = slice_dfs()
    del_list = []
        
    for key, value in df_dict.items():
        value = value.dropna(subset=['snow_depth'])
        if len(value.index) < 210:
            del_list.append(key)

    for item in del_list:
        df_dict.pop(item)
        
    return df_dict, start, end

def refine_dfs_for_temp_max():
    df_dict, start, end = slice_dfs()
    del_list = []
        
    for key, value in df_dict.items():
        value = value.dropna(subset=['snow_depth'])
        if len(value.index) < 210:
            del_list.append(key)

    for item in del_list:
        df_dict.pop(item)
    return df_dict, start, end

#This function plots one of the four variables charted in each dataframe, all of them daily: snow_fall, Snow Depth, Minimum Temperature and maximum Temperature, for a single station.  The user will first pick which station they want to view, then which variable.
def single_df_plotter():

    print('Pick a station for which to plot data:\n')
    station_dict = build_station_dict()
    station_names = list(station_dict.values())
    counter = 1
    for station in station_names:
        counter = str(counter)
        print(counter+'. '+station)
        counter = int(counter)
        counter += 1
        
    #The integer checker from above is used to validate user input here as well.
    
    df_name_choice = integer_checker('\nStation Choice (#): ')
                                
    if df_name_choice <= len(station_names):
        df_name_choice = df_name_choice-1
        df_name_choice = int(df_name_choice)

    else:
        print('Not a valid station number. Try again.')
        menu()
    
    station_codes = grab_db_table_list()
    df_choice = station_codes[df_name_choice]
    df_text = station_names[df_name_choice]

    #This probably can be deleted, but just a print statement to confirm to the user which station they've chosen.
    print('\nStation: '+df_choice+' - '+df_text)

    #A dictionary of the variables for plotting, on the model of the one above for stations.
    plot_var_dict = {1:'snow_fall', 2:'snow_depth',3:'temp_min', 4:'temp_max'} 
    
    #Versions of the same variables for 'pretty printing' (display on the plots, saving, etc.)
    print_var_dict = {1:'Daily Snow Fall', 2:'Daily Snow Depth',3:'Minimum Daily Temperature',4:'Maximum Daily Temperature'}
    
    plot_var_choice = integer_checker('Pick a variable for plotting: 1.Daily Snow Fall 2. Daily Snow Depth 3. Minimum Daily Temperature 4. Maximum Daily Temperature Choice: ')
    
    #Same mechanism as above for input validation: boot 'em to the main menu if the integer isn't appropriate...I'm 100% sure there's a better way to do this, but as I mentioned above, restarting the function itself produces unexpected errors, and I'm pretty much out of time at this point...perfect enemy of good mumble mumble...

    if plot_var_choice in plot_var_dict:
        pass
        
    else:
        print('Choice not valid.  Returning to main menu.')
        menu()
    
    #We'll turn that int to a string and use it grab the correct print variable from the print variable dictionary we created a few lines above.  Just so it'll look all nice later on.

    print_variable = print_var_dict[plot_var_choice]
    
    #Picking what units to display on the y axis of the plot (below); since choices one and two for plot_var are measured in inches, and 3 and 4 are measured in degrees Fahrenheit, the if clause here is pretty simple.
    if int(plot_var_choice) < 3:
        print_unit = ' (in)'
    else:
        print_unit = ' (°F)'
    
    #Use our plot variable choice to grab the right column name from the plot var dictionary we created at the start of the function code.
    plot_variable = plot_var_dict[plot_var_choice]
    
    if plot_var_choice == 1:
        color='skyblue'
        df_dict, start, end = refine_dfs_for_snow_fall()
    if plot_var_choice == 2:
        color='steelblue'
        df_dict, start, end = refine_dfs_for_snow_depth()
    if plot_var_choice == 3:
        color='blue'
        df_dict, start, end = refine_dfs_for_temp_min()
    if plot_var_choice == 4:
        color='red'        
        df_dict, start, end = refine_dfs_for_temp_max()

    #We use the plot variable uh...variable (a string) to select the appropriate column from our chosen dataframe and plot it using Pandas; if we ask Pandas to plot a single column, i.e., a Series, it will automatically use the index as the x-axis and the values in the series as the y-axis, handily setting the ticks and scale all by itself.  Below I've used the variables that I created above to assign the plot a title containing relevant information and labels for the x and y axes.  Ms stands for marker size, and figsize is x*y dimensions.
    
    if df_choice not in df_dict.keys():
        print('\nInsufficient data points for that station in that time frame; returning to station choice menu.\n')
        single_df_plotter()
    
    else:
    
        var_stat_plot = df_dict[df_choice][plot_variable].plot(title=df_text+' '+print_variable+' '+start+'-'+end, xlabel='Year', ylabel=print_variable+print_unit, style='.', ms=6, figsize = (8, 4), c=color)
        
        #The date variables have to be modified in order to be used in the filename (we're going to save it to the directory corresponding to the station name that we created way back at the start) because Windows quite sensibly won't allow filenames with /.  We could have avoided the use of slashes in the dates entirely, of course, but the .csv files came in that format, and frankly that's just how Americans write the date - I prefer '24.XII.2021' for Christmas Eve, e.g., myself, but I'm just a voice in the wilderness...
        start = start.replace('/', '.')
        end = end.replace('/', '.')
    
        #Retrieve the figure we created above in order to save it; I frankly don't know why it has to be done this way, but I believe that it does, at least if you don't want the figure to be displayed first, which I don't - I find that the display area in the console is too small for convenient viewing, and that the display of the plots clutters the display and distracts the user from the efficient retrieval of the plots.  My program saves all the plots to directories created for the purpose, to be viewed and compared later.  They're intuitively named, and the program informs the user of the save locations upon successful export as well.
        
        fig = var_stat_plot.get_figure()
        #The filepath for saving uses the df_text variable to get into the final directory, then creates a unique name for the plot using the station name, variable plotted, and date range used.  If someone entered the same variables (station, plot variable, and dates), the plot would be overwritten, but only with the exact same plot, so, no harm done.
        
        fig.savefig('C:/Users/brode/Python/Western_PA_Climate_Project/Plots/Single_Stations/'+df_text+'/'+df_text+'_'+print_variable+'_'+start+'-'+end+'.jpg')
    
        plt.close(fig)
        print('\nPlot successfully exported to \'Plots/Single_Stations/'+df_text+'\'')   

#This function graphs the mean for each weather variable for each station for the specified date range, and also averages the average for all stations, graphing that as well.

def refine_for_descriptive_stats_grapher(user_choice):  
    
    user_choices = {1:'snow_fall', 2:'snow_depth', 3:'temp_min', 4:'temp_max'}
    
    if user_choice in user_choices.keys():
        user_choice_var = user_choices[user_choice]
        if user_choice == 1:
            df_dict, start, end = refine_dfs_for_snow_fall()
    
        if user_choice == 2:
            df_dict, start, end = refine_dfs_for_snow_depth()
            
        if user_choice == 3:
            df_dict, start, end = refine_dfs_for_temp_min()

        if user_choice == 4:
            df_dict, start, end = refine_dfs_for_temp_max()

    else:
        print('Invalid choice, returning to main menu')    
        menu()        
    
    start = str(start)
    end = str(end)
    #turn the keys of the df_dict into a list that we can iterate over; don't necessarily have to cast it as a list, but seems a bit safer...
    df_keys =list(df_dict.keys())
    
    #This iterates over the whole dictionary of dataframes and replaces each with the (vastly smaller) dataframe that one gets from calling the df.describe() function; this returns means, standard deviations, counts, and the like.
    for key in df_keys:
        df_dict[key] = df_dict[key].describe()

    #For now, we don't care about anything in the described dfs other than the means for each weather variable, so we're going to redefine each value in the df_dict(ionary) as the Pandas Series we get when we use .loc['mean']; just to reiterate, what we now have in df_dict is a dictionary in which the keys are the station names (14 as of the time of writing) and the values are now just a series of 4 numbers, each the mean of a different weather variable (snow_fall, Snowdepth, TempMin and TempMax, in that order).
    for key in df_keys:
        df_dict[key] = pd.Series(df_dict[key].loc['mean'])
    
    #Drop null values from the series in our dictionary    
    for key in df_keys:
        df_dict[key] = df_dict[key].dropna()
        #If dropping nulls results in an empty series, then we get rid of that key/value pair entirely; not doing so would create problems later, especially in our calculation of the average of all stations.
        if df_dict[key].empty is True:
            del df_dict[key]
    
    #Same exact code as above, but we have to run it again to exclude the entries that may have been deleted when we dropped all our empty series.    
    df_keys = list(df_dict.keys())
        
    #Now we make a new set of two dictionaries, one for each of the temperature variables, since these don't need to drop non-winter months; each will be loaded up with one key/value pair for each station left in df_keys, the key being the station name and the value just being a number, whatever the mean for that weather variable at that station was.
    user_choice_dict = {}
    
    for key in df_keys:
        if key in df_dict:
            try:
                user_choice_dict[key] = float(df_dict[key].loc[user_choice_var])   
            except KeyError:
                pass
            
    return user_choice_dict, start, end

def descriptive_stats_grapher():

    av_var_choice = integer_checker('Pick a variable and to see the graph of its average for each station.  Note that some stations\' reporting period is longer or shorter than others, and each station\'s records may have gaps, so a comparison of means may be unequal. If a station does not appear in the plot, that means that there was no data for that station during the specified time frame.\n\n1. Average Daily Snowfall\n2. Average Daily Snow Depth\n3. Average Daily Minimum Temperature\n4. Average Daily Maximum Temperature\n\nChoice: ')
    av_var_choice = int(av_var_choice)
    valid_av_var_choices = [1, 2, 3, 4]
    
    if av_var_choice in valid_av_var_choices:
        
        if av_var_choice == 1:
            display_var = 'Snow Fall'
            display_color = 'skyblue'
            display_units = ' (inches)'
        if av_var_choice == 2:
            display_var = 'Snow Depth'
            display_color = 'plum'
            display_units = ' (inches)'
        if av_var_choice == 3:
            display_var = 'Minimum Temperature'
            display_color = 'powderblue'
            display_units = ' (°F)'
        if av_var_choice == 4:
            display_var = 'Maximum Temperature'
            display_color = 'red'
            display_units = ' (°F)'
        
        av_var_chosen, start, end = refine_for_descriptive_stats_grapher(av_var_choice)
        
        graph_values = []
        graph_keys = []
    
        #The floats were pretty big, so we'll round them, then append them to the values list we made just above.
        for value in av_var_chosen.values():
            value = round(value, 1)
            graph_values.append(value)
        
        #Calculate the average of all stations...    
        mean_av_var_chosen = (sum(graph_values))/(len(graph_values))
        #I believe that it wouldn't let me do this in one line, for whatever reason.
        mean_av_var_chosen = round(mean_av_var_chosen, 1)
        #Stick the average of all stations onto the list as the final value
        graph_values.append(mean_av_var_chosen)
         
        #Populate the keys list for the x-axis
  
        for key in av_var_chosen.keys():
            graph_keys.append(key)                 
        #And we'll add in the All Stations Average element to correspond to the average that we stuck into the values list above.    
        graph_keys.append('ALL STATIONS AVERAGE')
        
        station_dict= build_station_dict()
        station_list = list(station_dict.keys())
        matches = []
        
        for code in station_list:
            if code in graph_keys:
                matches.append(code)
        
        used_station_names = []    
        for key in matches:
            used_station_names.append(station_dict[key])
                
        used_station_names.append('ALL STATIONS AVERAGE')
        #And now we build our bar graph, listing x, then y, and picking a color.                  
        plt.bar(used_station_names, graph_values, color = display_color)
        #Just tweaking the xticks.
        plt.xticks(rotation =60, fontsize = 8, ha = 'right')
        #We'll give it a title...
        plt.title('Average '+display_var+', All Stations '+start+'-'+end, fontweight = 'bold', fontsize = 11, loc='center')
        #Define the margins and set the layout to tight to keep things neat
        plt.margins(0.05)
        #plt.tight_layout()
        
        #This for loop displays the value of each bar above it; optional, but a nice touch, I think.
        for item in range(len(graph_values)):
            plt.text(item, graph_values[item], graph_values[item], ha='center', rotation=0, verticalalignment='center_baseline', fontsize = 8)
            
        #Set our labels for the x and y axes; pretty straightforward.    
        plt.xlabel('Station', fontweight='bold', color = 'black', fontsize='10')
        plt.ylabel(display_var+display_units, fontweight='bold', color = 'black', fontsize='10', loc='top')
   
        
        #Here again, we fix our start and end dates so that they can be used in the ultimate filepath of the plot.
        start = start.replace('/', '.')
        end = end.replace('/', '.')            
        
        #And then we save the plot...
        plt.savefig('C:/Users/brode/Python/Western_PA_Climate_Project/Plots/All_Stations/Means_Comparisons/'+display_var+'_All_Stations-'+start+'-'+end+'.jpg')
        #And tell our user that we've done so...
        print('\nFigure saved to directory \'Plots/All_Stations/Means_Comparisons\'')
        #And close the plot.
        plt.close()   
   
#This is the main attraction; we pick a variable and plot it across the specified date range for all stations.  Lots of data getting processed for this one.                
def comparison_plotter():
     
    station_dict = build_station_dict()
    df_dict, start, end = slice_dfs()
    
    var_sel = integer_checker('Select a variable: \n1. Average Daily Snow Fall\n2. Average Daily Snow Depth\n3. Average Daily Minimum Temperature\n4. Average Daily Maximum Temperature\n\nChoice: ')
    
    var_sel_choices = {1:'snow_fall', 2:'snow_depth', 3:'temp_min', 4:'temp_max'}
    var_sel_keys = var_sel_choices.keys()

    if var_sel in var_sel_keys:
        if var_sel == 1:
            df_dict, start, end = refine_dfs_for_snow_fall()
        elif var_sel == 2:
            df_dict, start, end = refine_dfs_for_snow_depth()
        elif var_sel == 3:
            df_dict, start, end = refine_dfs_for_temp_min()
        elif var_sel == 4:
            df_dict, start, end = refine_dfs_for_temp_max()
        var_sel = var_sel_choices[var_sel]
    else:
        print('\nInvalid Choice\n')
        var_sel = integer_checker('Select a variable: \n1. Average Daily Snow Fall\n2. Average Daily Snow Depth\n3. Average Daily Minimum Temperature\n4. Average Daily Maximum Temperature\n\nChoice: ')
    
    for key in list(df_dict.keys()):
        if key not in station_dict.keys():
            station_dict.pop(key)

    df_dict = dict(zip(station_dict.values(), list(df_dict.values())))
    
    ob_count = 0
    
    for value in df_dict.values():
        ob_count += (len(value.index))

    df_keys = df_dict.keys()
    
    #This for loop redefines the values in the df_dict(ionary) as the series corresponding to whichever weather variable corresponds to the chosen var_sel value.  So, each key is a station name, and each value is now a series corresponding to var_sel.
    for key in df_keys:
        df_dict[key] = pd.Series(df_dict[key][var_sel], name = key)
        
    #We make an emptyy dataframe that we'll now merge all of the series into, matching them on the index; the result is a df with ObDate as the index, the stations names as the columns, and the values from each individual for var_sel (a weather variable, like snow_fall in inches, etc.) as the values in each cell.
    
    #Make the empty df
    all_stations_df = pd.DataFrame()
    
    #Merge all our series into it.  It's an outer merge, which I grok from SQL; left _index and right_index being true means that we are matching indexes where possible and adding them to the left(there'll only be one index in the final df) where there is no match.
    for key, value in df_dict.items():
        value = pd.Series(value)
        all_stations_df = all_stations_df.merge(value, how='outer', left_index=True, right_index=True)

    #The by-now familiar process of producing "pretty printable" versions of the weather variables.    
    fixed_var_sels = {'snow_depth':'Daily Snow Depth', 'snow_fall':'Daily Snow Fall', 'temp_min':'Daily Minimum Temperature', 'temp_max':'Daily Maximum Temperature'}
    
    #And we sort var_sel into two classes to determine what units to display on the plot.
    if var_sel == 'temp_min' or var_sel == 'temp_max':
        unit = ' (Degrees Fahrenheit)'
        
    else:
        unit = ' (inches)' 
                
    #Here we convert the index from string to datetime, which is a handy-dandy built-in feature of Pandas...
    all_stations_df.index = pd.to_datetime(all_stations_df.index)
    #And then just to be safe, we sort the index, which by default will be chronologically for this type of object
    all_stations_df = all_stations_df.sort_index()
    
    #And create this variable, a tally of the total number of observations graphed, to be displayed on the x-axis.  This needs to be corrected, since it's not taking dropped nulls into account at all.
    ob_count = str(ob_count)

    #So we finally get to plot our dataframe; the figure size is quite large, since we're dealing with a scatter plot with thousands of observations over nearly fifty years.
    
    color_dict = {'snow_depth': np.array(["darkgoldenrod","seagreen","goldenrod","darkgoldenrod","darkseagreen","darkkhaki","khaki","forestgreen","wheat","moccasin","olivedrab","green","yellowgreen", "olive"]), 'temp_min':np.array(['powderblue', 'lightblue', 'deepskyblue', 'skyblue', 'steelblue', 'dodgerblue', 'blue', 'cornflowerblue', 'royalblue', 'lightskyblue', 'silver', 'linen', 'mediumblue', 'lightsteelblue']), 'snow_fall':np.array(['mediumpurple', 'rebeccapurple', 'blueviolet', 'indigo', 'darkorchid', 'thistle', 'plum', 'fuchsia', 'orchid', 'hotpink', 'palevioletred', 'pink', 'magenta', 'mediumvioletred']), 'temp_max':np.array(['lightcoral', 'indianred', 'brown', 'firebrick', 'maroon', 'red', 'salmon', 'orangered', 'sienna', 'chocolate', 'darkorange', 'orange', 'gold', 'crimson'])}
    
    if var_sel in color_dict:
        colors = color_dict[var_sel]
    
    all_stations_plot = all_stations_df.plot(style='.', ms=8, figsize = (25, 10), fontsize=18, color=colors)
    
    #Set title, fontsize;
    plt.title('Historical '+fixed_var_sels[var_sel]+'\n', fontsize=24)
    #Set xlabel and ylabel, fontsizes.
    plt.xlabel('\nObservations = '+ob_count, fontsize=18)
    plt.ylabel(fixed_var_sels[var_sel]+unit+'\n', fontsize=18)
    
    #And fix these once again for filepath-ing
    start = start.replace('/', '.')
    end = end.replace('/', '.')
    
    #We don't want to display this huge plot, so we handle things this way instead, as above, and save the plot to the appropriate directory.
    fig = all_stations_plot.get_figure()
    fig.savefig('C:/Users/brode/Python/Western_PA_Climate_Project/Plots/All_Stations/All_Stations_'+var_sel+'_'+start+'-'+end+'.png')
    plt.close()
    print('\nPlot saved in directory \'Plots/All_Stations\' with appropriate variable names.')
    #And just because they might be handy, we save the descriptive statistics to csv as well.
    #First, we'll create a new colum to add to the df, one comprised of the averages for each station for each row in the df.
    all_stations_summary_df = all_stations_df.describe()
    
    counts = list(all_stations_summary_df.loc['count'])
    means = list(all_stations_summary_df.loc['mean'])
    stds = list(all_stations_summary_df.loc['std'])
    mins = list(all_stations_summary_df.loc['min'])
    twentyfives = list(all_stations_summary_df.loc['25%'])
    fifties = list(all_stations_summary_df.loc['50%'])
    seventyfives = list(all_stations_summary_df.loc['75%'])
    maxes = list(all_stations_summary_df.loc['max'])

    count = round((sum(counts))/(len(counts)), 1)
    mean = (sum(means))/(len(means))
    std = (sum(stds))/(len(stds))
    mins = (sum(mins))/(len(mins))
    twentyfive = round((sum(twentyfives))/(len(twentyfives)), 1)
    fifty = round((sum(fifties))/(len(fifties)), 1)
    seventyfive = round((sum(seventyfives))/(len(seventyfives)), 1)
    maxes = round((sum(maxes))/(len(maxes)), 1)
    
    all_stations_list = [count, mean, std, mins, twentyfive, fifty, seventyfive, maxes]
    
    all_stations_summary_df['All Stations Average'] = all_stations_list                     

    all_stations_summary_df.to_csv('C:/Users/brode/Python/Western_PA_Climate_Project/Summary_Stats/All_Stations_'+var_sel+'_'+start+'-'+end+'.csv')
    print('Descriptive statistics saved in directory \'Summary_Stats\'.')

#For this function, we take the same mean values used in the descriptive_stats_grapher and plot each/any one against the geographical characteristics of each station:  latitude, longitude, elevation, and a weighted average of latitude and elevation (more on that below). 
def correlation_plotter():
    #First bit is the same as in the descriptive stats grapher...
    df_dict, start, end = slice_dfs()
    start = str(start)
    end = str(end)
    df_keys = df_dict.keys()
    
    for key in df_keys:
        df_dict[key] = df_dict[key].describe()

    for key in df_keys:
        df_dict[key] = pd.Series(df_dict[key].loc['mean'])
        
    snow_fall_dict = {}
    snowdepth_dict = {}
    tempmin_dict = {}
    tempmax_dict = {}
    
    for key in df_keys:
        snow_fall_dict[key] = df_dict[key].loc['snow_fall']
        snowdepth_dict[key] = df_dict[key].loc['snow_depth']
        tempmin_dict[key] = df_dict[key].loc['temp_min']      
        tempmax_dict[key] = df_dict[key].loc['temp_max']     
    
    connstring = get_connstring()
    conn=psycopg2.connect(connstring)
    
    sql_query = pd.read_sql_query ('''
                               SELECT
                               *
                               FROM stations
                               ''', conn)

    stations_df = pd.DataFrame(sql_query, columns = ['station_code', 'station_name', 'latitude', 'longitude', 'elevation'])
    
    stations_df.set_index('station_code', inplace = True)

    stations_df.sort_index(inplace=True)

    #And we add four columns, one for each weather variable, to the stations dataframe, just by calling up the values from the previous dictionary; both are sorted alphabetically  to ensure that the values will correspond to the appropriate station.
    stations_df['snow_fall'] = sorted(snow_fall_dict.values())
    stations_df['snow_depth'] = sorted(snowdepth_dict.values())    
    stations_df['temp_min'] = sorted(tempmin_dict.values())
    stations_df['temp_max'] = sorted(tempmax_dict.values())   

    #Build a series of just the mean values for each weather variable for each station
    means_series = stations_df.describe().loc['mean']
    
    #We only need these variables (mean latitude and mean elevation for the set of stations) so that we can calculate our weighted average for latitude and elevation; as we'll see below, this is calculated by computing a z-score (standardized distance from the mean) for each, which removes the bias towards elevation that otherwise would have existed.  Once we have a z-score for each, we'll average the two and assign that to each station.
    lat_mean = means_series.loc['latitude']
    elev_mean = means_series.loc['elevation']
    
    #Make a list of all the latitude and all the longitude values, to be iterated over in our calculations of the z-scores for each.
    lat_list = stations_df['latitude'].to_list()
    elev_list = stations_df['elevation'].to_list()
    
    #Grab the standard deviations for latitude and elevation (need 'em to get the z-scores)
    lat_sd = stations_df.describe().at['std', 'latitude']
    elev_sd = stations_df.describe().at['std', 'elevation']
    
    #Some empty lists in which to put our z-scores via list.append()
    lat_z_scores = []
    elev_z_scores = []
    
    #Take each lat, calculate its z-score, stick it into the z-score list for latitudes...
    for lat in lat_list:
        lat_z=(lat-lat_mean)/lat_sd
        lat_z_scores.append(lat_z)
    #And do just the same thing for elevation...    
    for elev in elev_list:
        elev_z=(elev-elev_mean)/elev_sd
        elev_z_scores.append(elev_z)    
    #And then average the two z-scores to get our 'weighted average'
    combined_z_scores = [(a + b)/2 for a, b in zip(lat_z_scores, elev_z_scores)]
    #Add the weighted z's into the dataframe as the 'Lat/El' column...    
    stations_df['Lat/El'] = combined_z_scores

    #And we're ready to proceed:  these are the user's choices for the x axis.
    x_choices = {1:'latitude', 2:'longitude', 3:'elevation', 4:'Lat/El'}

    x_choice = integer_checker('Choose a variable to plot on the x axis: \n\n1.latitude\n2.longitude\n3.elevation\n4.Weighted Average for latitude and elevation\n\nChoice: ')

    if x_choice in x_choices.keys():
        x_chosen = x_choices[x_choice]
    else:
        print('Not a valid choice. Returning to Main Menu.')
        menu()
    
    #user choices for y-axis of correlation plotter    
    y_choices = {1:'snow_fall', 2:'snow_depth', 3:'temp_min', 4:'temp_max'}        
    y_choice = integer_checker('Choose a variable to plot on the y axis: \n\n1.Mean Daily snow_fall\n2.Mean Daily Snow Depth\n3.Mean Daily Minimum Temperature\n4.Mean Daily Maximum Temperature\n\nChoice: ')
    
    if y_choice in y_choices.keys():
        y_chosen = y_choices[y_choice]
    else:
        print('Not a valid choice. Returning to Main Menu.')
        menu()
    
    #Build two lists for plotting, using the chosen variables.  Might not need to cast them to lists, but it seemed safest and simplest.    
    x = stations_df[x_chosen].to_list()
    y = stations_df[y_chosen].to_list()
    
    #Create a list of names from our df's index, to be used in assigning names to our plotted points later
    names = stations_df['station_name'].to_list()
    
    #Just setting the units (x_units, y_units) and the labels (x_chosen, y_chosen) for display in the plot - pretty straightforward.
    if x_chosen == 'latitude':
        x_units = ' (°N)'
    if x_chosen =='longitude':
        x_units = ' (°W)'
    if x_chosen == 'elevation':
        x_units = ' (feet above sea level)'    
    if x_chosen == 'Lat/El':
        x_units = ''
        x_chosen = 'Weighted Average for latitude and elevation (Z-score)'
    if y_chosen == 'snow_fall':
        y_units = ' (inches)'
    if y_chosen == 'snow_depth':
        y_units = ' (inches)'
        y_chosen = 'Snow Depth'
    if y_chosen == 'temp_min' or y_chosen == 'temp_max':
        y_units = ' (degrees Fahrenheit)'

    #Set figure size - not a data-heavy plot, so fairly small is sufficient
    plt.figure(figsize=(12, 10))
    #And now to actually plot it; first two arguments are the lists for plotting against one another, s is the dot size.  Easy Peasy.
    plt.scatter(x, y, s=10)
    
    #Create a custom title for our plot, one that'll be unique.
    title = x_chosen+' v. '+y_chosen+' Correlation Plot ('+start+'-'+end+')'
    
    #Build the labels for the plot
    plt.xlabel(x_chosen+x_units)
    plt.ylabel('Mean '+y_chosen+y_units)
    #And set the title, as determined above.
    plt.title(title)
    
    #As usual by now, get rid of the slashes for filepath purposes.
    start = start.replace('/', '.')
    end = end.replace('/', '.')
    
    #And generate the title anew without slashes, so as for to save it.
    title = x_chosen+' v. '+y_chosen+' Correlation Plot ('+start+'-'+end+')'
    
    #And what was the trickiest part of this, annotating the points using matplotlib's annotate feature.  First argument is string to annotate with, then x/y corresponence (location of point on plot).
    for point in range(len(x)):
        plt.annotate(names[point], (x[point], y[point]))
    #Tight layout for safety's sake
    plt.tight_layout()
    #Save it
    plt.savefig('Western_PA_Climate_Project/Plots/Correlation_Plots/'+title+'.jpg')
    print('\nPlot saved to \'Plots/Correlation_Plots\'')
    #And close it up
    plt.close()

#And finally, the one function to rule them all, to find them and in the darkness bind them:  the menu function.  This one is pretty straightforward; each of the above functions is mapped to a menu sleection contained within an if clause.  
def menu():
    
    #This is not strictly necessary, since the user will see this list in some of the other functions, but in case they're curious when they first run the program, this will tell them the locations of the meteorological stations used.
    
    #List of options
    menu_choice = input('1. Pick date range (last used is default)\n2. Plot temperature, snowfall, and snow depth variables for a single station\n3. Compare those averages across all stations\n4. Compare a single variable across all stations\n5. Plot correlations between station location and temperature and precipitation \n6. View list of stations\n7. Show current start and end dates\n8. Show total number of records, data points, and stations in the database.\n9. Quit (Q/q)\n\nChoice: ')
    #python list of options, really.
    menu_choices = ['1', '2', '3', '4', '5', '6', '7', '8']
    #This should all be pretty self-evident
    if menu_choice in menu_choices:
        
        if menu_choice == '1':
            get_start_date()
            get_end_date()
            menu()
        elif menu_choice == '2':
            single_df_plotter()
            menu()
        elif menu_choice == '3':
            descriptive_stats_grapher()
            menu()
        elif menu_choice == '4':
            comparison_plotter()
            menu()
        elif menu_choice == '5':
            correlation_plotter()
            menu()

        elif menu_choice == '6':
            stations_number, station_dict = get_station_list()
            stations_counter = 0
            for station_code, station_name in station_dict.items():
                stations_counter = int(stations_counter)
                stations_counter += 1
                stations_counter = str(stations_counter)
                print(stations_counter+'. '+station_code+': '+station_name)      
            print('\nReturning to main menu...')
            menu()
        elif menu_choice == '7':
            show_slicer_dates()
            menu()

        elif menu_choice == '8':
            get_record_count()
            menu()
            
        elif menu_choice == '9':
            func_quit()
            
    elif menu_choice == 'Q' or menu_choice == 'q':
        func_quit()
        
    else:
        print('\nInput not recognized.  Try again.')
        menu()
        
def intro():
    print('\nWelcome!  A list of stations can be accessed from the menu below.\nAll climate data has been retrieved from https://www.ncdc.noaa.gov/. The source code for this program is available at https://github.com/silfeid/fa21python2_adam, as are the records in .csv format upon which it draws.  Filepaths are absolute but readily adaptable. Please address any comments or questions to brodeam@gmail.com.')

def main():

    os.chdir('C:/Users/brode/Python/')
    make_project_directories()
    db_query = check_db_exists()
    if db_query == 0:
        print('No database found')
        build_database()
    elif db_query == 1:
        print('Database found; checking for file updates')
        same_or_no = check_for_new_files()
        if same_or_no == True:
            print('No new .csv files found in Github Repository')
            pass
        else:
            print('New .csv files found in Github Repository; updating database.')
            build_database()
    set_initial_dates()
    intro()
    menu()
    
    
if __name__ == "__main__":
    main()