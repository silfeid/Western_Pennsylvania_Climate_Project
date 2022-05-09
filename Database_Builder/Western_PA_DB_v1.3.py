# -*- coding: utf-8 -*-
"""
Created on Tue May  3 22:51:55 2022

@author: brode
"""
import psycopg2
import psycopg2.extras
import csv
from csv import reader
import os.path
from psycopg2.extensions import AsIs
from git import Repo
from datetime import datetime


def download_files():
    
    if os.path.isdir('C:/Users/brode/OneDrive/Desktop/T&L/Final_Project/Raw_CSVs') is False:
        Repo.clone_from('https://github.com/silfeid/Western_PA_Climate_Project_Raw_Data.git', 'C:/Users/brode/OneDrive/Desktop/T&L/Final_Project/Raw_CSVs')
    
    else:
        print('\nRepository of raw climate data files in csv form already exists.')

#connstring is saved to file, because we're going to need to change it once we create the db; we need to start off with a dbname that we know will exist (postgres).
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

def check_db_exists():
	connstringtext="host=localhost dbname=postgres user=postgres password=boquet"
	with open('C:/Users/brode/OneDrive/Desktop/T&L/Final_Project/connstring.txt', 'w') as writer:
		writer.write(connstringtext)
		writer.close()	
	connection = open('C:/Users/brode/OneDrive/Desktop/T&L/Final_Project/connstring.txt', 'r')
	connstring = connection.readline()
	connection.close()
	conn=psycopg2.connect(connstring)
	conn.set_session(autocommit=True)
	cursor=conn.cursor()
	sql1 ="""SELECT datname FROM pg_catalog.pg_database WHERE datname = 'western_pa_climate_db'"""
	cursor.execute(sql1)
	row=cursor.fetchone();
	if row==None:
		print("No such database - creating!")
		sql3 = """CREATE DATABASE western_pa_climate_db"""
		cursor.execute(sql3)
		cursor.close()
		conn.close()
		with open('C:/Users/brode/OneDrive/Desktop/T&L/Final_Project/connstring.txt', 'w') as writer:
			writer.write("host=localhost dbname=western_pa_climate_db user=postgres password=boquet")
			writer.close()	
		connection = open('C:/Users/brode/OneDrive/Desktop/T&L/Final_Project/connstring.txt', 'r')
		connstring = connection.readline()
		conn=psycopg2.connect(connstring)
		cursor=conn.cursor()
		sql5 = """CREATE TABLE stations (
	station_code VARCHAR ( 50 ) PRIMARY KEY,
	station_name VARCHAR ( 50 ) NOT NULL,
	latitude NUMERIC ( 7,4 ) NOT NULL,
	longitude NUMERIC ( 7,4 ) NOT NULL,
	elevation NUMERIC ( 5,1 ) NOT NULL
);"""
		cursor.execute(sql5)
		conn.commit()
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
		print('Database created!')
		
	else:
		print('\nDatabase found!')
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
	return db_name

def import_station_from_csv():
	print()
	with open('C:/Users/brode/OneDrive/Desktop/T&L/Final_Project/connstring.txt', 'w') as writer:
		writer.write("host=localhost dbname=western_pa_climate_db user=postgres password=boquet")
		writer.close()	
		connection = open('C:/Users/brode/OneDrive/Desktop/T&L/Final_Project/connstring.txt', 'r')
		connstring = connection.readline()
	conn=psycopg2.connect(connstring)
	cur=conn.cursor()

	directory = 'C:/Users/brode/OneDrive/Desktop/T&L/Final_Project/Raw_CSVs/'
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
				if station_code!=None:
					print("Read: " + station_code.ljust(20, ' ') + station_name.ljust(10, ' ') + station_latitude.ljust(10, ' ') + station_longitude.ljust(10, ' ') + station_elevation.ljust(10, ' '))	
					cur.execute('CALL add_station(%s, %s, %s, %s, %s)', (station_code, station_name, station_latitude, station_longitude, station_elevation));
	conn.commit()
	cur.close()
	conn.close
	
def grab_db_table_list():
	connection = open('C:/Users/brode/OneDrive/Desktop/T&L/Final_Project/connstring.txt', 'r')
	connstring = connection.readline()
	conn=psycopg2.connect(connstring)
	cur=conn.cursor()
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
	
def import_observations_from_csv():
	print()
	connection = open('C:/Users/brode/OneDrive/Desktop/T&L/Final_Project/connstring.txt', 'r')
	connstring = connection.readline()
	conn=psycopg2.connect(connstring)
	cur=conn.cursor()
	
	table_list = grab_db_table_list()
	
	directory = 'C:/Users/brode/OneDrive/Desktop/T&L/Final_Project/Raw_CSVs/'
	
	file_list = os.listdir(directory)
	
	counter = 0
	
	for filename in file_list:
		if filename.endswith('.csv'):
			with open(directory+filename, 'r') as read_obj:
				csv_reader = reader(read_obj)
				header=next(csv_reader)
				row = next(csv_reader)
				station_code=row[0]
				if station_code in table_list:
					print('Table for station '+station_code+ ' already exists in db, skipping table creation.')
				else:
					sql1="""SELECT * FROM stations WHERE station_code=%s"""
					cur.execute(sql1, (station_code,))
					row=cur.fetchone();
					if row==None:
						print("Station not listed in stations table; skipping.")
						file_list.remove(filename)

					cur.execute("""CREATE TABLE IF NOT EXISTS """+station_code+"""(
							ob_number smallint GENERATED ALWAYS AS IDENTITY UNIQUE,
							station_code VARCHAR(50) REFERENCES stations (station_code) NOT NULL,
							ob_date Date NOT NULL,
							snow_fall numeric(3,1) NULL,
							snow_depth numeric (4,2) NULL,
							temp_max numeric (5,2) NULL,
							temp_min numeric (5,2) NULL);""");
					conn.commit()
					print('\nTable created for station '+station_code+';')
					
					read_obj.seek(0)
					row = next(csv_reader)
					for row in csv_reader:
						station_code=row[0]
						if station_code=='':
							station_code=None
						ob_date=row[5]
						if ob_date=='':
							ob_date=None
						snowfall=row[6]
						if snowfall=='':
							snowfall=None
						snowdepth=row[7]
						if snowdepth=='':
							snowdepth=None
						tempmax=row[8]
						if tempmax=='':
							tempmax=None
						tempmin=row[9]
						if tempmin=='':
							tempmin=None
						if station_code!=None:
							#print("Read: " + station_code.ljust(20, ' ') + ob_date.ljust(10, ' ') + snowfall.ljust(10, ' ') + snowdepth.ljust(10, ' ') + tempmax.ljust(10, ' ') + tempmin.ljust(10, ' '))	
							cur.execute("""INSERT INTO """+station_code+"""(station_code, ob_date, snow_fall, snow_depth, temp_max, temp_min) SELECT %s, %s, %s, %s, %s, %s WHERE NOT EXISTS (SELECT ob_date FROM """+station_code+""" WHERE ob_date = %s);""",(station_code, ob_date, snowfall, snowdepth, tempmax, tempmin, ob_date));
							
							counter += 1
					
					print(str(counter)+' values added to Table '+station_code+'.\n')
	conn.commit()
	cur.close()
	conn.close
	
def select_date():
	connection = open('C:/Users/brode/OneDrive/Desktop/T&L/Final_Project/connstring.txt', 'r')
	connstring = connection.readline()
	conn=psycopg2.connect(connstring)
	cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
	
	sql1="""SELECT * FROM stations"""
	cur.execute(sql1)
	row=cur.fetchone();
	stations=[]
	counter = 1
	if row==None:
		print("No records found.")
	else:
		print('\nAll Stations\n')
		while row is not None:
			print(str(counter)+' | '+row['station_code'] + " | " + row['station_name'] + " | " + str(row['latitude']) + " | " + str(row['longitude']) + " | " + str(row['elevation']))
			stations.append(row['station_code'])
			row=cur.fetchone()
			counter += 1

	solicit_station = integer_checker('Enter a station code from the table above:  ')
	while solicit_station < 1 or solicit_station > counter:
		solicit_station = integer_checker('Enter a station code from the table above:  ')
	else:
		station_number = solicit_station-1
	
	solicit_date = valiDate('Enter a date in the format MM/DD/YYYY:  ')
	solicit_station = stations[station_number]
	
	sql2 = """SELECT * FROM %s WHERE ob_date = %s;"""
	cur.execute(sql2, (AsIs(solicit_station), solicit_date))
	row=cur.fetchone();
	column_names = get_column_names(solicit_station)
	if row==None:
		print("No records found.")
	else:
		print('\nData from '+solicit_station+' on '+solicit_date+'\n')
		print(column_names[2]+'|'+column_names[3]+'|'+column_names[4]+'|'+column_names[5]+'|'+column_names[6])
		while row is not None:

			print(str(row['ob_date']) + " | " + str(row['snow_fall']) + "    | " + str(row['snow_depth']) + "     | " + str(row['temp_max']) + "  | " + str(row['temp_min']))
			row=cur.fetchone()
			
def select_dates():
	connection = open('C:/Users/brode/OneDrive/Desktop/T&L/Final_Project/connstring.txt', 'r')
	connstring = connection.readline()
	conn=psycopg2.connect(connstring)
	cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
	
	sql1="""SELECT * FROM stations"""
	cur.execute(sql1)
	row=cur.fetchone();
	stations=[]
	counter = 1
	if row==None:
		print("No records found.")
	else:
		print('\nAll Stations\n')
		while row is not None:
			print(str(counter)+' | '+row['station_code'] + " | " + row['station_name'] + " | " + str(row['latitude']) + " | " + str(row['longitude']) + " | " + str(row['elevation']))
			stations.append(row['station_code'])
			row=cur.fetchone()
			counter += 1

	solicit_station = integer_checker('Enter a station code from the table above:  ')
	while solicit_station < 1 or solicit_station > counter:
		solicit_station = integer_checker('Enter a station code from the table above:  ')
	else:
		station_number = solicit_station-1
	
	solicit_first_date = valiDate('Enter a start date in the format MM/DD/YYYY:  ')
	solicit_second_date = valiDate('Enter an end date in the format MM/DD/YYYY:  ')
	solicit_station = stations[station_number]
	
	sql2 = """SELECT * FROM %s WHERE ob_date BETWEEN %s AND %s;"""
	cur.execute(sql2, (AsIs(solicit_station), solicit_first_date, solicit_second_date))
	row=cur.fetchone();
	column_names = get_column_names(solicit_station)	
	if row==None:
		print("No records found.")
	else:
		print('\nData from '+solicit_station+' between ' + solicit_first_date + ' and ' + solicit_second_date + '\n')
		print(column_names[2]+'|'+column_names[3]+'|'+column_names[4]+'|'+column_names[5]+'|'+column_names[6])
		while row is not None:
			print(str(row['ob_date']) + " | " + str(row['snow_fall']) + " | " + str(row['snow_depth']) + " | " + str(row['temp_max']) + " | " + str(row['temp_min']))
			row=cur.fetchone()

def select_dates_temp_max_average():
	connection = open('C:/Users/brode/OneDrive/Desktop/T&L/Final_Project/connstring.txt', 'r')
	connstring = connection.readline()
	conn=psycopg2.connect(connstring)
	cur=conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
	
	sql1="""SELECT * FROM stations"""
	cur.execute(sql1)
	row=cur.fetchone();
	stations=[]
	counter = 1
	if row==None:
		print("No records found.")
	else:
		print('\nAll Stations\n')
		while row is not None:
			print(str(counter)+' | '+row['station_code'] + " | " + row['station_name'] + " | " + str(row['latitude']) + " | " + str(row['longitude']) + " | " + str(row['elevation']))
			stations.append(row['station_code'])
			row=cur.fetchone()
			counter += 1

	solicit_station = integer_checker('Enter a station code from the table above:  ')
	while solicit_station < 1 or solicit_station > counter:
		solicit_station = integer_checker('Enter a station code from the table above:  ')
	else:
		station_number = solicit_station-1
	
	solicit_first_date = valiDate('Enter a start date in the format MM/DD/YYYY:  ')
	solicit_second_date = valiDate('Enter an end date in the format MM/DD/YYYY:  ')
	solicit_station = stations[station_number]
	
	sql2 = """SELECT ROUND(AVG(temp_max), 2) AS av_temp_max FROM %s WHERE ob_date BETWEEN %s AND %s;"""
	cur.execute(sql2, (AsIs(solicit_station), solicit_first_date, solicit_second_date))
	row=cur.fetchone();
	if row==None:
		print("No records found.")
	else:
		print('\nData from '+solicit_station+' between ' + solicit_first_date + ' and ' + solicit_second_date + '\n')
		print('Average Daily Maximum Temperature: \n')
		while row is not None:
			print(str(row['av_temp_max']))
			row=cur.fetchone()

def get_column_names(table_name):
	connection = open('C:/Users/brode/OneDrive/Desktop/T&L/Final_Project/connstring.txt', 'r')
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
			test_str = input('Enter a date in the format MM/DD/YYYY: ')
			result = bool(datetime.strptime(test_str, format))
		except ValueError:
			result = False
		else:
			return test_str

def integer_checker(integer):
  while True:
    try:
       user_input = int(input(integer))       
    except ValueError:
       print("\nInput must be an integer. Try again.")
       continue
    else:
       return user_input 
       break 

def menu():
	choice = input('1 to check if db exists\n2 to close all connections to db\n3 to import stations csv\n4 to import station observations\n5 to see observations for a given date from a given station\n6 to see observations for a range of dates from a given station\n7 to download remote repository of raw climate data files in csv form from Github\nQ/q to quit:  ')	

	if choice == '1':
		check_db_exists()
		menu()
	elif choice == '2':
		close_all_connections_to_db()
		menu()
	elif choice == '3':
		import_station_from_csv()
		menu()
	elif choice == '4':
		import_observations_from_csv()
		menu()
	elif choice == '5':
		select_date()
		menu()
	elif choice == '6':
		select_dates()
		menu()
	elif choice == '7':
		download_files()
		menu()
	elif choice == 'Q' or choice =='q':
		pass
	elif choice =='8':
		select_dates_temp_max_average()
		
menu()