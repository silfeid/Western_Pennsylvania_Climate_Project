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
		print('Database found!')
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
	with open('C:/Users/brode/OneDrive/Desktop/T&L/Final_Project/connstring.txt', 'w') as writer:
		writer.write("host=localhost dbname=western_pa_climate_db user=postgres password=boquet")
		writer.close()	
		connection = open('C:/Users/brode/OneDrive/Desktop/T&L/Final_Project/connstring.txt', 'r')
		connstring = connection.readline()
	conn=psycopg2.connect(connstring)
	cur=conn.cursor()

	directory = 'C:/Users/brode/OneDrive/Desktop/Raw_CSVs/'
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
	
def import_observations_from_csv():
	connection = open('C:/Users/brode/OneDrive/Desktop/T&L/Final_Project/connstring.txt', 'r')
	connstring = connection.readline()
	conn=psycopg2.connect(connstring)
	cur=conn.cursor()
	
	directory = 'C:/Users/brode/OneDrive/Desktop/Raw_CSVs/'
	file_list = os.listdir(directory)
	for filename in file_list:
		if filename.endswith('.csv'):
			with open(directory+filename, 'r') as read_obj:
				csv_reader = reader(read_obj)
				header=next(csv_reader)
				row = next(csv_reader)
				station_code=row[0]
				sql1="""SELECT * FROM stations WHERE station_code=%s"""
				cur.execute(sql1, (station_code,))
				row=cur.fetchone();
				if row==None:
					print("Station not listed in stations table; skipping.")
					file_list.remove(filename)
		else:
			file_list.remove(filename)
				
	for filename in file_list:
		with open(directory+filename, 'r') as read_obj:
			csv_reader = reader(read_obj)
			header=next(csv_reader)
			row = next(csv_reader)
			station_code=row[0]
			cur.execute("""CREATE TABLE IF NOT EXISTS """+station_code+"""(
					ob_number smallint GENERATED ALWAYS AS IDENTITY UNIQUE,
					station_code VARCHAR(50) REFERENCES stations (station_code) NOT NULL,
					ob_date Date NOT NULL,
					snow_fall numeric(3,1) NULL,
					snow_depth numeric (4,2) NULL,
					temp_max numeric (5,2) NULL,
					temp_min numeric (5,2) NULL);""");
			conn.commit()	
	#Probably should change this so that the default is to just skip files for which there is already a table in the DB, leave a separate function so that the user can add new dates etc. if they want; this is memory-intensive and time-consuming (took computer five minutes to add everything, just to get one new date...)			
	for filename in file_list:			
		with open(directory+filename, 'r') as read_obj:
			csv_reader = reader(read_obj)
			header=next(csv_reader)
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
					print('values added')
			
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
	if row==None:
		print("No records found")
	else:
		print('\nAll Stations\n')
		while row is not None:
			print(row['station_code'] + " | " + row['station_name'] + " | " + str(row['latitude']) + " | " + str(row['longitude']) + " | " + str(row['elevation']))
			row=cur.fetchone()

	
	solicit_station = input('Enter a station code from the table above:  ')
	solicit_date = input('Enter desired date in format:  ')
	
	sql2 = """SELECT * FROM %s WHERE ob_date = %s;"""
	cur.execute(sql2, (AsIs(solicit_station), solicit_date))
	row=cur.fetchone();
	if row==None:
		print("No records found")
	else:
		print('Data from '+solicit_station+' on '+solicit_date)
		while row is not None:
			print(str(row['ob_date']) + " | " + str(row['snow_fall']) + " | " + str(row['snow_depth']) + " | " + str(row['temp_max']) + " | " + str(row['temp_min']))
			row=cur.fetchone()


choice = input('1 to check if db exists\n2 to close all connections to db\n3 to import stations csv\n4 to import station observations\n5 to see observations for a given date from a given station:  ')	
choices = ['1', '2', '3', '4', '5', '6', '7']
if choice == '1':
	db_name = check_db_exists()
elif choice == '2':
	close_all_connections_to_db()
elif choice == '3':
	import_station_from_csv()
elif choice == '4':
	import_observations_from_csv()
elif choice == '5':
	select_date()
