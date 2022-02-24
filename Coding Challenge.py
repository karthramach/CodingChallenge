# -*- coding: utf-8 -*-
"""
Created on Fri Feb 18 12:11:29 2022

@author: Karthik
"""

# Importing vital libaries to be used in the process of ELT
import requests
import psycopg2
import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt
import time


# This function extracts data from the server through multiple api calls
def Data_Extraction():

    url='https://informed-data-challenge.netlify.app/api/breweries'
    data_list=[]
    i=1
    while True:
            params={'page':i,'per_page':100}
            response = requests.get(url,
                          params=params)
            result = response.json()
            if result!={'data': []}:
                data_list.append(result)
                i = i+1
            else:
                break
    return data_list  
    
# This function creates a breweries table and loads the extracted data to it   
def Data_Loading(data):
        
    conn = None
    try:
        # Connecting to the PostgreSQL database server
        conn = psycopg2.connect("dbname=postgres user=postgres password='postgres'")
        cur = conn.cursor()
        command = (""" DROP TABLE IF EXISTS breweries;
        CREATE TABLE breweries (address_2 VARCHAR(100),address_3 VARCHAR(100),brewery_type VARCHAR(30),
                                city VARCHAR(40),country CHAR(13),county_province VARCHAR(40),id VARCHAR(100),latitude NUMERIC,
                                longitude NUMERIC,name VARCHAR(100),number_of_ratings NUMERIC,phone VARCHAR(30),postal_code VARCHAR(30),
                                rating NUMERIC,state CHAR(100),street VARCHAR(100),tags VARCHAR(5),website_url TEXT)""")
        # Executing the Table creation query
        cur.execute(command)
        for a in data_list:
            for b in a.values():
                for c in b:
                        command1=("INSERT INTO breweries VALUES(%(address_2)s, %(address_3)s, %(brewery_type)s, %(city)s, %(country)s, %(county_province)s, %(id)s, %(latitude)s, %(longitude)s, %(name)s, %(number_of_ratings)s, %(phone)s, %(postal_code)s, %(rating)s, %(state)s, %(street)s, %(tags)s, %(website_url)s")
                        # Executing the data insertion query
                        cur.execute(command1,c) 
        # Closing communication with the PostgreSQL database server
        cur.close()
        # Committing the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
# This function pulls data from the breweries table for the analysis that answers the questions Q1,Q2,and Q3         
def Data_Analysis():
    
    conn = None
    try:
        # Connecting to the PostgreSQL database server
        conn = psycopg2.connect("dbname=postgres user=postgres password='postgres'")
        
        # Analysis-1 : Breweries with the most ratings
        Q1=("""SELECT name, number_of_ratings, rating FROM breweries WHERE rating = (SELECT MAX(rating) FROM breweries) ORDER BY 2 DESC;""")
        df1 = pd.read_sql(Q1, con = conn)
        print('\nThe Following are the list of breweries with most ratings sorted in the order of higher no_of_ratings\n')
        print(df1)
        time.sleep(5)
        
        # Analysis-2 : Average ratings by brewery type
        Q2=("""SELECT brewery_type, AVG(rating) as Mean_Rating FROM breweries GROUP BY brewery_type ORDER BY Mean_Rating DESC;""")
        df2 = pd.read_sql(Q2, con = conn)
        print('\nThe Following are the breweries_types with average rating sorted in the order of higher average rating\n')
        print(df2)
        time.sleep(5)
        
        # Analysis-3 : Highest rated brewery in a state based on user input
        print ("\nKindly enter the state to fetch the highest rated brewery in that state\n")
        st = str(input("Enter your choice: "))
        Q3=("""SELECT name, MAX(rating) as Max_Rating FROM breweries WHERE state = '{}' GROUP BY 1 ORDER BY Max_Rating DESC LIMIT 1;""".format(st))
        df3 = pd.read_sql(Q3, con = conn)
        print('\nThe Following brewery is the one with highest rating in the state choosen by the user\n')
        print(df3)
        time.sleep(5)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        # Closing communication with the PostgreSQL database server
        if conn is not None:
            conn.close()

# This function calculates the distance between the two geo coordinates based on Haversine formula
def Haversine(lon1, lat1, lon2, lat2):

    # Converting decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # Implementing Haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    # Radius of earth in kilometers is 6372.8
    dist = 6373 * c
    return dist  

# This function pulls data from the breweries table for the final part of analysis that answer the question Q4         
def Data_Analysis_Final():                  
            
    conn = None
    try:
            # Connecting to the PostgreSQL database server
            conn = psycopg2.connect("dbname=postgres user=postgres password='postgres'")

            print ("\nKindly enter your current latitude and longitude positions to fetch the closest brewery to you\n")
            lat = float(input("Enter your latitude: "))
            long = float(input("Enter your longitude: "))
            
            # Analysis-4 : Closest brewery to the user geo coordinates
            df4 = pd.read_sql("""SELECT name,city,state,latitude,longitude
                                    FROM breweries""", con = conn)
    except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    finally:
            # Closing communication with the PostgreSQL database server
            if conn is not None:
                conn.close()       
    df4['latitude'] = df4['latitude'].replace('', np.nan)
    df4['longitude'] = df4['longitude'].replace('', np.nan)  
    df4['latitude'] = pd.to_numeric(df4['latitude'])
    df4['longitude'] = pd.to_numeric(df4['longitude'])
    df4['lat'] = lat
    df4['long'] = long
    df4 = df4.dropna(subset=['latitude', 'longitude']).reset_index()     
    df4['distance(Kms)'] = [Haversine(df4.long[i],df4.lat[i],df4.longitude[i],df4.latitude[i]) for i in range(len(df4))]       
    df5 = df4.sort_values('distance(Kms)',ascending=True)
    print('\nThe closest brewery to your location is\n')
    print(df5.iloc[0, [1,2,3,8]])
    
# Making function calls to perform the required tasks    
    
print('\nPlease be patient....Data is being extracted from the server through multiple API calls\n')
data_list = Data_Extraction()
print('\nData Extraction process successfull\n')
time.sleep(5)
print('\nTable creating and Data insertion is in progress...\n')
Data_Loading(data_list)
print('\nData Loading process successfull\n')
time.sleep(5)
print('\nExecuting the Data analytics query and printing the results on the console\n')
Data_Analysis()
Data_Analysis_Final()

            

            
                
            
            
        