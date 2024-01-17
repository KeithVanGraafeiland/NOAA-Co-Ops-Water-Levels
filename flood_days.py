from urllib.request import urlopen
import json
import pandas as pd
import re


#This is the query to get all the stations. 
stations_URL = 'https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json'
#This is the query to get all the water level stations.
waterlevels_URL = 'https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json?type=waterlevels'

response = urlopen(stations_URL).read()
# print(response)
df= pd.read_json(stations_URL)
# df = pd.read_json(response)
# print(df)
for col in df.columns:
    print(col)
df_new = df['stations'].str.split(' ')
print(df_new)
df_new.to_csv("test1.csv")
