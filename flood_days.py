from urllib.request import urlopen
import json
import pandas as pd
import re
import arcpy


#This is the query to get all the stations. 
stations_URL = 'https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json'
#This is the query to get all the water level stations.
waterlevels_URL = 'https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.json?type=waterlevels'

response = urlopen(stations_URL).read()
# print(response)
json_object = json.loads(response)
json_formatted_str = json.dumps(json_object, indent=2)
print(json_formatted_str)
df = pd.read_json(stations_URL, orient='records')
df.drop(['count', 'units', 'self'], inplace=True, axis=1)
df
print(df)
# df= pd.read_json(stations_URL, orient='records')
# df2 = pd.json_normalize(json['response'])
# json_schema = spark.read.json(df.rdd.map(lambda rec: rec.json_result)).schema
# df = df.withColumn('json', F.from_json(F.col('json_result'), json_schema)) \
#     .select("count", "units", "json.0._source.*")
# # df = pd.read_json(response)
# print(df2)
# for col in df.columns:
#     print(col)

df[['x1']] = df.stations.str.split(',', expand=True)
# print(df)
# df.to_csv("test1.csv")

