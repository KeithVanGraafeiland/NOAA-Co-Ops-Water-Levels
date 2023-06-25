## Co-Ops API Reference - https://api.tidesandcurrents.noaa.gov/api/prod/
## Sample Request - https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?begin_date=20130101 10:00&end_date=20130101 10:24&station=8454000&product=water_level&datum=mllw&units=metric&time_zone=gmt&application=web_services&format=xml
## Feature Service of Active Stations -  https://www.arcgis.com/home/item.html?id=8cb6814dd34747ca96fa616af8d78b25#data
## Sample Request - https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?begin_date=today&range=24&station=8454000&product=water_level&datum=mllw&units=metric&time_zone=gmt&application=web_services&format=xml
## Example Query for Water Level: https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?begin_date=20210908&range=48&station=8454000&product=water_level&datum=mllw&units=english&time_zone=gmt&application=Esri_KeithVanGraafeiland&format=xml&interval=h
## Example Query for Prediction: https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?begin_date=20210908&range=72&station=8454000&product=predictions&datum=mllw&units=english&time_zone=gmt&application=Esri_KeithVanGraafeiland&format=xml
## products = water_level, air_temperature, water_temperature, wind, air_pressure, air_gap, conductivity, visibility, humidity, salinity,\
##            hourly_height, high_low, daily_mean, monthly_mean, one_minute_water_level, predictions, datums, currents, currents_predictions

## #NOTE - No predictions available for Great Lakes (IGLD)

from arcgis.features import FeatureLayer
from datetime import datetime
from datetime import date
from datetime import timedelta
import urllib.request
import pandas
import arcpy
import glob
import os

## Define Constants
STATIONS_FL_URL = 'https://idpgis.ncep.noaa.gov/arcgis/rest/services/NOS_Observations/CO_OPS_Products/FeatureServer/0'
# STATIONS_FL_URL = 'https://idpgis.ncep.noaa.gov/arcgis/rest/services/NOS_Observations/CO_OPS_Stations/FeatureServer/0'
URL1 = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?begin_date='
WATER_LEVEL_QUERY = '&product=water_level'
PREDICTION_QUERY = '&product=predictions'
STATION_QUERY = '&station='
RANGE = '&range=72'
TODAY = date.today()
print(TODAY)
YESTERDAY = TODAY - timedelta(days=1)
print(YESTERDAY)
CURRENT_DATE = (TODAY.strftime("%Y%m%d"))
YESTERDAY_DATE = (YESTERDAY.strftime("%Y%m%d"))
ROOT = "C:/GitHub/NOAA-Co-Ops-Water-Levels" #TODO replace with your own ROOT

WATER_LEVEL_STATION_MASTER = ROOT + "\stations\Water_Level_Stations_Master.csv"
WATER_LEVEL_STATIONS_MLLW = ROOT + "\stations\Water_Level_Stations_mllw.csv"
WATER_LEVEL_STATIONS_MSL = ROOT + "\stations\Water_Level_Stations_msl.csv"
WATER_LEVEL_STATIONS_IGLD = ROOT + "\stations\Water_Level_Stations_igld.csv"
WATER_LEVEL_CSV_DIRECTORY = ROOT + "\\water_level"
PREDICTIONS_CSV_DIRECTORY = ROOT + "\\prediction"

# Get active station list

def split_water_level_stations_by_datum():
    STATION_MASTER_DF = pandas.read_csv(WATER_LEVEL_STATION_MASTER)
    dfs = dict(tuple(STATION_MASTER_DF.groupby('datum')))
    mllw = dfs['mllw']
    msl = dfs['msl']
    igld = dfs['igld']
    mllw = mllw[mllw.note != 'offline']
    mllw.to_csv(WATER_LEVEL_STATIONS_MLLW, index=False)
    msl = msl[msl.note != 'offline']
    msl.to_csv(WATER_LEVEL_STATIONS_MSL, index=False)
    igld = igld[igld.note != 'offline']
    igld.to_csv(WATER_LEVEL_STATIONS_IGLD, index=False)

def get_MLLW_station_info():
    get_station_info('mllw', WATER_LEVEL_STATIONS_MLLW)

def get_MSL_station_info():
    get_station_info('msl', WATER_LEVEL_STATIONS_MSL)

def get_station_info(datum, feature_table):
    valueList = []  # array to hold list of values collected
    valueSet = set()  # set to hold values to test against to get list
    rows = arcpy.SearchCursor(feature_table)
    field = "id"
    for row in rows:
        value = row.getValue(field)
        if value not in valueSet:
            valueList.append(value)
            valueSet.add(value)
    valueList.sort()
    print("Done creating " + datum + "value list.....")

    for x in valueList:
        station = x
        water_level_csv = ROOT + "\\water_level\\" + str(station) + "_" + CURRENT_DATE + "_water_level.csv"
        prediction_csv = ROOT + "\\prediction\\" + str(station) + "_" + CURRENT_DATE + "_prediction.csv"
        URL2 = '&datum='+ datum + '&units=english&time_zone=gmt&application=web_services&format=csv&interval=h'
        ## Build URLs
        WATER_LEVEL_URL = URL1 + YESTERDAY_DATE + RANGE + STATION_QUERY + str(station) + zzzURL2
        PREDICTION_URL = URL1 + CURRENT_DATE + RANGE + STATION_QUERY + str(station) + PREDICTION_QUERY + URL2
        ## Retreive results from URL requests
        urllib.request.urlretrieve(WATER_LEVEL_URL, water_level_csv)
        WATER_LEVEL_TABLE = pandas.read_csv(water_level_csv)
        WATER_LEVEL_TABLE['id'] = station
        WATER_LEVEL_TABLE.to_csv(water_level_csv, index=False)
        print('Done processing ' + datum + ' water levels for station ' + str(station))
        urllib.request.urlretrieve(PREDICTION_URL, prediction_csv)
        PREDICTION_TABLE = pandas.read_csv(prediction_csv)
        PREDICTION_TABLE['id'] = station
        PREDICTION_TABLE.to_csv(prediction_csv, index=False)
        print('Done processing ' + datum + ' predictions for station ' + str(station))

def get_IGLD_station_info():
    # get_station_info('igld', WATER_LEVEL_STATIONS_IGLD)
    valueList = []  # array to hold list of values collected
    valueSet = set()  # set to hold values to test against to get list
    rows = arcpy.SearchCursor(WATER_LEVEL_STATIONS_IGLD)
    field = "id"
    for row in rows:
        value = row.getValue(field)
        if value not in valueSet:
            valueList.append(value)
            valueSet.add(value)
    valueList.sort()
    print("Done creating IGLD value list.....")

    for x in valueList:
        station = x
        water_level_csv = ROOT + "\\water_level\\" + str(station) + "_" + CURRENT_DATE + "_water_level.csv"
        URL2 = '&datum=igld&units=english&time_zone=gmt&application=web_services&format=csv&interval=h'
        ## Build URLs
        WATER_LEVEL_URL = URL1 + YESTERDAY_DATE + RANGE + STATION_QUERY + str(station) + WATER_LEVEL_QUERY + URL2
        ## Retreive results from URL requests
        urllib.request.urlretrieve(WATER_LEVEL_URL, water_level_csv)
        WATER_LEVEL_TABLE = pandas.read_csv(water_level_csv)
        WATER_LEVEL_TABLE['id'] = station
        WATER_LEVEL_TABLE.to_csv(water_level_csv, index=False)
        print('Done processing IGLD water levels for station ' + str(station))

def merge_water_level_CSV_products():
    WATER_LEVEL_STATIONS_MERGED_CSV = WATER_LEVEL_CSV_DIRECTORY + "\\" + "All_Water_Level_Stations.csv"
    PREDICTIONS_STATIONS_MERGED_CSV = PREDICTIONS_CSV_DIRECTORY + "\\" + "All_Stations_Predictions.csv"
    os.remove(WATER_LEVEL_STATIONS_MERGED_CSV)
    os.remove(PREDICTIONS_STATIONS_MERGED_CSV)
    ## merging the files
    merged_water_level = os.path.join(WATER_LEVEL_CSV_DIRECTORY, "*.csv")
    merged_predictions = os.path.join(PREDICTIONS_CSV_DIRECTORY, "*.csv")
    ## A list of all joined files is returned
    joined_water_level_list = glob.glob(merged_water_level)
    joined_predictions_list = glob.glob(merged_predictions)
    ## Now the files are joined
    WATER_LEVEL_STATIONS_MERGED = pandas.concat(map(pandas.read_csv, joined_water_level_list), ignore_index=True)
    WATER_LEVEL_STATIONS_MERGED.to_csv(WATER_LEVEL_STATIONS_MERGED_CSV, index=False)
    print("Finished merging water level data.....")
    PREDICTION_STATIONS_MERGED = pandas.concat(map(pandas.read_csv, joined_predictions_list), ignore_index=True)
    PREDICTION_STATIONS_MERGED.to_csv(PREDICTIONS_STATIONS_MERGED_CSV, index=False)
    print("Finished merging prediction data.....")

def delete_intermediate_csv_files(path, wildcard):
    print('Deleting intermediate csv files.....')
    # Get a list of all the file paths that ends with .txt from in specified directory
    fileList = glob.glob(path + '\\' + wildcard)

    # Iterate over the list of filepaths & remove each file.
    for filePath in fileList:
        try:
            os.remove(filePath)
        except:
            print("Error while deleting file : ", filePath)

print("***** Start processing water level and prediction data *****")
split_water_level_stations_by_datum()
get_MLLW_station_info()
get_MSL_station_info()
get_IGLD_station_info()
merge_water_level_CSV_products()
delete_intermediate_csv_files(WATER_LEVEL_CSV_DIRECTORY, '*water_level.csv')
delete_intermediate_csv_files(PREDICTIONS_CSV_DIRECTORY, '*prediction.csv')
print("***** Finished processing water level and prediction Data *****")
