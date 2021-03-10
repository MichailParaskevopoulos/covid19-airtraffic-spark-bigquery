import pyspark as ps
from pyspark.sql import functions as f
from pyspark.sql.functions import col, unix_timestamp, to_date
from pyspark.sql import types as t
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType

import math
import re

inputDir = 'gs://covid19flights/*.csv'
#inputDir = 'gs://covid19flights/{}'
#inputDir = "gs://covid19flights/flightlist_20190701_20190731.csv"

def month_of_row(day):
	day_components = day.split('-')
	return "{}-{}-01".format(day_components[0],day_components[1])

def distance_travelled(lat1, lon1, lat2, lon2):
	R = 6373.0
	lat1 = math.radians(lat1)
	lon1 = math.radians(lon1)
	lat2 = math.radians(lat2)
	lon2 = math.radians(lon2)
	
	dlon = lon2 - lon1
	dlat = lat2 - lat1
	
	a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
	c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
	
	distance = R * c
	return distance

def preprocessing(dataFrame):
	udf_month_of_row = f.udf(month_of_row, StringType())
	udf_distance = f.udf(distance_travelled, FloatType())
	
	#Drop null values
	dataFrame = dataFrame.na.drop(how='any', thresh=None, subset=["origin","destination","typecode","callsign"])
	#Create "month" dimension to be used as the partitioning field
	dataFrame = dataFrame.withColumn("month", to_date(unix_timestamp(udf_month_of_row("day"), "yyyy-MM-dd").cast("timestamp")))
	
	for column in ("latitude_1","longitude_1","latitude_2","longitude_2"):
		dataFrame = dataFrame.withColumn(column, dataFrame[column].cast(FloatType()))
	
	dataFrame = dataFrame.withColumn("distance", udf_distance("latitude_1","longitude_1","latitude_2","longitude_2"))
	
	return dataFrame
	
def main():
	scSpark = SparkSession.builder.appName("covid19_airtraffic_data").getOrCreate()	
	scSpark.conf.set("temporaryGcsBucket","pyspark_output_files")
	
	data_file = inputDir
	sdfData = scSpark.read.csv(data_file, header=True, sep=",")
	
	sdfData = preprocessing(sdfData)
	sdfData.write.format("bigquery").option("table","covid19flights:covid19_airtraffic.records").option("partitionField", "month").mode("append").save()	
		
if __name__ == '__main__':
	main()
