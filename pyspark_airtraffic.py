import pyspark as ps
from pyspark.sql import functions as f
from pyspark.sql.functions import regexp_extract, col, unix_timestamp, to_date
from pyspark.sql import types as t
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType

import math
import re

inputDir = 'gs://covid19flights/*.csv'

def month_of_row(day):
	day_components = day.split('-')
	return "{}-{}-01".format(day_components[0],day_components[1])

def day_formatted(day):
	day_components = day.split('-')
	return "{}-{}-{}".format(day_components[0],day_components[1],day_components[2])

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

def flight_category(distance):
	if distance < 1500:
		category = "short-haul"
	elif distance > 4000:
		category = "long-haul"
	else:
		category = "medium-haul"
	return category

def preprocessing(dataFrame):
	udf_month_of_row = f.udf(month_of_row, StringType())
	udf_day_formatted = f.udf(day_formatted, StringType())
	udf_distance = f.udf(distance_travelled, FloatType())
	udf_flight_category = f.udf(flight_category, StringType())
	
	#Drop null values
	dataFrame = dataFrame.na.drop(how='any', thresh=None, subset=["origin","destination","typecode","callsign","latitude_1","longitude_1","latitude_2","longitude_2"])
	#Create "month" dimension to be used as the partitioning field
	dataFrame = dataFrame.withColumn("month", to_date(unix_timestamp(udf_month_of_row("day"), "yyyy-MM-dd").cast("timestamp")))
	#Format day
	dataFrame = dataFrame.withColumn("day", to_date(unix_timestamp(udf_day_formatted("day"), "yyyy-MM-dd").cast("timestamp")))
	#Change column data type
	for column in ("latitude_1","longitude_1","latitude_2","longitude_2"):
		dataFrame = dataFrame.withColumn(column, dataFrame[column].cast(FloatType()))
	#Create column of distance travelled
	dataFrame = dataFrame.withColumn("distance", udf_distance("latitude_1","longitude_1","latitude_2","longitude_2"))
	#Create column of flight category
	dataFrame = dataFrame.withColumn("flight_category", udf_flight_category("distance"))
	#Create column of IATA airline identifier			 
	dataFrame = dataFrame.withColumn("iata_airline", regexp_extract(col("callsign"), "^[A-Z]{3}", 0))	 
	#Drop columns
	dataFrame = dataFrame.drop(*["latitude_1","longitude_1","latitude_2","longitude_2","altitude_1","altitude_2","number","icao24","registration","firstseen","lastseen"])					
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
