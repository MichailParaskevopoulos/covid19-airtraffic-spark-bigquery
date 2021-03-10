import pyspark as ps
from pyspark.sql import functions as f
from pyspark.sql.functions import col, unix_timestamp, to_date
from pyspark.sql import types as t
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType

import warnings
import re

inputDir = 'gs://covid19flights/*.csv'
#inputDir = 'gs://covid19flights/{}'
#inputDir = "gs://covid19flights/flightlist_20190701_20190731.csv"

def month_of_row(day):
	day_components = day.split('-')
	return "{}-{}-01".format(day_components[0],day_components[1])
	
def main():
	scSpark = SparkSession.builder.appName("covid19_airtraffic_data").getOrCreate()	
	scSpark.conf.set("temporaryGcsBucket","pyspark_output_files")
	
	data_file = inputDir
	sdfData = scSpark.read.csv(data_file, header=True, sep=",")
	
	#Drop null values
	sdfData = sdfData.na.drop(how='any', thresh=None, subset=["origin","destination","typecode","callsign"])
	
	udf_month_of_row = f.udf(month_of_row, StringType())
	
	sdfData_with_month = sdfData.withColumn("month", to_date(unix_timestamp(udf_month_of_row("day"), "yyyy-MM-dd").cast("timestamp")))
	sdfData_with_month.write.format("bigquery").option("table","covid19flights:covid19_airtraffic.records").option("partitionField", "month").mode("append").save()	
		
if __name__ == '__main__':
	main()
