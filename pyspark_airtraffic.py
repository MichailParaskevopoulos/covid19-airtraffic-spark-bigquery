import pyspark as ps
from pyspark.sql import functions as f
from pyspark.sql.functions import col, unix_timestamp, to_date
from pyspark.sql import types as t
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType

import warnings
import re

#set input to all csv files of the covid19flights bucket
#inputDir = 'gs://covid19flights/*.csv'
inputDir = 'gs://covid19flights/flightlist_20190101_20190131.csv'

if __name__ == '__main__':
	scSpark = SparkSession.builder.appName("reading csv").getOrCreate()
	data_file = inputDir
	scSpark.conf.set("temporaryGcsBucket","pyspark_output_files")
	
	sdfData = scSpark.read.csv(data_file, header=True, sep=",").cache()

	#def month_of_row(day):
	#	day_components = day.split('-')
	#	return "{}{}01".format(day_components[0],day_components[1])
	
	#udf_month_of_row = f.udf(month_of_row, StringType())
	#sdfData_with_month = sdfData.withColumn("month", udf_month_of_row("day"))
	
	sdfData_with_month = sdfData.withColumn("month", to_date(unix_timestamp(col("day"), "MM-dd-yyyy HH:mm:ss").cast("timestamp")))
	sdfData_with_month.write.format("bigquery").option("partitionField", "month").save("covid19flights:covid19_airtraffic.count")
	#sdfData_with_month.write.format("bigquery").save("covid19flights:covid19_airtraffic.count")
		
	#sdfData.registerTempTable("airports")
	
	#output =  scSpark.sql('SELECT COUNT(destination) as count_destination, origin, destination from airports GROUP BY origin, destination')
	#output.write.format("bigquery").save("covid19flights:covid19_airtraffic.count")

	#df = sdfData.groupBy('origin').count()
	#df.write.format("bigquery").save("covid19flights:covid19_airtraffic.count")


	
