import pyspark as ps
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType

import warnings
import re

#set input to all csv files of the covid19flights bucket
inputDir = 'gs://covid19flights/*.csv'

if __name__ == '__main__':
	scSpark = SparkSession.builder.appName("reading csv").getOrCreate()
	data_file = inputDir
	scSpark.conf.set("temporaryGcsBucket","pyspark_output_files")
	
	sdfData = scSpark.read.csv(data_file, header=True, sep=",").cache()

	sdfData.registerTempTable("airports")
	
	output =  scSpark.sql('SELECT COUNT(destination) as count_destination from airports GROUP BY origin')
	output.write.format("bigquery").save("covid19flights:covid19_airtraffic.count")

	
	#df = sdfData.groupBy('origin').count()
	#df.write.format("bigquery").save("covid19flights:covid19_airtraffic.count")


	
