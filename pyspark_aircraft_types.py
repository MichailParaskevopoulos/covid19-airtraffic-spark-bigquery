import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

def main():
	scSpark = SparkSession.builder.appName("aircraft_types").getOrCreate()	
	scSpark.conf.set("temporaryGcsBucket","pyspark_output_files")
	
	data_file = "gs://covid19flights_dim_tables/doc8643AircraftTypes.csv"
  
	sdfData = scSpark.read.csv(data_file, header=True, sep=",")
	sdfData.dropDuplicates(["Designator"])
	sdfData.write.format("bigquery").option("table","covid19flights:covid19_airtraffic.aircraft_types").mode("overwrite").save()	


if __name__ == '__main__':
	main()
