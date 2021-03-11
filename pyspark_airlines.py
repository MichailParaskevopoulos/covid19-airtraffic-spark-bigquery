import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():
	scSpark = SparkSession.builder.appName("airlines").getOrCreate()	
	scSpark.conf.set("temporaryGcsBucket","pyspark_output_files")
	
	data_file = "gs://covid19flights_dim_tables/airlines.dat"
	
	schema = StructType([
			StructField("index", IntegerType(), True),
    			StructField("airline_name", StringType(), True),
    			StructField("alias", StringType(), True),
			StructField("airline_iata_code", StringType(), True),
			StructField("airline_icao_code", StringType(), True),
			StructField("airline_callsign_id", StringType(), True),
			StructField("country", StringType(), True),
    			StructField("active", StringType(), True)])	
	
	sdfData = scSpark.read.csv(data_file, header=False, schema=schema, sep=",")
	sdfData.write.format("bigquery").option("table","covid19flights:covid19_airtraffic.airlines").mode("overwrite").save()	


if __name__ == '__main__':
	main()
