import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

def main():
	scSpark = SparkSession.builder.appName("plane_data").getOrCreate()	
	scSpark.conf.set("temporaryGcsBucket","pyspark_output_files")
	
	data_file = "gs://openflights/planes.dat"
	
	schema = StructType([
    			StructField("plane_name", StringType(), True),
    			StructField("plane_iata_code", StringType(), True),
    			StructField("plane_icao_code", StringType(), True)])	
	
	sdfData = scSpark.read.csv(data_file, header=False, schema=schema, sep=",")
	sdfData.write.format("bigquery").option("table","covid19flights:covid19_airtraffic.planes").mode("overwrite").save()	


if __name__ == '__main__':
	main()
