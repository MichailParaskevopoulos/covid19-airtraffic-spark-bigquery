import pyspark as ps
from pyspark.sql import functions as f
from pyspark.sql import SQLContext

def main():
	scSpark = SparkSession.builder.appName("analysis").getOrCreate()	
	scSpark.conf.set("temporaryGcsBucket","pyspark_output_files")	
	
	airtraffic_data = spark.read.format('bigquery').option('table', "covid19flights:covid19_airtraffic.airtraffic").load()
	


if __name__ == '__main__':
	main()
