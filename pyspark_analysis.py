import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

def main():
	scSpark = SparkSession.builder.appName("analysis").getOrCreate()	
	scSpark.conf.set("temporaryGcsBucket","pyspark_output_files")	
	
	airtraffic_data = scSpark.read.format('bigquery').option('table', "covid19flights:covid19_airtraffic.airtraffic").load()
	
	airtraffic_data.registerTempTable("airtraffic")
	
	total_flights_monthly = scSpark.sql(""" SELECT count(callsign) as total_flights, month
							FROM airtraffic
							WHERE flights_category = 'long-haul'
							GROUP BY month """)
	
	flights_by_aircraft_type = scSpark.sql(""" SELECT count(callsign) as flights, month, typecode
							FROM airtraffic
							WHERE flights_category = 'long-haul'
							GROUP BY month, typecode """)
	
	summary = flights_by_aircraft_type.join(total_flights_monthly, on=['month'], how='left')
	summary = summary.withColumn('flight_fraction', summary['flights']/summary['total_flights'])
	
	summary.write.format("bigquery").option("table","covid19flights:covid19_airtraffic.analysis").mode("overwrite").save()	
	

if __name__ == '__main__':
	main()
