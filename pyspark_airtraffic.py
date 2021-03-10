import pyspark as ps
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.types import StringType
import warnings
import re

inputDir = gs://covid19flights/

#define regex pattern for preprocessing
iata_airline = r"^[A-Z]{3}"

def processing_airline(column):
  return re.match(iata_airline,column)[0]


def main(input_dir):
	try:
	    sc = ps.SparkContext()
	    sc.setLogLevel("ERROR")
	    sqlContext = ps.sql.SQLContext(sc)
	    print('Created a SparkContext')
	except ValueError:
	    warnings.warn('SparkContext already exists in this scope')  

