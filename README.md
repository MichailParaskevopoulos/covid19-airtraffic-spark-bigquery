# ✈ A Spark-based ETL Pipeline for the OpenSky and OpenFlights Datasets
### Overview
This project executes a PySpark pipeline to extract and transform the [OpenSky Network 2020](https://zenodo.org/record/4601479#.YE9oqp30mUk) dataset, and load on to Bigquery.   
### Directory
- `envSetup.sh` Shell script that executes `datasetFetch.py`, downloads and unzips the OpenSky and OpenFlights dataset files, and copies the files to Cloud Storage.
- `datasetFetch.py` Python script that uses BeautifulSoup to scrap the file paths for the OpenSky dataset (26 files as of March 2021) and dumps them into a JSON file
- `clusterSetup.sh` Shell script that enables the required Google Cloud APIs, creates a Dataproc cluster, and submits the PySpark jobs
- `pyspark_airtraffic.py` Main PySpark file for the ETL of the OpenSky dataset:
  - The OpenSky dataset files are loaded from the Cloud Storage into a single PySpark dataframe
  - Rows with null values are dropped
  - Date format is modified to be compatible with the BigQuery Date data type
  - The month part of the date of each flight is copied to a new column to be used a partioning field
  - A regex expression is used to extract the airline ICAO identifier from the callsign of each flight
  - The origin and destination coordinates are used to calculate the approximate travel distance for each flight and categorize it either as a short-, medium-, or long-haul flight
  - Reduntant columns are dropped
  - The trasnformed dataframe is loaded into BigQuery partitioned using the month column
- `pyspark_airlines.py` and `pyspark_aircraft_types.py` PySpark files for the ETL of dimensional tables for airlines and aircraft types
