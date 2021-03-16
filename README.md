# ✈ A Spark-based ETL Pipeline for the OpenSky and OpenFlights Datasets
### Motivation
The [OpenSky](https://zenodo.org/record/4601479#.YE9oqp30mUk) dataset spans flights since January 2019 has been instrumental in analyzing the effect of COVID-19 on the aviation industry. This repository contains a PySpark pipeline to extract and transform the OpenSky dataset, and load it on to Bigquery as a fact table. PySpark pipelines are also included to process data from the [OpenFlights](https://github.com/jpatokal/openflights) dataset to construct dimensional tables with airline and aircraft data. Combined together in a single Data Warehouse, the OpenSky and OpenFlights datasets not only can be used to analyze trends in airtraffic during the COVID-19 pandemic, but also to explore patterns between different airlines and aircraft types.
### Description of Files
- `envSetup.sh` Shell script that executes `datasetFetch.py`, downloads and unzips the OpenSky and OpenFlights dataset files, and copies the files to Cloud Storage.
- `datasetFetch.py` Python script that uses BeautifulSoup to scrap the file paths for the OpenSky dataset (26 files as of March 2021) and dumps them into a JSON file
- `clusterSetup.sh` Shell script that enables the required Google Cloud APIs, creates a Dataproc cluster, and submits the PySpark jobs
- `pyspark_airtraffic.py` Main PySpark file for the ETL of the OpenSky dataset:
  - The OpenSky dataset files are loaded from the Cloud Storage into a single PySpark dataframe
  - Rows with null values are dropped
  - Date format is modified to be compatible with the BigQuery date data type
  - The month part of the date of each flight is copied to a new column to be used a partioning field
  - A regex expression is used to extract the airline ICAO identifier from the callsign of each flight
  - The origin and destination coordinates are used to calculate the approximate travel distance for each flight and categorize it either as a short-, medium-, or long-haul flight
  - Reduntant columns are dropped
  - The trasnformed dataframe is loaded into BigQuery as a partitioned table based on the month column to imrpove query performance
- `pyspark_airlines.py` and `pyspark_aircraft_types.py` PySpark files for the ETL of dimensional tables for airlines and aircraft types
### Data Sources and Credits
<b>OpenSky:</b><br>
Data from the [OpenSky Network 2020](https://zenodo.org/record/4601479#.YE9oqp30mUk) were used to construct the airtraffic and aircraft type tables.

Matthias Schäfer, Martin Strohmeier, Vincent Lenders, Ivan Martinovic and Matthias Wilhelm.
"Bringing Up OpenSky: A Large-scale ADS-B Sensor Network for Research".
In Proceedings of the 13th IEEE/ACM International Symposium on Information Processing in Sensor Networks (IPSN), pages 83-94, April 2014.

Xavier Olive.
"traffic, a toolbox for processing and analysing air traffic data."
Journal of Open Source Software 4(39), July 2019.

<b>OpenFlights:</b><br>
Data from [OpenFlights](https://github.com/jpatokal/openflights) were used to construct the airlines dimensional table. 
