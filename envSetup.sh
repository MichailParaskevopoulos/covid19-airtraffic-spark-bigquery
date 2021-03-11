cd
sudo apt install wget

git clone https://github.com/MichailParaskevopoulos/covid19_airtraffic_data.git

apt-get install python3-venv

cd covid19_airtraffic_data

gsutil cp pyspark_airtraffic.py gs://pyspark_job_files
gsutil cp pyspark_aircraft_types.py gs://pyspark_job_files
gsutil cp pyspark_airlines.py gs://pyspark_job_files

python3 -m venv env
source env/bin/activate
pip install bs4
pip install requests

python datasetJSON.py

gsutil config

export PROJECT_ID='covid19flights'
for i in $(jq -r ". | .[]" datasetURLs.json)
  do
    wget $i
    gunzip $(basename $i)
    gsutil cp $(basename $i .gz) gs://${PROJECT_ID}
    rm $(basename $i .gz)
  done
  
cd
git clone https://github.com/MichailParaskevopoulos/covid19_airtraffic_data.git
cd openflights/data
gsutil cp airlines.dat gs://covid19flights_dim_tables

wget https://opensky-network.org/datasets/metadata/doc8643AircraftTypes.csv
gsutil cp doc8643AircraftTypes.csv gs://covid19flights_dim_tables
