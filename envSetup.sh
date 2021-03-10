cd
sudo apt install wget

git clone https://github.com/MichailParaskevopoulos/covid19_airtraffic_data.git

python3 -m venv env

cd covid19_airtraffic_data

source env/bin/activate
pip install bs4
pip install requests

python datasetJSON.py

export PROJECT_ID='covid19flights'
for i in $(jq -r ". | .[]" datasetURLs.json)
  do
    wget $i
    gunzip $(basename $i)
    gsutil cp $(basename $i .gz) gs://${PROJECT_ID}/covid19_airtraffic_data
    rm $(basename $i .gz)
  done
