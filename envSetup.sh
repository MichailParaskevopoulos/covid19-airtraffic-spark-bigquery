cd
sudo apt install wget

git clone https://github.com/MichailParaskevopoulos/covid19_airtraffic_data.git

apt-get install python3-venv

cd covid19_airtraffic_data

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
