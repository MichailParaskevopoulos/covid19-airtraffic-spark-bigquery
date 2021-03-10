cd
git clone https://github.com/MichailParaskevopoulos/covid19_airtraffic_data.git

python3 -m venv env

cd covid19_airtraffic_data
source env/bin/activate

pip install bs4
pip install requests

python datasetJSON.py

while read entry; 
do
    jq -r '.key' <<< $entry
done <<< $(jq -c '.[]' datasetURLs.json)


