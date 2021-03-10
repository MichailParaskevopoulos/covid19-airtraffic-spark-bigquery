import requests
import json
from bs4 import BeautifulSoup

urlJSON = "https://zenodo.org/record/4485741/export/json"
soup = BeautifulSoup( requests.get(urlJSON).content, "html.parser")
datasetJSON = json.loads( soup.find("pre", style = "white-space: pre-wrap;").contents[0] )
#datasetURLs = [ {"month":entity["key"].split('_')[1],"key":entity["key"]} for entity in datasetJSON["files"] if "csv.gz" in entity["key"] ]
datasetURLs = [ "https://zenodo.org/record/4485741/files/{}".format(entity["key"]) for entity in datasetJSON["files"] if "csv.gz" in entity["key"] ]
datasetURLs = [datasetURLs[0]]
with open('datasetURLs.json', 'w') as json_file:
 json.dump(datasetURLs, json_file)

 
