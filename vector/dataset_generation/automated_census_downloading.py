# -*- coding: utf-8 -*-
"""
Spyder Editor

This script will automate the downloading process
"""

import requests, zipfile, io, csv
from bs4 import BeautifulSoup

theShapeFileDirectory = r"E:\scidb_datasets\vector"
theUrl = "https://www2.census.gov/geo/tiger/TIGER2010BLKPOPHU/"

r = requests.get(theUrl)
webpage = BeautifulSoup(r.text, "html.parser")

table = webpage.find("table")
# Find all table row (tr rows)
tr = table.find_all("tr")

hrefs = []
#skipping headers
for each_tr in tr[3:]:
    td = each_tr.find_all('td')
    # In each tr rown find each td cell
    for each_td in td:
        #print(each_td.text)
        if each_td.find('a'): hrefs.append(each_td.find('a')['href'])
        
        
  
        

print("Downloading and Extracting files")
for c, h in enumerate(hrefs):
    print("Downloading %s of %s" % (c+1, len(hrefs)) )
    urlZip = "%s/%s" % (theUrl, h)
    theZip = requests.get(urlZip)
    z = zipfile.ZipFile(io.BytesIO(theZip.content))
    z.extractall(theShapeFileDirectory)
