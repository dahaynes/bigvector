# -*- coding: utf-8 -*-
"""
Created on Wed Nov 28 13:56:26 2018

@author: dahaynes
"""


import requests, zipfile, io, csv
from bs4 import BeautifulSoup

theShapeFileDirectory = r"E:\scidb_datasets\vector\synthetic_population"
theUrl = "https://www.epimodels.org//drupal-new/?q=node/163"

r = requests.get(theUrl)
webpage = BeautifulSoup(r.text, "html.parser")

table = webpage.find("table")
# Find all table row (tr rows)
tr = table.find_all("tr")

hrefs = []
#skipping headers
for each_tr in tr:
    td = each_tr.find_all('td')
    # In each tr rown find each td cell
    for each_td in td:
        #print(each_td.text)
        if each_td.find('a'): hrefs.append(each_td.find('a')['href'])
        
        
  
    
print("Downloading and Extracting files")
for c, h in enumerate(hrefs):
    print("Downloading %s of %s" % (c+1, len(hrefs)) )
    theZip = requests.get(h)
    z = zipfile.ZipFile(io.BytesIO(theZip.content))
    z.extractall(theShapeFileDirectory)


