# -*- coding: utf-8 -*-
"""
Created on Mon Dec 17 14:59:06 2018

@author: dahaynes
"""

import fiona, geopandas, psycopg2

def CreatePostgreSQLConnection(theHost='localhost', theDB='research', thePort=5432, theUser='david'):
    """
    
    """
    con = psycopg2.connect(host=theHost, database=theDB, user=theUser, port=thePort)
    #cur = con.cursor()
    
    return(con)
    


sqlQuery = """SELECT gid, statefp10, countyfp10, tractce10, blockce blockid10, pop10, geom as geometry, valid 
FROM us_blocks""".replace("\n", "")

pgCon = CreatePostgreSQLConnection()

df = geopandas.GeoDataFrame.from_postgis(sqlQuery, pgCon, geom_col='geometry' )
    
df.to_file(driver="ESRI Shapefile", filename = r"E:\scidb_datasets\vector\us_blocks")    
    