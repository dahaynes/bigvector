# -*- coding: utf-8 -*-
"""
Spyder Editor

This script will partition load a partitioned dataset into the database.
"""

import fiona
import psycopg2
from collections import Counter, OrderedDict
from shapely.geometry import shape

fileIn = r"/media/sf_data/scidb_datasets/vector/states_hash.shp"


def CreatePostgreSQLConnection(theHost='localhost', theDB='research', thePort=5434, theUser='david', ):
    """
    
    """
    con = psycopg2.connect(host=theHost, database=theDB, user=theUser, port=thePort, password='david')
    
    
    return(con)

def CreateTable(con, ddl, tableName='None', partition="None", childTable=False, partitionValues="None"):
    """
    This function will create a table
    """
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS %s" % (tableName))
    
    if tableName and childTable and partitionValues:
        query = "CREATE TABLE %s %s %s ;" % (tableName, partition, partitionValues)
    elif tableName and partition:
        query = "CREATE TABLE %s ( %s ) %s ;" % (tableName, ddl, partition)        
    elif tableName:
        query = "CREATE TABLE %s ( %s );" % (tableName, ddl)
    else:
        query = ddl
        
    try:
        cur.execute(query)
    except psycopg2.DatabaseError as e:
        print(cur.query)
        print("Error creating table", e)


def CreateIndex(con, tableName, indexType, indexField ):
    """
    
    """

    cur = con.cursor()
    query = """ CREATE INDEX ON %s USING %s ("%s"); """ % (tableName, indexType, indexField)
    
    try:
        cur.execute(query)
    except psycopg2.DatabaseError as e:
        print("Error adding index to child table", e)

        
def AddGeometryField(con, tableName):
    """
    
    """
    
    cur = con.cursor()
    query = "ALTER TABLE %s ADD COLUMN geom geometry;" % (tableName)

    try:
        cur.execute(query)
    except Exception as e:
        print("Error adding geometry field to table", e)
        
def ReadAttributes(features):
    """
    
    """
    theKeys = features[0]['properties'].keys()

    attributeDict = OrderedDict()
    for k in theKeys:
        items = [ f['properties'][k] for f in features]
        attributeDict[k] = items
    
    return attributeDict
        
def GenerateDDL(attributeDict):
    """
    This function creates PostgreSQL specific DDL 
    
    Input: attributeDict {'field1': [f1, f2, f3,..], 'field2': [f1,f2,f3,...] }
    
    """    
    
    for k in attributeDict.keys():
        attributeDict[k]
    pass
    
def LoadShapefile(con, tableName, features, srid):
    """
    
    """

    cur = con.cursor()
    #propertyDict = ReadAttributes(features)
    
    for feature in features:
        theGeom = shape(feature['geometry'])
        
        fieldValues = str(tuple(feature['properties'].values() )  )
        geomText = str("ST_GeomFromText('%s',%s)" % (theGeom.wkt, srid))
        fieldValuesText = fieldValues.replace("(", "").replace(")", "") + ", " + geomText
        #fieldValuesText = " ".join([fieldValues, geomText])
        
        #fieldNames = list(features['properties'].keys())
        #fieldNames.append('geom')
        fieldNames = tuple(feature['properties'].keys()) + ('geom',)
        fieldNamesText = str(fieldNames).replace("'", '"')
        #fieldNames = tuple(fieldNames)
        #[feature['properties'][propertyKey] for propertyKey in feature['properties'].keys() ]
        query = "INSERT INTO %s %s VALUES (%s) ;" % (tableName, fieldNamesText , fieldValuesText)
        
        #print(query)
        try:
            cur.execute(query)
        except psycopg2.DatabaseError as e:
            print(cur.query, e)
            break
    
    return(query)
        

def ReadShapefile(fileIn, tableName, partitionField, partitionType ="LIST"):
    
    v = fiona.open(fileIn)

    if 'init' in v.crs.keys():
        epsgText = v.crs['init']
        epsgCode = epsgText.split(":")[1]
        
        tableDDL = "name text, hash_6 varchar(7), hash_2 varchar(3), partition_ integer, geom geometry"
        tablePartition = "PARTITION BY %s (%s)" % (partitionType, partitionField)
        
    #pgCon = CreatePostgreSQLConnection()
    
    with CreatePostgreSQLConnection() as pgCon:
        CreateTable(pgCon, tableDDL, tableName=tableName, partition=tablePartition)
#    for feature in v:
    if partitionField in v[0]['properties'].keys() and partitionType.upper() == "LIST":
        partitionedFeatures = [feature['properties'][partitionField] for feature in v]
        partitions = Counter(partitionedFeatures)
        #numPartitions = len(partitions)
        
        for p, partition in enumerate(partitions):
            #Building statements to create child tables.
            partitionTableName = "%s_%s" % (tableName,p)
            partitionTable = "partition OF %s " % (tableName)
            
            #This needs to be better for determining if the parition field is a string or integer.
            if isinstance(partition, str):
                partitionListValue = "FOR VALUES IN ('%s')" % (partition,)
            else:
                partitionListValue = "FOR VALUES IN (%s)" % (int(partition),)
            
            try:                
                CreateTable(pgCon, ddl="None", tableName=partitionTableName, partition=partitionTable, childTable=True, partitionValues= partitionListValue)
            except:
                print("Error creating child table %s" % (partitionTableName))
        
        h = LoadShapefile(pgCon, tableName, v, epsgCode)

        for p, partition in enumerate(partitions):
            #Building statements to create child tables.
            partitionTableName = "%s_%s" % (tableName,p)
            CreateIndex(pgCon, partitionTableName, "GIST", "geom" )
            
        pgCon.commit()
        return h
        

    else:
        print("Error")


def argument_parser():
    """
    Parse arguments and return Arguments
    """
    import argparse
    
    parser = argparse.ArgumentParser(description= "Script for loading partitioned data into Postgresql")  
    parser.add_argument("-shp", required =True, help="Output timing results into CSV file", dest="shapePath", default="None")  
    parser.add_argument("-s", required =True, help="Output timing results into CSV file", dest="srid", default="None")  
    parser.add_argument("-I", required =True, help="Output timing results into CSV file", dest="index", default="None")  
    parser.add_argument("-t", required =True, help="Output timing results into CSV file", dest="tableName", default="None")  

postGISTable = 'states_partitioned'
k = ReadShapefile(fileIn, postGISTable, 'hash_2')

#con = psycopg2.connect(host='localhost', database='research', user='david', port=5434, password='david')
#
#if __name__ == '__main__':
#    args = argument_parser().parse_args()