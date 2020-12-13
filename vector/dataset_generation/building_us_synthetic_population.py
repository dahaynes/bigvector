# -*- coding: utf-8 -*-

import csv, os
import subprocess, psycopg2


fipsCodeFilePath = r"C:\scidb\mongodb_spatial_analysis\fips_codes.txt"
syntheticFilesDir = r"E:\scidb_datasets\vector\synthetic_population"
#geoDatabase = r"E:\scidb_datasets\vector\vector_editing\vector_editing.gdb"
#arcpy.env.workspace = shapeFilesDir
#arcpy.env.overwriteOutput = True


def GetFIPS(theFilePath):
    """
    This function takes an input file path and returns a dictionary object.
    Input: file path (expecting fields: StateName, Abbreviation, fips)
    Ouput: Dictionary with fips (str) for keys.
    """
    with open(theFilePath, 'r') as fin:
        theFile = csv.DictReader(fin, delimiter=",")
        fips = {line['fips']: {'name': line['StateName'], 'fips': int(line['fips']), 'abbreviation': line['Abbreviation'] } for line in theFile }
    
    return(fips)

def CreatePostgreSQLConnection(theHost='localhost', theDB='research', thePort=5432, theUser='david'):
    """
    
    """
    con = psycopg2.connect(host=theHost, database=theDB, user=theUser, port=thePort)
    #cur = con.cursor()
    
    return(con)
    
def CreateTable(tableName):
    """
    
    """

    dropStatement = """ psql -d research -U david -c "DROP TABLE IF EXISTS %s;" """ % (tableName)
    
    print("Attempting to drop table %s" % (tableName))
    p = subprocess.Popen(dropStatement, shell=True)
    p.wait()
    
    createStatement = """ psql -d research -U david -c "CREATE TABLE %s (sp_id bigint, serialno bigint, stcotrbg bigint, hh_race integer, hh_income float, hh_size integer, hh_age integer, latitude double precision, longitude double precision)" """ % (tableName)
    p = subprocess.Popen(createStatement, shell=True)
    p.wait()


def AddGeometry(pgTable):
    """
    
    """
    pgCon = CreatePostgreSQLConnection()
    pgCur = pgCon.cursor()
    alterStatement = "ALTER TABLE %s ADD COLUMN geom geometry;" % (pgTable)
    pgCur.execute(alterStatement )
    createGeom = "UPDATE %s SET geom = ST_PointFromText('Point('|| longitude || ' ' || latitude || ')', 4326 );" % (pgTable) 
    pgCur.execute(createGeom)
    
    
    
allFipsCodes = GetFIPS(fipsCodeFilePath)


tableNames = [('big_vector.us_synthetic_households', "households.txt"), ('big_vector.us_synthetic_population','people.txt')]
for tableName, fileIdentifier in tableNames:
    syntheticHouseholdsPath = [os.path.join(syntheticFilesDir, f) for path, dirs, files in os.walk(syntheticFilesDir) for f in files if fileIdentifier in f ]     
    CreateTable(tableName)
    for s in syntheticHouseholdsPath:
        if s.split("\\")[-1].split("_")[2] in allFipsCodes.keys():
            loadStatement = """ psql -d research -U david -c "COPY %s FROM '%s' WITH CSV HEADER delimiter ',';" """ % (tableName, s)
            print(loadStatement)
            p = subprocess.Popen(loadStatement, shell=True)
            p.wait()
            
AddGeometry(tableNames[0][0])    
    
#    with open(s, 'r') as fin:
#        theCSV = csv.DictReader(fin, delimiter=",")
#        the