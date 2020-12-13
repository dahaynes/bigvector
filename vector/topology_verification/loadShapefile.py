# -*- coding: utf-8 -*-


import os, psycopg2, csv
import subprocess, geopandas

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

    
def LoadToPostGreSQL(theShapeFilePath, tableName, schema="big_vector", srid=4326):
    """
    Function for loading shapefiles into a database
    """
    
    dropTable = """ psql -c "DROP TABLE IF EXISTS %s.%s;" """ % (schema, tableName)
    print("Attempting to drop table %s" % (tableName))
    p = subprocess.Popen(dropTable, shell=True)
    p.wait()
        
    postgresCommand = "shp2pgsql -I -s %s %s %s.%s | psql -d research -U david " % (srid, theShapeFilePath, schema, tableName ) 
    print(postgresCommand)
    try:
        p = subprocess.Popen(postgresCommand, shell=True )
        p.wait()
    except:
        dropTable = """ psql -c "DROP TABLE IF EXISTS %s.%s;" """ % (schema, tableName)
        print("Attempting to drop table %s" % (tableName))
        p = subprocess.Popen(dropTable, shell=True)
        p.wait()
        print("Reloading table")
        p = subprocess.Popen(postgresCommand, shell=True )
        p.wait()
        
    

def VerifyTopology(theFIPS):
    """
    Function for modifying the topoloyg
    """
    
    pgCon = CreatePostgreSQLConnection()
    with pgCon:
        try:
            cur = pgCon.cursor()
            for fip in theFIPS:
                theFIPS[fip]
                query = """
                UPDATE big_vector.{name}
                SET valid = ST_IsValid(geom);
                
                UPDATE big_vector.{name}
                SET geom = ST_CollectionExtract(ST_MakeValid(geom),3)
                WHERE ST_ISValid(geom) IS NOT TRUE;
                
                UPDATE big_vector.{name}
                SET valid = ST_IsValid(geom);
                """.format(**allFipsCodes[fip])
                print("Validating and Modifying table %s" % (theFIPS[fip]['name']))
                
                #print(query)
                cur.execute(query.replace("\n", "").replace(";", "; "))
                pgCon.commit()
                sqlQuery = """SELECT gid, statefp10, countyfp10, tractce10, blockce blockid10, geom FROM big_vector.%s; """ % (theFIPS[fip]['name'])
                shpFilePath = r"E:\scidb_datasets\vector\us_blocks\%s.shp" % (theFIPS[fip]['name'])
                print(sqlQuery)
                df = geopandas.GeoDataFrame.from_postgis(sqlQuery, pgCon, geom_col='geom' )  
                df.to_file(driver="ESRI Shapefile", filename=shpFilePath)    
                
        except Exception as e:
            print("Error", e)
            #pgCon.close()


#### SET PARAMETERS #####
fipsCodeFilePath = r"C:\scidb\mongodb_spatial_analysis\fips_codes.txt"
shapeFilesDir = r"E:\scidb_datasets\vector"
#geoDatabase = r"E:\scidb_datasets\vector\vector_editing\vector_editing.gdb"
#arcpy.env.workspace = shapeFilesDir
#arcpy.env.overwriteOutput = True

    
allFipsCodes = GetFIPS(fipsCodeFilePath)


shapefiles = [(os.path.join(shapeFilesDir, f), f) for path, dirs, files in os.walk(shapeFilesDir) for f in files if "shp" in f and not 'xml' in f ]     
#shapefiles = [ (r"%s\%s" % (shapeFilesDir, f), f.split(".")[0]) for f in arcpy.ListFeatureClasses() ]

try:
    
    for c, shapefile in enumerate(shapefiles):
        print("Processing %s of %s" % (c+1, len(shapefiles)) )
        #Shapefiles is a tuple (filepath and shapefileName)
        shapeFilePath, shapeFileName = shapefile
        theKey = shapeFileName.split("_")[1]
        #allFipsCodes[theKey]
        
        featureDataset = allFipsCodes[theKey]['name']
        #LoadToPostGreSQL(shapeFilePath, featureDataset)
            
except:
    print("Something... errored")

VerifyTopology(allFipsCodes)

