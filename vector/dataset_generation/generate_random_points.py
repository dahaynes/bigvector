##
##
##

import psycopg2

def CreatePostgreSQLConnection(theHost='localhost', theDB='research', thePort=5432, theUser='david'):
    """
    
    """
    con = psycopg2.connect(host=theHost, database=theDB, user=theUser, port=thePort)
    #cur = con.cursor()
    
    return(con)
    
    

def CreateRandomPoints(tableName, numLoops):
    """
    numLoops is in numbers of 10 Million inserted records
    
    """
    pgCon = CreatePostgreSQLConnection()
    
    with pgCon:
        pgCur = pgCon.cursor()
        pgCur.execute("DROP TABLE IF EXISTS %s;"% tableName)
        pgCur.execute("CREATE TABLE %s (id bigint, geom geometry)" % (tableName))
        
        for i in range(numLoops):
                
            query = """
            with data as
            (
            SELECT ST_Dump(ST_GeneratePoints(geom, 10000100)) as dump
            FROM big_vector.continent
            )
            INSERT INTO %s
            SELECT (dump).path[1] as id, (dump).geom as geom
            FROM data
            LIMIT 10000000""" % (tableName)
            pgCur.execute(query)
            pgCon.commit()
            print("Inserted %s of %s" % (i+1, numLoops) )



dataGeneratingInfo = (("big_vector.randpoints_10m", 1), ("big_vector.randpoints_50m", 5), ("big_vector.randpoints_100m", 10))
for table, loops in dataGeneratingInfo:
    print(table, loops)
    CreateRandomPoints(table, loops)

    