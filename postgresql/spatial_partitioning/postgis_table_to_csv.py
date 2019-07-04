# -*- coding: utf-8 -*-
"""
Created on Wed Jul  3 10:37:39 2019

Script will allow a user to specify the table they want to have written to a CSV
CSV will be semicolon separated.

@author: dahaynes
"""

import psycopg2, csv
from psycopg2 import extras
from collections import OrderedDict




def CreateConnection(theConnectionDict):
    """
    This method will get a connection. Need to make sure that the DB is set correctly.
    """
    
    connection = psycopg2.connect(host=theConnectionDict['host'], database=theConnectionDict['db'], user=theConnectionDict['user'], port=theConnectionDict['port'], password='haynes')

    return connection

def ExecuteQuery(pgCon, query):
    
    pgCur = extras.RealDictCursor(pgCon)
    try:
        pgCur.execute(query)
    except:
        print("ERROR...", query)
    
    return pgCur

def WriteFile(filePath, results, theFields):
    """
    This function writes out the results to a csv
    CSV is delimited with semicolon
    """
    
    with open(filePath, 'w', newline='\n') as f:
        theWriter = csv.DictWriter(f, fieldnames=theFields, delimiter=";")
        theWriter.writeheader()
        try:
            for r in results:
                theWriter.writerow(r) 
        except:
            print("ERROR.... ", r)
    
def GetColumnNames(tableName):
    """
    SELECT *
    FROM information_schema.columns
    WHERE table_schema = 'your_schema'
    AND table_name   = 'your_table'
    
    """
    if "." in tableName:
        psqlSchema, psqlTable = tableName.split(".")
        return "SELECT * FROM information_schema.columns WHERE table_schema = '{}' AND table_name = '{}';".format(psqlSchema, psqlTable)
    else:
        return "SELECT * FROM information_schema.columns WHERE table_schema = '{}' AND table_name = '{}';".format('public', psqlTable)

    

def argument_parser():
    """
    Parse arguments and return Arguments
    """
    import argparse

    parser = argparse.ArgumentParser(description= "Module for removing data from a PostgreSQL database to a CSV")    
    
    parser.add_argument("-t", required=True, type=str, help="Name of CituSDB table", dest="tableName")
    parser.add_argument("-f", required=False, type=str, help="Field Name for geometry", dest="geomField")
    
    #All of the required connection informaiton
    parser.add_argument("-d", required=True, type=str, help="Name of database", dest="db")
    parser.add_argument("-host", required=True, type=str, help="Host of database", dest="host")
    parser.add_argument("-p", required=True, type=int, help="port number of citusDB", dest="port")   
    parser.add_argument("-u", required=True, type=str, help="db username", dest="user")
    
    parser.add_argument("-o", required=False, type=str, help="The file path of the csv", dest="csv", default=None)

    return parser    


if __name__ == '__main__':
    args = argument_parser().parse_args()
#    myTable = 'big_vector.randpoints_10thousand_hashed'
#    geomField = 'geom'
#    outFile = r'e:\spatial_datasets\randpoints.csv'
    myConnection = {"host": args.host, "db": args.db, "port": args.port, "user": args.user}
#    myConnection = {"host": "localhost", "db": "research", "port": 5432, "user": "david"}
    psqlCon = CreateConnection(myConnection)
    queryResult = ExecuteQuery(psqlCon, GetColumnNames(args.tableName))
    tableColumns = [ r['column_name'] for r in queryResult ] 
    
    if args.geomField in tableColumns:
        csvFields = [c  for c in tableColumns]
        csvFields[csvFields.index(args.geomField)] = 'geom_text'
        tableColumns[tableColumns.index(args.geomField)] = 'ST_AsText({}) as geom_text'.format(args.geomField)
        
        finalQuery = "SELECT {} FROM {} ".format(", ".join(tableColumns), args.tableName)
        print("Querying Database")
        psqlResults = ExecuteQuery(psqlCon, finalQuery)
        print("Writing CSV")
        WriteFile(args.csv, psqlResults, csvFields)
    else:
        print("ERROR can't find geometry column name")
        print("Entered geom field: {}".format(args.geomField) )
        print("Available fields: {}".format(", ".join(tableColumns)))


    psqlCon.close()
    