# -*- coding: utf-8 -*-
"""
Spyder Editor

This script will import a series of shapefiles into individual feature classes
Create topology for each and validate.
You must create the geodatabase

"""


import arcpy, csv


def ImportFeatureClass(theShapeFilePath, theGeoDatabaseFeatureDataset, theFeatureClassName):
    """
    
    arcpy.FeatureClassToFeatureClass_conversion(r"c:\scidb\4326\states.shp", r"c:\scidb\vector\vector.gdb\boundaries", "states")
    """
    arcpy.FeatureClassToFeatureClass_conversion(theShapeFilePath, theGeoDatabaseFeatureDataset, theFeatureClassName)

def AddFeatureToTopology(theTopology, theFeatureClassName, xy_rank=1, z_rank=1):
    """
    
    """
    
    arcpy.AddFeatureClassToTopology_management(theTopology, theFeatureClassName, xy_rank, z_rank)


def AddTopologyRules(theTopologyPath, theFeatureClassPath):
    """
    
    arcpy.AddRuleToTopology_management(r"c:\scidb\vector\vector.gdb\boundaries\boundaries_Topology", 'Must Not Have Gaps (Area)', r"c:\scidb\vector\vector.gdb\boundaries\states")
    """
    
    for rule in ['Must Not Have Gaps (Area)', 'Must Not Overlap (Area)']:
        arcpy.AddRuleToTopology_management(theTopologyPath, rule, theFeatureClassPath)
        
    
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





################################################## --- MAIN --- #############################################

#### SET PARAMETERS #####
fipsCodeFilePath = r"C:\scidb\mongodb_spatial_analysis\fips_codes.txt"
shapeFilesDir = r"E:\scidb_datasets\vector"
geoDatabase = r"E:\scidb_datasets\vector\vector_editing\vector_editing.gdb"
arcpy.env.workspace = shapeFilesDir
arcpy.env.overwriteOutput = True

    
allFipsCodes = GetFIPS(fipsCodeFilePath)
shapefiles = [ (r"%s\%s" % (shapeFilesDir, f), f.split(".")[0]) for f in arcpy.ListFeatureClasses() ]

for c, shapefile in enumerate(shapefiles):
    print("Processing %s of %s" % (c+1, len(shapefiles)) )
    #Shapefiles is a tuple (filepath and shapefileName)
    shapeFilePath, shapeFileName = shapefile
    theKey = shapeFileName.split("_")[1]
    #allFipsCodes[theKey]
    
    featureDataset = allFipsCodes[theKey]['name']
    topologyName = "%s_topo" % (featureDataset)
    arcpy.CreateFeatureDataset_management(geoDatabase, featureDataset)
    
    featureDatasetPath = r"%s\%s" % (geoDatabase, featureDataset)
    topologyPath = r"%s\%s\%s" % (geoDatabase, featureDataset, topologyName)
    arcpy.CreateTopology_management(featureDatasetPath, topologyName)  
    featureClassName = featureDataset
    ImportFeatureClass(shapeFilePath, featureDatasetPath, featureClassName)
    featureClassPath = r"%s\%s" % (featureDatasetPath, featureClassName)
    AddFeatureToTopology(topologyPath, featureClassPath)
    AddTopologyRules(topologyPath, featureClassPath)
    print("validating_topology")
    try:
        arcpy.ValidateTopology_management(topologyPath)
    except:
        print("Topology not created %s" % (featureClassName))
#    
    
    
print("Finished")
