import ogr,csv,sys



# shpfile=r'C:\Temp\test.shp' #sys.argv[1]
# csvfile=r'C:\Temp\test.csv' #sys.argv[2]


def WriteMetadata(filePath, theDictionary):
    """
    This function writes out the dictionary as csv
    """
    
    thekeys = list(theDictionary.keys())
    
    with open(filePath, 'w', newline='\n') as csvFile:
        
        for k in theDictionary.keys():
            outputString = "--keyvalue {}={}\n".format(k, theDictionary[k])
            print(outputString)
            csvFile.write(outputString)

def Main(inShpPath, outCSVPath, geomFieldName='geom_text'):
    """
    """
    #Open files
    with open(outCSVPath,'w', newline='\n') as fout:
        dataDefinition = {}
        
        ds=ogr.Open(inShpPath)
        lyr=ds.GetLayer()
        
        #Get field names
        dfn=lyr.GetLayerDefn()
        nfields=dfn.GetFieldCount()
        fields=[]
        for i in range(nfields):
            fields.append(dfn.GetFieldDefn(i).GetName())
            dataDefinition[dfn.GetFieldDefn(i).GetName()] = dfn.GetFieldDefn(i).GetTypeName()
        
        fields.append(geomFieldName)
        dataDefinition[geomFieldName] = "geometry"
        print(fields)
        
        csvWriter = csv.writer(fout, delimiter=';')
        # try:
        #     csvWriter.writeheader() #python 2.7+
        # except:
        csvWriter.writerow(','.join(fields)+'\n')
    
        # Write attributes and kml out to csv
        for feat in lyr:
            attributes=feat.items()
            geom=feat.GetGeometryRef()
            
        
            attributes[geomFieldName]=geom.ExportToWkt()
            csvWriter.writerow(attributes)
    
        #clean up
        metaDataFilePath = "{}.md".format( outCSVPath.split(".")[0])
        WriteMetadata(metaDataFilePath, dataDefinition)
        del csvWriter,lyr,ds


if __name__ == '__main__':
    inShpPath = r"f:\Business_analyst\Data\US_Businesses_WGS84.shp"
    outCSVPath = r"f:\Business_analyst\Data\US_Businesses.csv"
    Main(inShpPath, outCSVPath)