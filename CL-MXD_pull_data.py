#-------------------------------------------------------------------------------
# Name:  MXD Locator and Interogator
# Purpose:
#
# Author:      John Spence
#
# Created:  23 February 2020
# Modified: 24 February 2020
# Modification Purpose: Call me bug hunter.  
#
#
#
#-------------------------------------------------------------------------------


# 888888888888888888888888888888888888888888888888888888888888888888888888888888
# ------------------------------- Configuration --------------------------------
# Set the data store location to search.
#
# ------------------------------- Dependencies ---------------------------------
#
#
# 888888888888888888888888888888888888888888888888888888888888888888888888888888
#
# Data Store Location Where MXDs Reside
data_store_path_MXD = r'E:\ScriptTesting'
#
# Where the Output will reside from the search
inCsv = r'E:\ScriptTesting'
#
# File Name Pre-Fix for the Output
fileprefix = 'VDrive_'
#
# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

# Import Python libraries
import arcpy, os, sys, datetime, csv
import xml.dom.minidom as DOM
import pandas as pd
import collections
from os import listdir
from os.path import isfile, join

arcpy.env.overwriteOutput = False

#-------------------------------------------------------------------------------
#
#
#                                 FUNCTIONS
#
#
#-------------------------------------------------------------------------------

def dirWalk():
# ------------------------------------------------------------------------------
# Walks through the target directory and sub directories
# ------------------------------------------------------------------------------

    for dirpath, dirnames, filenames in os.walk(data_store_path_MXD):

        for filenames in [f for f in filenames if f.endswith(".mxd")]:
            print ("Target found:  " + os.path.join(dirpath,filenames))
            data_path_MXD = dirpath

            doc = filenames

            mxd_service_name = doc
            name = mxd_service_name.replace('.mxd', '')
            currentDT = datetime.datetime.now()
            print ("Sources Task Started:  " + str(currentDT))
            print ("Source name:  " + name)
            doc = data_path_MXD + '\\' + doc
            print ("File path:  " + doc)

            mxdInfo(doc, mxd_service_name, inCsv, data_path_MXD)
            currentDT = datetime.datetime.now()
            print ("Sources Task Completed:  " + str(currentDT) + "\n")

    return

def mxdInfo(doc, mxd_service_name, inCsv, data_path_MXD):
# ------------------------------------------------------------------------------
# Dig and snag info from MXD
# ------------------------------------------------------------------------------

    layer = []

    mxd = arcpy.mapping.MapDocument(doc)

    a = collections.OrderedDict()

    df = arcpy.mapping.ListDataFrames(mxd)

    for frame in df:

        if frame <> None:

            mxd_frame = frame.name

            print ('Frame:  ' + mxd_frame)

            if len(arcpy.mapping.ListLayers(mxd, '', frame))>0:

                print ('\nLayers:')

                for lyr in arcpy.mapping.ListLayers(mxd, '', frame):
                    try:
                        print (lyr.name)
                        mxd_lyr_name = lyr.name
                        print ('Pulled Layer Name')
                    except Exception as error_pull:
                        print "Status:  Failure!"
                        print(error_pull.args[0])
                        mxd_lyr_name = 'N/A'
                    try:
                        mxd_lyr_datasetName = lyr.datasetName
                        print (lyr.datasetName)
                        print ('Pulled Data Set Name')
                    except Exception as error_pull:
                        print "Status:  Failure!"
                        print(error_pull.args[0])
                        mxd_lyr_datasetName = 'N/A'
                    try:
                        print (lyr.dataSource)
                        print ('Pulled Data Source')
                        mxd_lyr_source = lyr.dataSource
                    except:
                        mxd_lyr_source = 'Can not pull source'
                    try:
                        print (lyr.definitionQuery)
                        ldquery = lyr.definitionQuery
                        if ldquery == None:
                            ldquery = ''
                        print ('Pulled Def Query')
                    except:
                        ldquery = 'None'
                    print (lyr.isBroken)
                    mxd_lyr_isBroken = lyr.isBroken

                    if lyr.isBroken == True:
                        serverName = 'None'
                        serviceName = 'None'
                        dbName = 'None'
                        userName = 'None'
                        authMode = 'None'
                        verName = 'None'
                        mxd_lyr_isBroken = 'Yes'
                    else:
                        try:
                            servProp = lyr.serviceProperties
                        except:
                            servProp = 'None'
                        try:
                            layerType = servProp.get('ServiceType', 'None')
                            if layerType == 'SDE':
                                dbName = servProp.get('Database', 'None')
                                serviceName = servProp.get('Service', 'None')
                                if serviceName <> 'N/A':
                                    print ('Server Details')
                                    serverDetails = serviceName.split(':')
                                    serverName = serverDetails[2]
                                else:
                                    serverName = 'N/A'
                        except:
                            layerType = 'None'
                            dbName = 'None'
                            serviceName = 'None'
                            serverDetails = 'None'
                        try:
                            userName = servProp.get('UserName', 'None')
                        except:
                            userName = 'None'
                        try:
                            authMode = servProp.get('AuthenticationMode', 'None')
                        except:
                            authMode = 'None'
                        try:
                            verName = servProp.get('Version', 'None')
                        except:
                            verName = 'None'
                        mxd_lyr_isBroken = 'No'


                    #write out to csv
                    if not os.path.isfile(inCsv):
                        csvFile = open(inCsv, 'wb')
                        try:
                            writer = csv.writer(csvFile)
                            writer.writerow(('MXD', 'Data Frame', 'MXD Layer Name', 'Stored Layer Name', 'Definition Query', 'Broken Layer', 'Layer Source', 'Server', 'Database', 'Auth Method',
                                             'User Name', 'Version', 'MXD Path', 'Completed'))
                            writer.writerow((mxd_service_name, mxd_frame, mxd_lyr_name, mxd_lyr_datasetName, ldquery, mxd_lyr_isBroken, mxd_lyr_source, serverName, dbName, authMode,
                                             userName, verName, data_path_MXD, str(datetime.datetime.now())))
                        except Exception as error_write:
                            print "Status:  Failure!"
                            print(error_write.args[0])
                            print "error writing first row of csv"
                    else:
                        csvFile = open(inCsv, 'ab')
                        try:
                            writer = csv.writer(csvFile)
                            writer.writerow((mxd_service_name, mxd_frame, mxd_lyr_name, mxd_lyr_datasetName, ldquery, mxd_lyr_isBroken, mxd_lyr_source, serverName, dbName, authMode,
                                             userName, verName, data_path_MXD, str(datetime.datetime.now())))
                        except Exception as error_write:
                            print "Status:  Failure!"
                            print(error_write.args[0])

            else:
                #write out to csv
                print ('Bypassed')
                if not os.path.isfile(inCsv):
                    csvFile = open(inCsv, 'wb')
                    try:
                        writer = csv.writer(csvFile)
                        writer.writerow(('MXD', 'Data Frame', 'MXD Layer Name', 'Stored Layer Name', 'Definition Query', 'Broken Layer', 'Layer Source', 'Server', 'Database', 'Auth Method',
                                         'User Name', 'Version', 'MXD Path', 'Completed'))
                        writer.writerow((mxd_service_name, mxd_frame, 'None', 'None', 'None', 'None', 'None', 'None', 'None', 'None',
                                         'None', 'None', data_path_MXD, str(datetime.datetime.now())))
                    except Exception as error_write:
                        print "Status:  Failure!"
                        print(error_write.args[0])
                        print "error writing first row of csv"
                else:
                    csvFile = open(inCsv, 'ab')
                    try:
                        writer = csv.writer(csvFile)
                        writer.writerow((mxd_service_name, mxd_frame, 'None', 'None', 'None', 'None', 'None', 'None', 'None', 'None',
                                         'None', 'None', data_path_MXD, str(datetime.datetime.now())))
                    except Exception as error_write:
                        print "Status:  Failure!"
                        print(error_write.args[0])

    return

#-------------------------------------------------------------------------------
#
#
#                                 MAIN SCRIPT
#
#
#-------------------------------------------------------------------------------

inCsv = inCsv + '\\' + fileprefix + 'MXDSources_' + str(datetime.datetime.now().date()) + '.csv'

dirWalk()