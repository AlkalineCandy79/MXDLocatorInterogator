#-------------------------------------------------------------------------------
# Name:  MXD Locator and Interogator
# Purpose:
#
# Author:      John Spence
#
# Created:      23 Feb 2020
# Modified:
# Modification Purpose:
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
fileprefix = 'Drive_'
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
            findMXD (data_path_MXD, inCsv)

    return


def findMXD(data_path_MXD, inCsv):
# ------------------------------------------------------------------------------
# Process directory looking for *.mxd files
# ------------------------------------------------------------------------------

    mxdfiles = [f for f in listdir(data_path_MXD) if f.endswith(".mxd")]
    for mxd in mxdfiles:
        mxd_service_name = mxd
        name = mxd_service_name.replace('.mxd', '')
        currentDT = datetime.datetime.now()
        print ("Sources Task Started:  " + str(currentDT))
        print ("Source name:  " + name)
        doc = data_path_MXD + '\\' + mxd
        print (doc)

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

        if len(arcpy.mapping.ListLayers(mxd, '', frame))>0:

            print ('\nLayers:')

            for lyr in arcpy.mapping.ListLayers(mxd, '', frame):
                try:
                    print (lyr.name)
                    mxd_lyr_name = lyr.name
                except Exception as error_pull:
                    print "Status:  Failure!"
                    print(error_pull.args[0])
                    mxd_lyr_name = 'N/A'
                try:
                    mxd_lyr_datasetName = lyr.datasetName
                except Exception as error_pull:
                    print "Status:  Failure!"
                    print(error_pull.args[0])
                    mxd_lyr_datasetName = 'N/A'
                try:
                    print (lyr.dataSource)
                    mxd_lyr_source = lyr.dataSource
                except:
                    mxd_lyr_source = 'Can not pull source'
                try:
                    print (lyr.definitionQuery)
                    ldquery = lyr.definitionQuery
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
                    dbName = 'None'
                    verName = 'None'
                else:
                    try:
                        servProp = lyr.serviceProperties
                        layerType = servProp.get('ServiceType', 'None')
                        if layerType == 'SDE':
                            dbName = servProp.get('Database', 'None')
                            serviceName = servProp.get('Service', 'None')
                            if serviceName <> 'N/A':
                                serverDetails = serviceName.split(':')
                                serverName = serverDetails[2]
                            else:
                                serverName = 'N/A'
                            userName = servProp.get('UserName', 'None')
                            authMode = servProp.get('AuthenticationMode', 'None')
                            verName = servProp.get('Version', 'None')
                    except:
                        'Pull failed.'

                #write out to csv
                if not os.path.isfile(inCsv):
                    csvFile = open(inCsv, 'wb')
                    try:
                        writer = csv.writer(csvFile)
                        writer.writerow(('MXD', 'MXD Layer Name', 'Stored Layer Name', 'Definition Query', 'Broken Layer', 'Layer Source', 'Server', 'Database', 'Auth Method',
                                         'User Name', 'Version', 'MXD Path'))
                        writer.writerow((mxd_service_name, mxd_lyr_name, mxd_lyr_datasetName, ldquery, mxd_lyr_isBroken, mxd_lyr_source, serverName, dbName, authMode,
                                         userName, verName, data_path_MXD))
                    except Exception as error_write:
                        print "Status:  Failure!"
                        print(error_write.args[0])
                        print "error writing first row of csv"
                else:
                    csvFile = open(inCsv, 'ab')
                    try:
                        writer = csv.writer(csvFile)
                        writer.writerow((mxd_service_name, mxd_lyr_name, mxd_lyr_datasetName, ldquery, mxd_lyr_isBroken, mxd_lyr_source, serverName, dbName, authMode,
                                         userName, verName, data_path_MXD))
                    except Exception as error_write:
                        print "Status:  Failure!"
                        print(error_write.args[0])

        else:
            #write out to csv
            if not os.path.isfile(inCsv):
                csvFile = open(inCsv, 'wb')
                try:
                    writer = csv.writer(csvFile)
                    writer.writerow(('MXD', 'MXD Layer Name', 'Stored Layer Name', 'Definition Query', 'Broken Layer', 'Layer Source', 'Server', 'Database', 'Auth Method',
                                     'User Name', 'Version', 'MXD Path'))
                    writer.writerow((mxd_service_name, 'None', 'None', 'None', 'None', 'None', 'None', 'None', 'None',
                                     'None', 'None', data_path_MXD))
                except Exception as error_write:
                    print "Status:  Failure!"
                    print(error_write.args[0])
                    print "error writing first row of csv"
            else:
                csvFile = open(inCsv, 'ab')
                try:
                    writer = csv.writer(csvFile)
                    writer.writerow((mxd_service_name, 'None', 'None', 'None', 'None', 'None', 'None', 'None', 'None',
                                     'None', 'None', data_path_MXD))
                except Exception as error_write:
                    print "Status:  Failure!"
                    print(error_write.args[0])

    del mxd

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