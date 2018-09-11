#!/usr/bin/env python
# -*- coding: utf-8 -*-
# importNHME.py



import sys, os, re, types, configparser, optparse, json, csv
import psycopg2
import logging
import logging.handlers

from collections import defaultdict
from optparse import OptionParser
from datetime import datetime, tzinfo, timedelta
from time import strptime
from psycopg2 import extras
import time

NAME = "importNHME"
CONFIGFILE = "insecta_NHM.cnf"
logger = logging.getLogger('Insecta.mergeCOLtables')
testing = False
verbose = False

def error(s):
    """output an error and take an early bath."""
    print("%s: %s" % (NAME, s),file=sys.stderr)
    if (logger):
      logger.error('%s: %s' % (NAME, s))
    sys.exit(1)

def getconfigstring(config, section, key):
    """helper function to let us include error trapping"""
    errorMessage = "Error: "
    keyvalue = ""
    try:
      keyvalue = config.get(section, key)
    except configparser.NoSectionError:
      error("Error: Unable to find section %s." % (section))
    except configparser.NoOptionError:
      error("Error: Unable to find option %s." % (key))

    return keyvalue

def getconfigint(config, section, key):
    """helper function to let us include error trapping"""
    errorMessage = "Error: "
    try:
      keyvalue = config.getint(section, key)
    except configparser.NoSectionError:
      error("Error: Unable to find section %s." % (section))
    except configparser.NoOptionError:
      error("Error: Unable to find option %s." % (key))

    return keyvalue

def getboolean(config, section, key):
    try:
        keyvalue = config.getboolean(section, key)
    except configparser.NoSectionError:
      error("Error: Unable to find section %s." % (section))
    except configparser.NoOptionError:
      error("Error: Unable to find option %s." % (key))

    return keyvalue

def putconfigstring(config, section, key):
    """helper function to write a section key"""
    try:
        config.set(section, key)
    except configparser.NoSectionError:
      error("Error: Unable to find section %s." % (section))

def updateConfig(config, ProcUpToDateTime):
    config.set('Log', 'PrevProcDateTime', ProcUpToDateTime.isoformat())
    configfile = open(CONFIGFILE, 'wb')
    config.write(configfile)

def incSecond(currentDate):
    """ increment the currentDate by 1 second and return """
    delta =  timedelta(seconds=1)
    return currentDate + delta

def quote(sourceString):
    """ wrap the sourceString in quotes
    """
    return "'"+sourceString+"'"
    
def doublequote(sourceString):
    """ wrap the sourceString in quotes
    """
    return '"'+sourceString+'"'    

def makeValue(inList):
    """ return the first string in the list or "" if its empty
    """
    if inList[0] == None:
      return ""
    else :
      return inList[0]
def makeBoolValue(inList):
    """ return True or False depending if inList exists at all
    """
    if not inList:
      return False
    else:
      return True

def rowString(row):
    valueString = ""
    for k, value in row.items():
      try:
        valueString += value+","
      except:
        error("Failed on"+value)
    return(valueString)


class TZ(tzinfo):
    """ Fixup the datetime for isoformat to show the TZ correctly
        ie: 2010-02-09T18:49:14+00:00
        instead of 2010-02-09T18:49:35.271000
        Stolen openly from http://blog.hokkertjies.nl/2008/03/24/isoformat-in-python-24/ """
    def utcoffset(self, dt): return timedelta(seconds=time.timezone)
    def dst(self, dt): return timedelta(0)


def main():
    """
    Import the Etymology CSV file, handling JSON and any odd strings
    """
    usage = "usage: %prog -f/--file file"
    csvFile = ""
    tableName = ""
    createTable = False
    

    parser = OptionParser(usage)
    parser.add_option("-p", "--pattern", dest="pattern",
                    help="pattern of import tables to process")
    parser.add_option("-f","--file", dest="csvFile",
                    help="file to import.")
    parser.add_option("-t","--table", dest="tableName",
                    help="table to import into.")
    parser.add_option("-c","--create", action = 'store_true', default = False, dest="createTable",
                    help="Create the table to import into.")
                    

    (options, args) = parser.parse_args()
    stripTags = re.compile(r'<[^<]*?>')
    stripBadChar = re.compile(r'\x19')
    config = configparser.ConfigParser()
    config.read(CONFIGFILE)

    verbose = getboolean(config, 'Configure', 'verbose')
    testing = getboolean(config, 'Configure', 'testing')
    logfilename = getconfigstring(config, 'Log', 'logfilename')

    print("csvFile = "+options.csvFile)

    if verbose == True:
      logger.setLevel(logging.INFO)
    elif testing == True:
      logger.setLevel(logging.DEBUG)
    else:
      logger.setLevel(logging.INFO)

    loghandler = logging.FileHandler(logfilename)
    logger.addHandler(loghandler)



    if testing:
      logger.info('Test Run')

    PrevProcDateTime = getconfigstring(config, 'Log', 'PrevProcDateTime')
    db = getconfigstring(config,'Database', 'host')
    dbport = getconfigstring(config,'Database', 'port')
    dbname = getconfigstring(config,'Database', 'name')
    dbuser = getconfigstring(config,'Database', 'user')
    dbpassword = getconfigstring(config,'Database', 'password')

#
# Process<input type="radio" name="" value="" />
#
    dbConnectionString = "host="+quote(db)+" port="+quote(dbport)+" dbname="+quote(dbname)+" user="+quote(dbuser)+" password="+quote(dbpassword)
    if testing:
      logger.debug('Connection string '+dbConnectionString)
      
    try:
      dbConn = psycopg2.connect(dbConnectionString)
    except psycopg2.OperationalError as e:
      error(e)
    else:
      print(dbname,"connected.")
    dbCursor = dbConn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
# open the csv file

    with open(options.csvFile) as csvfile:
      NHMReader = csv.reader(csvfile,delimiter=',',quotechar='"')
      fieldList = ""
      allFields = next(NHMReader)
      for field in allFields:
        fieldList += doublequote(field)+","
      fieldList = fieldList.rstrip(',')
      insertStringbase = """INSERT INTO "NHM_Occurrence" ("""+fieldList+") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
      print(insertStringbase)
      for row in NHMReader:
        if testing == False:
          try:
            dbCursor.execute(insertStringbase,tuple(row))
            print(row[1])
          except psycopg2.OperationalError as e:
            error(e)
            
        else:
          print("Would Insert using ")
          print(*row,sep=',')
          
      if testing == False:
        try:
          dbConn.commit()
        except psycopg2.OperationalError as e:
          error(e)


    dbConn.close()
    print("Merge of tables complete.")

if __name__ == "__main__":
    main()
