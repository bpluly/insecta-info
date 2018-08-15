#!/usr/bin/env python
# -*- coding: utf-8 -*-
# mergeCOLtables.py



import sys, os, re, types, configparser, optparse, json
import psycopg2
import logging
import logging.handlers

from collections import defaultdict
from optparse import OptionParser
from datetime import datetime, tzinfo, timedelta
from time import strptime
from optparse import OptionParser
from psycopg2 import extras
import time

NAME = "mergeCOLtables"
CONFIGFILE = "insecta.cnf"
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

class TZ(tzinfo):
    """ Fixup the datetime for isoformat to show the TZ correctly
        ie: 2010-02-09T18:49:14+00:00
        instead of 2010-02-09T18:49:35.271000
        Stolen openly from http://blog.hokkertjies.nl/2008/03/24/isoformat-in-python-24/ """
    def utcoffset(self, dt): return timedelta(seconds=time.timezone)
    def dst(self, dt): return timedelta(0)


def main():
    """
    Process all of the available imported tables that match the pattern in --pattern parameter.
    For each row see if it exists in the core taxon table, if not create it
    otherwise merge the content into the existing row.
    """
    usage = "usage: %prog -f/--file file"

    parser = OptionParser(usage)
    parser.add_option("-p", "--pattern", dest="pattern",
                    help="pattern of import tables to process")

    (options, args) = parser.parse_args()
    stripTags = re.compile(r'<[^<]*?>')
    stripBadChar = re.compile(r'\x19')
    config = configparser.ConfigParser()
    config.read(CONFIGFILE)

    verbose = getboolean(config, 'Configure', 'verbose')
    testing = getboolean(config, 'Configure', 'testing')
    logfilename = getconfigstring(config, 'Log', 'logfilename')

    if verbose == True:
      logger.setLevel(logging.INFO)
    elif testing == True:
      logger.setLevel(logging.DEBUG)
    else:
      logger.setLevel(logging.INFO)

    loghandler = logging.FileHandler(logfilename)
    logger.addHandler(loghandler)



    if testing:
      logger.debug('Test Run')

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
    dbCursor = dbConn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    if options.pattern != None:
      likeString = "'%"+options.pattern+"%'"
    else:
      likeString = ""'%col_%'""

    if testing:
      logger.debug('Selecting '+likeString)
      
    dbCursor.execute("select table_schema, table_name from information_schema.tables WHERE table_name LIKE "+likeString+";")
    importTables = dbCursor.fetchall()

    queryString = ""
    for importTable in importTables:
      tableName = importTable['table_name']
      ProcUpToDateTime = datetime.now()
      logger.info('Import Table '+tableName)
      if ("taxon" in tableName):
        queryString = """Insert into "insecta_taxon" SELECT uuid_generate_v1mc() as "insectaID", "taxonID", identifier, "datasetID", "datasetName", "acceptedNameUsageID", "parentNameUsageID", "taxonomicStatus",
                                  "taxonRank", "verbatimTaxonRank", "scientificName", kingdom, phylum, class, "order", superfamily, family, "genericName", genus, subgenus, "specificEpithet",
                  "infraspecificEpithet", "scientificNameAuthorship", source, "namePublishedIn", "nameAccordingTo", modified, description, "taxonConceptID", "scientificNameID", 
                  "references", "isExtinct","""
      if ("distribution" in tableName):
        queryString = """Insert into "insecta_distribution" SELECT "taxonID", "locationID", locality, "occurrenceStatus", "establishmentMeans", """
      if ("description" in tableName):
        queryString = """Insert into "insecta_description" SELECT "taxonID", locality, """
      if ("reference" in tableName):      
        queryString = """Insert into "insecta_reference" SELECT "taxonID", creator, date, title, description, identifier, type, """
      if ("vernacular" in tableName):
         queryString = """Insert into "insecta_vernacular" SELECT "taxonID", "vernacularName", language, "countryCode", locality, transliteration, """
         
      queryString += "'"+tableName+"'" + 'as "sourceTable", current_date as "dateCreate" FROM '+ '"'+tableName+'"'+';'

      if verbose:
        print("Importing "+tableName)
        logger.info("Importing "+tableName)
      if testing:
        logger.info("Would execute "+queryString)
      else:
        try:
          dbCursor.execute(queryString)
          if verbose:
            print("Imported "+tableName)
            logger.info("Imported "+tableName)
        except psycopg2.OperationalError as e:
          error(e)
          

    dbConn.close()

if __name__ == "__main__":
    main()
