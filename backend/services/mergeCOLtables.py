#!/usr/bin/env python
# -*- coding: utf-8 -*-
# mergeCOLtables.py



import sys, os, re, types, tempfile, csv, httplib, xmlrpclib, ConfigParser, syslog, optparse, json
import psycopg2
import logging
import logging.handlers

from collections import defaultdict
from optparse import OptionParser
from datetime import datetime, tzinfo, timedelta
from time import strptime
from optparse import OptionParser
import time

NAME = "mergeCOLtables"
CONFIGFILE = "insecta.cnf"
# logger = logging.getLogger('SDFEPublish-Logger')
testing = False
verbose = False

def error(s):
    """output an error and take an early bath."""
    print >> sys.stderr, "%s: %s" % (NAME, s)
    if (logger):
      logger.error('%s: %s' % (NAME, s))
    sys.exit(1)

def getconfigstring(config, section, key):
    """helper function to let us include error trapping"""
    errorMessage = "Error: "
    keyvalue = ""
    try:
      keyvalue = config.get(section, key)
    except ConfigParser.NoSectionError:
      error("Error: Unable to find section %s." % (section))
    except ConfigParser.NoOptionError:
      error("Error: Unable to find option %s." % (key))

    return keyvalue

def getconfigint(config, section, key):
    """helper function to let us include error trapping"""
    errorMessage = "Error: "
    try:
      keyvalue = config.getint(section, key)
    except ConfigParser.NoSectionError:
      error("Error: Unable to find section %s." % (section))
    except ConfigParser.NoOptionError:
      error("Error: Unable to find option %s." % (key))

    return keyvalue

def getboolean(config, section, key):
    try:
        keyvalue = config.getboolean(section, key)
    except ConfigParser.NoSectionError:
      error("Error: Unable to find section %s." % (section))
    except ConfigParser.NoOptionError:
      error("Error: Unable to find option %s." % (key))

    return keyvalue

def putconfigstring(config, section, key):
    """helper function to write a section key"""
    try:
        config.set(section, key)
    except ConfigParser.NoSectionError:
      error("Error: Unable to find section %s." % (section))

def updateConfig(config, ProcUpToDateTime):
    config.set('Log', 'PrevProcDateTime', ProcUpToDateTime.isoformat())
    configfile = open(CONFIGFILE, 'wb')
    config.write(configfile)

def incSecond(currentDate):
    """ increment the currentDate by 1 second and return """
    delta =  timedelta(seconds=1)
    return currentDate + delta

def readKey(keyValue, bucket):
    """ Read a key from the bucket
    """
    if bucket.get_key(keyValue) == None:
      logger.error('PII %s not found' % (keyValue,))
      return ""

    sampleKey = Key(bucket)
    sampleKey.key = keyValue
    return sampleKey.get_contents_as_string()

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

def createMediaRow(pii,extensionList, eidList, dbCursor):
    rows = len(extensionList)
    for row in range(0, rows):
      extension = extensionList[row][:9]
      dbCursor.execute("SELECT eid from media WHERE eid = %s;", (eidList[row],))
      if dbCursor.rowcount == 0:
        dbCursor.execute("INSERT INTO media (pii, extension, eid) VALUES (%s, %s, %s)",
                        (pii, extension,eidList[row]))

def createCIRow(pii, fieldname, ciList, dbCursor):
    rows = len(ciList)
    for row in range(0, rows):
      dbCursor.execute("SELECT pii from ci WHERE pii = %s AND ci_name = %s;", (pii,fieldname))
      if dbCursor.rowcount == 0:
        dbCursor.execute("INSERT INTO ci (pii, ci_name, ci_present) VALUES (%s, %s, %s)",
                        (pii, fieldname, True))

def handleDeleted(pii, status, content, dbCursor, readQueue, updateMessage):
  """ Deleted records may have empty content as it has been deleted from the bucket already
      this function side steps the problem and either updates an existing row in the database
      or inserts a new one.
  """
  dbCursor.execute("SELECT pii from article_categories WHERE pii = %s;", (pii,))
  logger.info('%s %s' % (status, pii,))
  if testing == False:
    if dbCursor.rowcount == 0:
      dbCursor.execute("INSERT INTO article_categories (pii, status) VALUES (%s, %s)", (pii, status))
    else:
      dbCursor.execute("UPDATE article_categories set status = "+"'"+status+"' WHERE pii = "+ "'"+pii+"'")
    readQueue.delete_message(updateMessage)



class TZ(tzinfo):
    """ Fixup the datetime for isoformat to show the TZ correctly
        ie: 2010-02-09T18:49:14+00:00
        instead of 2010-02-09T18:49:35.271000
        Stolen openly from http://blog.hokkertjies.nl/2008/03/24/isoformat-in-python-24/ """
    def utcoffset(self, dt): return timedelta(seconds=time.timezone)
    def dst(self, dt): return timedelta(0)


def main():
    """
    Process all of the available imported tables that match the pattern in .
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
    config = ConfigParser.SafeConfigParser()
    config.read(CONFIGFILE)

    verbose = getboolean(config, 'Configure', 'verbose')
    testing = getboolean(config, 'Configure', 'testing')

    if verbose == True:
      logger.setLevel(logging.INFO)
    elif testing == True:
      logger.setLevel(logging.DEBUG)
    else:
      logger.setLevel(logging.INFO)

    loghandler = logging.handlers.SysLogHandler(address = '/dev/log')
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
    dbConnectionString = "host="+quote(db)+" dbname="+quote(dbname)+" user="+quote(dbuser)+" password="+quote(dbpassword)
    dbConn = psycopg2.connect(dbConnectionString)
    dbCursor = dbConn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    dbCursor.execute("select table_schema, table_name from information_schema.tables WHERE table_name LIKE '%col_%';")
    importTables = dbCursor.fetchall()
    for (importTable in importTables):
      if ("taxon" in importTable['table_name'][1]):
        queryString = """Insert into "insecta_taxon" SELECT uuid_generate_v1mc() as "insectaID", "taxonID", identifier, "datasetID", "datasetName", "acceptedNameUsageID", "parentNameUsageID", "taxonomicStatus",
                                  "taxonRank", "verbatimTaxonRank", "scientificName", kingdom, phylum, class, "order", superfamily, family, "genericName", genus, subgenus, "specificEpithet",
                  "infraspecificEpithet", "scientificNameAuthorship", source, "namePublishedIn", "nameAccordingTo", modified, description, "taxonConceptID", "scientificNameID", 
                  "references", "isExtinct","""
      if ("distribution" in importTable['table_name'][1]):
        queryString = """"Insert into "insecta_distribution" SELECT taxonID", "locationID", locality, "occurrenceStatus", "establishmentMeans", """
      if ("description" in importTable['table_name'][1]):
        queryString = """Insert into "insecta_description" SELECT "taxonID", locality, """
      if ("reference" in importTable['table_name'][1]):      
        queryString = """Insert into "insecta_reference" SELECT "taxonID", locality, """
      if ("vernacular" in importTable['table_name'][1]):)
         queryString = """Insert into "insecta_vernacular" SELECT "taxonID", "vernacularName", language, "countryCode", locality, transliteration, """
         
      queryString .= '"'+importTable+'"' + 'as "sourceTable", current_date as "dateCreate" FROM '+ '"'+importTable+'";'
      dbCursor.execute(queryString)       

    dbConn.close()
    PrevProcDateTime = getconfigstring(config, 'Log', 'PrevProcDateTime')
    updateConfig(config, ProcUpToDateTime)

if __name__ == "__main__":
    main()
