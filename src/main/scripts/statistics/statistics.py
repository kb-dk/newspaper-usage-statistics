#!/usr/bin/env python2

# https://sbprojects.statsbiblioteket.dk/jira/browse/MSAVIS-4 - post process statistics log.
#
# For development purposes invoke as
#
#     python2 statistics.py fromDate=2013-07-01 toDate=2015-12-31
#
# The extra arguments trigger non-CGI output, and provides parameters to the script.
#

from __future__ import print_function # for stderr

from io import BytesIO
from lxml import etree as ET
import ConfigParser
import cgi
import cgitb
import csv
import datetime
import glob
import os
import re
import simplejson
import suds
import sys
import time

# 

config_file_name = "../../newspaper_statistics.py.cfg" # outside web root.

encoding = "utf-8" # What to use for output

# ---

commandLine = len(sys.argv) > 1 # script name is always #0

if commandLine:
    # parse command line arguments on form "fromDate=2015-03-03" as map
    parameters = {}
    for arg in sys.argv[1:]:
        keyvalue = arg.partition("=")
        if (keyvalue[2])>0:
            parameters[keyvalue[0]] = keyvalue[2]
else:
    # we are a cgi script
    cgitb.enable()
    parameters = cgi.FieldStorage()


# -- load configuration file.  If not found, provide absolute path looked at.

absolute_config_file_name = os.path.abspath(config_file_name)
if not os.path.exists(absolute_config_file_name):
    # http://stackoverflow.com/a/14981125/53897
    print("Configuration file not found: ", absolute_config_file_name, file=sys.stderr)
    exit(1)

config = ConfigParser.SafeConfigParser()
config.read(config_file_name)


# -- create web service client from WSDL url. see https://fedorahosted.org/suds/wiki/Documentation

mediestream_wsdl = config.get("cgi", "mediestream_wsdl")
client = suds.client.Client(mediestream_wsdl)

# -- extract and setup

if "type" in parameters:
    requiredType = parameters["type"]
else:
    requiredType = ""

if "fromDate" in parameters:
    start_str = parameters["fromDate"] # "2013-06-15"
else:
    start_str = "2013-06-01"

if "toDate" in parameters:
    end_str = parameters["toDate"]
else:
    end_str = "2015-07-01"

# Example: d68a0380-012a-4cd8-8e5b-37adf6c2d47f (optionally trailed by a ".fileending")
re_doms_id_from_url = re.compile("([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(\.[a-zA-Z0-9]*)?$")

statistics_file_pattern = config.get("cgi", "statistics_file_name_pattern")

# http://stackoverflow.com/a/2997846/53897 - 10:00 is to avoid timezone issues in general.
start_date = datetime.date.fromtimestamp(time.mktime(time.strptime(start_str + " 10:00", '%Y-%m-%d %H:%M')))
end_date = datetime.date.fromtimestamp(time.mktime(time.strptime(end_str + " 10:00", '%Y-%m-%d %H:%M')))

namespaces = {
    "rdf":"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dc":"http://purl.org/dc/elements/1.1/"
}

# Titles for columns in CSV:
fieldnames = ["Timestamp", "Type", "AvisID", "Avis", "Adgangstype", "Udgivelsestidspunkt", "Udgivelsesnummer",
              "Sidenummer", "Sektion", "Klient", "schacHomeOrganization", "eduPersonPrimaryAffiliation",
              "eduPersonScopedAffiliation", "eduPersonPrincipalName", "eduPersonTargetedID",
              "SBIPRoleMapper", "MediestreamFullAccess", "UUID"]

if not commandLine:
    print("Content-type: text/csv")
    print("Content-disposition: attachment; filename=stat-" + start_str + "-" + end_str + ".csv")
    print("")

result_file = sys.stdout

result_dict_writer = csv.DictWriter(result_file, fieldnames, delimiter="\t" )
# Writes out a row where each column name has been put in the corresponding column.  If
# Danish characters show up in a header, these must be encoded too.
header = dict(zip(result_dict_writer.fieldnames, result_dict_writer.fieldnames))
result_dict_writer.writerow(header)

summa_resource_cache = {}
summa_resource_cache_max = 10000 # number of items to cache, when reached cache is flushed.


previously_seen_uniqueID = set() # only process ticket/domsID combos once

for statistics_file_name in glob.iglob(statistics_file_pattern):

    if os.path.isfile(statistics_file_name) == False:
        continue

    statistics_file = open(statistics_file_name, "rb")

    for line in statistics_file:

        # Mon Jun 22 15:28:02 2015: {"resource_id":"...","remote_ip":"...","userAttributes":{...},"dateTime":1434979682,"ticket_id":"...","resource_type":"Download"}

        lineParts = line.partition(": ")

        json = lineParts[2]

        try:
            entry = simplejson.loads(json)
        except simplejson.scanner.JSONDecodeError as e:
            print("Bad JSON skipped: ", json, file=sys.stderr)
            continue

            # -- line to be considered?

        entryDate = datetime.date.fromtimestamp(entry["dateTime"])

        if not start_date <= entryDate <= end_date:
            continue

        if requiredType != "" and not requiredType == entry["resource_type"]:
            continue

        resource_id = entry["resource_id"]

        downloadPDF = entry["resource_type"] == "Download"

        # -- only process each ticket/domsID once (deep zoom makes _many_ requests).

        uniqueID = resource_id + " " + entry["ticket_id"] + " " + str(downloadPDF)

        if uniqueID in previously_seen_uniqueID:
            continue
        else:
            previously_seen_uniqueID.add(uniqueID)

        # -- ask summa for additional information (with a cache)

        summa_resource_cache_key = resource_id + " " + str(downloadPDF)

        if summa_resource_cache_key in summa_resource_cache:
            summa_resource = summa_resource_cache[summa_resource_cache_key]
        else:
            if downloadPDF:
                query = {}
                query["search.document.query"] = "editionUUID:\"doms_aviser_edition:uuid:" +resource_id + "\""
                query["search.document.maxrecords"] = "20"
                query["search.document.startindex"] = "0"
                query["search.document.resultfields"] = "pageUUID, shortformat, familyId"
                query["solrparam.facet"] = "false"
                query["group"] = "true"
                query["group.field"] = "editionUUID"
                query["search.document.collectdocids"] = "false"
            else:
                query = {}
                query["search.document.query"] = "pageUUID:\"doms_aviser_page:uuid:" +resource_id + "\""
                query["search.document.maxrecords"] = "20"
                query["search.document.startindex"] = "0"
                query["search.document.resultfields"] = "pageUUID, shortformat, familyId"
                query["solrparam.facet"] = "false"
                query["group"] = "true"
                query["group.field"] = "pageUUID"
                query["search.document.collectdocids"] = "false"

            queryJSON = simplejson.dumps(query)
            summa_resource_text = client.service.directJSON(queryJSON)
            # print(summa_resource_text.encode(encoding))

            summa_resource = ET.parse(BytesIO(bytes(bytearray(summa_resource_text, encoding='utf-8'))))
            summa_resource_cache[summa_resource_cache_key] = summa_resource

            # for very large log files the cache needs to be emptied once in a while.
            if len(summa_resource_cache) > summa_resource_cache_max:
                summa_resource_cache = {}

        # --

        shortFormat = (summa_resource.xpath("/responsecollection/response/documentresult/group/record[1]/field[@name='shortformat']/shortrecord"))[0]

        # -- ready to generate output

        outputLine = {}

        outputLine["Type"] = "info:fedora/doms:Newspaper_Collection"

        outputLine["Adgangstype"] = entry["resource_type"]

        outputLine["UUID"] = resource_id

        outputLine["Timestamp"] = datetime.datetime.fromtimestamp(entry["dateTime"]).strftime("%Y-%m-%d %H:%M:%S")

        outputLine["Klient"] = entry["remote_ip"]

        # print(ET.tostring(shortFormat))
        outputLine["AvisID"] = (summa_resource.xpath("/responsecollection/response/documentresult/group/record[1]/field[@name='familyId']/text()") or [""])[0]
        outputLine["Avis"] = (shortFormat.xpath("rdf:RDF/rdf:Description/newspaperTitle/text()",namespaces=namespaces) or [""])[0]
        outputLine["Udgivelsestidspunkt"] = (shortFormat.xpath("rdf:RDF/rdf:Description/dateTime/text()",namespaces=namespaces) or [""])[0]
        outputLine["Udgivelsesnummer"] = (shortFormat.xpath("rdf:RDF/rdf:Description/newspaperEdition/text()",namespaces=namespaces) or [""])[0]

        outputLine["schacHomeOrganization"] = ", ".join(e for e in entry["userAttributes"].get("schacHomeOrganization",{}))
        outputLine["eduPersonPrimaryAffiliation"] = ", ".join(e for e in entry["userAttributes"].get("eduPersonPrimaryAffiliation",{}))
        outputLine["eduPersonScopedAffiliation"] = ", ".join(e for e in entry["userAttributes"].get("eduPersonScopedAffiliation",{}))
        outputLine["eduPersonPrincipalName"] = ", ".join(e for e in entry["userAttributes"].get("eduPersonPrincipalName",{}))
        outputLine["eduPersonTargetedID"] = ", ".join(e for e in entry["userAttributes"].get("eduPersonTargetedID",{}))
        outputLine["SBIPRoleMapper"] = ", ".join(e for e in entry["userAttributes"].get("SBIPRoleMapper",{}))
        outputLine["MediestreamFullAccess"] = ", ".join(e for e in entry["userAttributes"].get("MediestreamFullAccess",{}))

        if not downloadPDF:
            # Does not make sense on editions
            outputLine["Sektion"] = (shortFormat.xpath("rdf:RDF/rdf:Description/newspaperSection/text()",namespaces=namespaces) or [""])[0]
            outputLine["Sidenummer"] = (shortFormat.xpath("rdf:RDF/rdf:Description/newspaperPage/text()",namespaces=namespaces) or [""])[0]

        encodedOutputLine = dict((key, outputLine[key].encode(encoding)) for key in outputLine.keys())
        result_dict_writer.writerow(encodedOutputLine)

    statistics_file.close()
