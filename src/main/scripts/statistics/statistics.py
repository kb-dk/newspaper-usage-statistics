#!/usr/bin/env python2.4

# Jira issue NO-154.  Enrich mediastream player log with DOMS meta data.

from __future__ import print_function

from lxml import etree as ET
import ConfigParser
import csv
import datetime
import simplejson
import os
import re
import sys
import time
import cgi
import cgitb
import urllib2
import urllib
from io import StringIO, BytesIO
import glob
import string
import suds

# 

config_file_name = "../../statistics.py.cfg"

# -----

#cgitb.enable() # TODO ENABLE THIS WHEN CLI TESTS DONE. web page feedback in case of problems
parameters = cgi.FieldStorage()

encoding = "utf-8" # What to convert non-ASCII chars to.

absolute_config_file_name = os.path.abspath(config_file_name)
if not os.path.exists(absolute_config_file_name):
    # http://stackoverflow.com/a/14981125/53897
    print("Configuration file not found: ", absolute_config_file_name, file=sys.stderr)
    exit(1)


config = ConfigParser.SafeConfigParser()
config.read(config_file_name)

# --

mediestream_wsdl = config.get("cgi", "mediestream_wsdl") # .../fedora/

# https://fedorahosted.org/suds/wiki/Documentation
client = suds.client.Client(mediestream_wsdl)
print(client)

# --

# Example: d68a0380-012a-4cd8-8e5b-37adf6c2d47f (optionally trailed by a ".fileending")
re_doms_id_from_url = re.compile("([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(\.[a-zA-Z0-9]*)?$")

# The type of resource to get

for i in parameters.keys():
    print(parameters[i].value)

if "type" in parameters:
    requiredType = parameters["type"]
else:
    requiredType = ""

statistics_file_pattern = config.get("cgi", "statistics_file_name_pattern")

if "fromDate" in parameters:
    start_str = parameters["fromDate"].value # "2013-06-15"
else:
    start_str = "2013-06-01"

if "toDate" in parameters:
    end_str = parameters["toDate"].value
else:
    end_str = "2015-07-01"

# http://stackoverflow.com/a/2997846/53897 - 10:00 is to avoid timezone issues in general.
start_date = datetime.datetime.fromtimestamp(time.mktime(time.strptime(start_str + " 10:00", '%Y-%m-%d %H:%M')))
end_date = datetime.datetime.fromtimestamp(time.mktime(time.strptime(end_str + " 10:00", '%Y-%m-%d %H:%M')))

# generate dates. note:  range(0,1) -> [0] hence the +1
dates = [start_date + datetime.timedelta(days = x) for x in range(0,(end_date - start_date).days + 1)]

handler = urllib2.HTTPHandler()
opener = urllib2.build_opener(handler)

namespaces = {
    "rdf":"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dc":"http://purl.org/dc/elements/1.1/"
}

# Prepare output CSV:
fieldnames = ["Timestamp", "Type", "Avis", "Adgangstype", "Udgivelsestidspunkt", "Udgivelsesnummer",
              "Sidenummer", "Sektion", "Klient", "schacHomeOrganization", "eduPersonPrimaryAffiliation",
              "eduPersonScopedAffiliation", "eduPersonPrincipalName", "eduPersonTargetedID",
              "SBIPRoleMapper", "MediestreamFullAccess", "UUID"]

print("Content-type: text/csv")
print("Content-disposition: attachment; filename=stat-" + start_str + "-" + end_str + ".csv")
print("")


result_file = sys.stdout

result_dict_writer = csv.DictWriter(result_file, fieldnames, delimiter="\t")
# Inlined result_dict_writer.writeheader() - not present in 2.4.
# Writes out a row where each column name has been put in the corresponding column 
header = dict(zip(result_dict_writer.fieldnames, result_dict_writer.fieldnames))
result_dict_writer.writerow(header)

doms_ids_seen = {} # DOMS lookup cache, id is key
uniqueIDs = {} # ticket/domsID combos we have already handled.

for statistics_file_name in glob.iglob(statistics_file_pattern):

    # FIXME: Silently skip older logfiles.
    if os.path.isfile(statistics_file_name) == False:
        continue

    statistics_file = open(statistics_file_name, "rb")
    splitString = ": "

    for line in statistics_file:

        # Thu Jun 18 15:46:09 2015: {"resource_id":"cca6e5a6-a635-49f0-8f26-0e05ac9dd8c2",...
        splitPosition = string.find(line, splitString)

        if (splitPosition == -1): # ignore bad log lines
            continue

        json = line[splitPosition + len(splitString):]

        try:
            entry = simplejson.loads(json)
        except simplejson.scanner.JSONDecodeError as e:
            print("Bad JSON skipped: ", json, file=sys.stderr)
            continue # next line

        # -- ready to generate output

        outputLine = {}

        #They are all from this collection
        outputLine["Type"] = "info:fedora/doms:Newspaper_Collection"

        #If not correct type, ignore
        if requiredType != "" and not requiredType == entry["resource_type"]:
            continue

        outputLine["Adgangstype"] = entry["resource_type"]

        doms_id = entry["resource_id"]
        outputLine["UUID"] = doms_id

        #If this ticket/domsId have been seen before ignore.
        uniqueID = doms_id + entry["ticket_id"]
        if uniqueID in uniqueIDs:
            continue
        else:
            uniqueIDs[uniqueID] = uniqueID # only key matters.

        log_entry_date_time = entry["dateTime"]
        outputLine["Timestamp"] =  datetime.datetime.fromtimestamp(log_entry_date_time).strftime("%Y-%m-%dT%H:%M:%S")

        outputLine["Klient"] = entry["remote_ip"]

        # currently only caching shortFormat field, not complete response (including familiyId).
        if doms_id in doms_ids_seen:
            shortFormat = doms_ids_seen[doms_id]
        else:
            # {search.document.query:"pageUUID:
            # \"doms_aviser_page:uuid:c5ea9975-dbc6-49ca-a68c-5c27fefae407\" OR
            # pageUUID:\"doms_aviser_page:uuid:f2816832-7bd4-4353-a763-ad9eff91cf09
            # \"",
            # search.document.maxrecords:"20", search.document.startindex:"0",
            # search.document.resultfields:"pageUUID, shortformat",
            # solrparam.facet:"false",
            # group:"true",
            # group.field:"pageUUID",
            # search.document.collectdocids:"false"}

            query = {}
            query["search.document.query"] = "pageUUID:\"doms_aviser_page:uuid:" +doms_id + "\""
            query["search.document.maxrecords"] = "20"
            query["search.document.startindex"] = "0"
            query["search.document.resultfields"] = "pageUUID, shortformat, familyId"
            query["solrparam.facet"] = "false"
            query["group"] = "true"
            query["group.field"] = "pageUUID"
            query["search.document.collectdocids"] = "false"

            queryJSON = simplejson.dumps(query)
            core_body_text = client.service.directJSON(queryJSON)
            # print(core_body_text.encode(encoding))

            core = ET.parse(BytesIO(bytes(bytearray(core_body_text, encoding='utf-8'))))
            shortFormat = (core.xpath("/responsecollection/response/documentresult/group/record[1]/field[@name='shortformat']/shortrecord"))[0]
            doms_ids_seen[doms_id] = shortFormat

        # TODO fix for pdf downloads also, where not all these fields might exist
        # print(ET.tostring(shortFormat))
        outputLine["Avis"] = (shortFormat.xpath("rdf:RDF/rdf:Description/newspaperTitle/text()",namespaces=namespaces) or [""])[0]
        outputLine["Udgivelsestidspunkt"] = (shortFormat.xpath("rdf:RDF/rdf:Description/dateTime/text()",namespaces=namespaces) or [""])[0]
        outputLine["Udgivelsesnummer"] = (shortFormat.xpath("rdf:RDF/rdf:Description/newspaperEdition/text()",namespaces=namespaces) or [""])[0]
        outputLine["Sektion"] = (shortFormat.xpath("rdf:RDF/rdf:Description/newspaperSection/text()",namespaces=namespaces) or [""])[0]
        outputLine["Sidenummer"] = (shortFormat.xpath("rdf:RDF/rdf:Description/newspaperPage/text()",namespaces=namespaces) or [""])[0]

        # credentials
        creds = entry["userAttributes"]

        for cred in ["schacHomeOrganization", "eduPersonPrimaryAffiliation",
                     "eduPersonScopedAffiliation", "eduPersonPrincipalName", "eduPersonTargetedID",
                     "SBIPRoleMapper", "MediestreamFullAccess"]:
            if creds and cred in creds:
                # creds[cred] is list, encode each entry, and join them as a single comma-separated string.
                outputLine[cred] = ", ".join(e.encode(encoding) for e in creds[cred])
            else:
                outputLine[cred] = ""

        encodedOutputLine = {}
        for key in outputLine.keys():
            encodedOutputLine[key] = outputLine[key].encode(encoding)

        result_dict_writer.writerow(encodedOutputLine)

    statistics_file.close()
