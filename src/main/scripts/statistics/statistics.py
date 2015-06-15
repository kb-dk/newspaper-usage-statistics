#!/usr/bin/env python2.4

# Jira issue NO-154.  Enrich mediastream player log with DOMS meta data.

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
from io import StringIO, BytesIO

# 

config_file_name = "../../statistics.py.cfg"

# -----

#cgitb.enable() # TODO ENABLE THIS WHEN CLI TESTS DONE. web page feedback in case of problems
parameters = cgi.FieldStorage()

encoding = "utf-8" # What to convert non-ASCII chars to.

config = ConfigParser.SafeConfigParser()
config.read(config_file_name)

doms_url = config.get("cgi", "doms_url") # .../fedora/

# Example: d68a0380-012a-4cd8-8e5b-37adf6c2d47f (optionally trailed by a ".fileending")
re_doms_id_from_url = re.compile("([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(\.[a-zA-Z0-9]*)?$")

# The type of resource to get

for i in parameters.keys():
    print parameters[i].value

if "type" in parameters:
    requiredType = parameters["type"]
else:
    requiredType = ""

log_file_pattern = config.get("cgi", "log_file_pattern")

if "fromDate" in parameters:
    start_str = parameters["fromDate"].value # "2013-06-15"
else:
    start_str = "2013-06-01"

if "toDate" in parameters:
    end_str = parameters["toDate"].value
else:
    end_str = "2013-07-01"

# http://stackoverflow.com/a/2997846/53897 - 10:00 is to avoid timezone issues in general.
start_date = datetime.datetime.fromtimestamp(time.mktime(time.strptime(start_str + " 10:00", '%Y-%m-%d %H:%M')))
end_date = datetime.datetime.fromtimestamp(time.mktime(time.strptime(end_str + " 10:00", '%Y-%m-%d %H:%M')))

# generate dates. note:  range(0,1) -> [0] hence the +1
dates = [start_date + datetime.timedelta(days = x) for x in range(0,(end_date - start_date).days + 1)]

# prepare urllib2
username = config.get("cgi", "username")
password = config.get("cgi", "password")

# https://docs.python.org/2/howto/urllib2.html#id6
password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
top_level_url = doms_url
password_mgr.add_password(None, top_level_url, username, password)

handler = urllib2.HTTPBasicAuthHandler(password_mgr)
opener = urllib2.build_opener(handler)


namespaces = {
    "rdf":"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dc":"http://purl.org/dc/elements/1.1/"
}

# Prepare output CSV:
fieldnames = ["Timestamp", "Type", "AvisID", "Adgangstype", "Udgivelsestidspunkt", "Udgivelsesnummer",
              "Sidenummer", "Sektion", "Klient", "schacHomeOrganization", "eduPersonPrimaryAffiliation",
              "eduPersonScopedAffiliation", "eduPersonPrincipalName", "eduPersonTargetedID",
              "SBIPRoleMapper", "MediestreamFullAccess", "UUID"]

print "Content-type: text/csv"
print "Content-disposition: attachment; filename=stat-" + start_str + "-" + end_str + ".csv"
print


result_file = sys.stdout

result_dict_writer = csv.DictWriter(result_file, fieldnames, delimiter="\t")
# Inlined result_dict_writer.writeheader() - not present in 2.4.
# Writes out a row where each column name has been put in the corresponding column 
header = dict(zip(result_dict_writer.fieldnames, result_dict_writer.fieldnames))
result_dict_writer.writerow(header)

doms_ids_seen = {} # DOMS lookup cache, id is key
uniqueIDs = {} # ticket/domsID combos we have already handled.

for date in dates:

    log_file_name = log_file_pattern % date.strftime("%Y-%m-%d")

    # Silently skip non-existing logfiles.
    if os.path.isfile(log_file_name) == False:
        continue

    log_file = open(log_file_name, "rb")

    for line in log_file:
        if not 'AUTHLOG' in line: continue
        outputLine = {}
        #They are all from this collection
        outputLine["Type"] = "info:fedora/doms:Newspaper_Collection"

        #Parse the log entry
        (crap1, json) = line.split("AUTHLOG:")
        logEntry = simplejson.loads(json)

        #If not correct type, ignore
        if requiredType != "" and not requiredType == logEntry["resource_type"]:
            continue
        outputLine["Adgangstype"] = logEntry["resource_type"]

        doms_id = logEntry["resource_id"]
        outputLine["UUID"] = doms_id

        #If this ticket/domsId have been seen before ignore.
        uniqueID = doms_id + logEntry["ticket_id"]
        if uniqueID in uniqueIDs:
            continue
        else:
            uniqueIDs[uniqueID] = uniqueID # only key matters.

        outputLine["Timestamp"] = logEntry["dateTime"]

        outputLine["Klient"] = logEntry["remote_ip"]

        if doms_id in doms_ids_seen:
            shortFormat = doms_ids_seen[doms_id]
        else:
            url_core = doms_url + "?method=simpleSearch&query=pageUUID:\"doms_aviser_page:uuid:"+doms_id+"\"&numberOfRecords=1&startIndex=0"

            core_body = opener.open(url_core)
            core_body_text = core_body.read()
            core_body.close()
            core = ET.fromstring(core_body_text)
            soapNS = {"soapenv":"http://schemas.xmlsoap.org/soap/envelope/",
                          "ns1":"http://statsbiblioteket.dk/summa/search"}
            core_body_text = core.xpath("/soapenv:Envelope/soapenv:Body/simpleSearchResponse/ns1:simpleSearchReturn/text()",namespaces=soapNS)[0]
            core = ET.parse(BytesIO(bytes(bytearray(core_body_text, encoding='utf-8'))))
            shortFormat = (core.xpath("/responsecollection/response/documentresult/record[1]/field[@name='shortformat']/shortrecord"))[0]
            doms_ids_seen[doms_id] = shortFormat

        #TODO fix for pdf downloads also, where not all these fields might exist
        #print ET.tostring(shortFormat)
        outputLine["AvisID"] = shortFormat.xpath("rdf:RDF/rdf:Description/newspaperTitle/text()",namespaces=namespaces)[0]
        outputLine["Udgivelsestidspunkt"] = shortFormat.xpath("rdf:RDF/rdf:Description/dateTime/text()",namespaces=namespaces)[0]
        outputLine["Udgivelsesnummer"] = shortFormat.xpath("rdf:RDF/rdf:Description/newspaperEdition/text()",namespaces=namespaces)[0]
        outputLine["Sektion"] = shortFormat.xpath("rdf:RDF/rdf:Description/newspaperSection/text()",namespaces=namespaces)[0]
        outputLine["Sidenummer"] = shortFormat.xpath("rdf:RDF/rdf:Description/newspaperPage/text()",namespaces=namespaces)[0]

        # credentials
        creds = logEntry["userAttributes"]

        for cred in ["schacHomeOrganization", "eduPersonPrimaryAffiliation",
                     "eduPersonScopedAffiliation", "eduPersonPrincipalName", "eduPersonTargetedID",
                     "SBIPRoleMapper", "MediestreamFullAccess"]:
            if creds and cred in creds:
                # creds[cred] is list, encode each entry, and join them as a single comma-separated string.
                outputLine[cred] = ", ".join(e.encode(encoding) for e in creds[cred])
            else:
                outputLine[cred] = ""

        result_dict_writer.writerow(outputLine)

    log_file.close()
