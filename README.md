Usage statistics for the newspaper project.
===

The Apache access checker in newspaper-fastcgi-ticket-checker logs
user data and DOMS information for each asset access check.

This module contains a small Python CGI program which reads the
generated logs, looks up information and generate a CSV with usage
information which can be post-processed in Excel.

For development purposes, copy statistics.py.cfg-example to
src/main/statistics.py.cfg (the ../.. path is to get outside
the code tree when deployed).  The sample file is set up to parse the
sample-logs/thumbnails.log file.

For IntelliJ see http://stackoverflow.com/a/24769264/53897

For Ubuntu 15.04 "sudo apt-get install python-simplejson python-suds" is needed.

/tra 2015-06-19

