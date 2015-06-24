Usage statistics for the newspaper project.
===

The Apache access checker in newspaper-fastcgi-ticket-checker logs
user data and DOMS information for each asset access check.

This module contains a small Python CGI program which reads the
generated logs, looks up information and generate a CSV with usage
information which can be post-processed in Excel.

For development purposes on local machine, copy
newspaper_statistics.py.cfg-example to
src/main/newspaper_statistics.py.cfg (the ../.. path is to get outside
the code tree when deployed).  The sample file is set up to parse the
sample-logs/thumbnails.log file.  When running standalone provide one
or more arguments on the form "fromDate=2015-06-24" to trigger
terminal mode and emulate html page parameters.

For archenar use, clone project and symlink
src/main/scripts/statistics to a CGI enabled location where "../.."
ends up outside the publicly visible pages and put a
newspaper_statistics.py.cfg file there.  The /var/log/httpd log files
are helpful in getting the configuration right.

For IntelliJ see http://stackoverflow.com/a/24769264/53897

For Ubuntu 15.04 "sudo apt-get install python-simplejson python-suds"
is needed.

For Centos "sudo yum install python-lxml python-simplejson
python-suds"

/tra 2015-06-24

