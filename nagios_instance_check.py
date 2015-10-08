#!/usr/bin/python
####################
# Annalisa Russo   #
# Coresystems      #
# 2013             #
####################

# Standard Nagios return codes
OK = 0
WARNING = 1
CRITICAL = 2
UNKNOWN = 3

import datetime
import os
import re
import sys
import signal
import time
import commands

minutes = 5
now = datetime.datetime.now()

##########################################################
#check that the file exists
##########################################################


path = '/opt/applications/backupy/US-2Operation/backup_ec2-with-AMI-US-NORTH-VIRGINIA-US_Operation.py'
if not os.path.exists(path):

        print "CRITICAL: not found  (%s)" % (path)
        sys.exit(2)
else:
        path1 = '/opt/applications/backupy/US-2Operation/backup.log'
        if not os.path.exists(path1):
               print "CRITICAL: not found  (%s)" % (path1)
               sys.exit(2)
        else:
               inf1 = open(path1, "r").readlines()


##########################################################
#Checks if the log file is empty
##########################################################


        if os.path.getsize(path1) == 0:
             print 'ERROR--Backup Log File is empty. Please check'
             sys.exit(2)

##########################################################
#checks if the backup script has been killed
##########################################################
        lines0 = 0
        lines1 = 0
        success = u'SUCCESS--BACKUP--END'
        for line0 in inf1:
            lines0+=1
        last_line0 = line0

        time.sleep(30)

        inf2 = open(path1, "r").readlines()
        for line1 in inf2:
            lines1+=1
        last_line1 = line1

        if (last_line0 == last_line1) & (last_line1.find(success) <= 0):
            print 'CRITICAL - Backup Stopped'
            sys.exit(2)



##########################################################
#Checks if there are any errors or warning in the log file
##########################################################
b=(time.strftime('%A %d %b'))
END_BACKUP_DESCRIPTION = "{}".format(b)
check_comparison = ("---SUCCESS--BACKUP--END---: ["+ END_BACKUP_DESCRIPTION +"]")
#print check_comparison

for line in inf1:

         error = u'kill'
         if line.find(error) >= 0:
                 print 'CRITICAL - ' + line
                 sys.exit(2)

         error = u'Kill'
         if line.find(error) >= 0:
                 print 'CRITICAL - ' + line
                 sys.exit(2)

         error = u'<Response><Errors><Error>'
         if line.find(error) >= 0:
                 print 'CRITICAL - ' + line
                 sys.exit(2)

         error = u'CRASH'
         if line.find(error) >= 0:
                 print 'CRITICAL - ' + line
                 sys.exit(2)

         warning = u'DANGER'
         if line.find(warning) >= 0:
                 print 'WARNING - ' + line
                 sys.exit(1)


         ok = u'SUCCESS--BACKUP--END'
         if line.find(ok) >= 0:
                 print 'OK - ' + line
                 sys.exit(0)

print  'OK - Backup still in progress...' + last_line1
sys.exit(0)
##########################################################
#END
##########################################################
