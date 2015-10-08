#us-east-1 = US east North Virginia
#us-west-1 = US west North California
#eu-west-1 = EU Ireland

#####################################
#servers present in North Virginia  #
#####################################
#node1.us1.dc.coresuite.com         #
#node1.us2.dc.coresuite.com         #
#us.operation.coresuite.com         #
#                                   #
#####################################




'''EC2 snapshot settings'''
import logging
from boto.ec2.connection import EC2Connection
from boto import ec2
from boto.exception import EC2ResponseError
import unicodedata
from datetime import date,timedelta
import iso8601
import optparse
from boto.ec2.blockdevicemapping import BlockDeviceType
from boto.ec2.blockdevicemapping import BlockDeviceMapping
from boto.ec2 import instance
import time
import os
import base64




logger = logging.getLogger('backup')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
fh = logging.handlers.RotatingFileHandler('backup.log',maxBytes=104857600)
fh.setLevel(logging.DEBUG)
frmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(frmt)
ch.setFormatter(frmt)
logger.addHandler(fh)
logger.addHandler(ch)


ec2_region = 'eu-west-1'
try:
    conn = ec2.connect_to_region(ec2_region)
except EC2ResponseError as error:
    logger.error(error)
    exit(2)


BACKUP_RETAIN = { 'daily': 8, 'weekly': 6, 'monthly': 6 }


EXCLUDED_INSTANCES = ['node2.mc.coresuite.com', 'node1.eu2.dc.coresuite.com', 'node1.eu1.dc.coresuite.com', 'b3aep.dc.coresuite.com', 'd3aepm.dc.coresuite.com', 'b1bepsc.coresuite.com', 'd1bepscm.coresuite.com', 'wt1.coresuite.com', 'w1aep.coresuite.com', 'd5cepm.dc.coresuite.com', 'b5cep.dc.coresuite.com', 'b4bep.dc.coresuite.com', 'd4bepm.dc.coresuite.com' ]









