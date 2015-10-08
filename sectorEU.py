#!/usr/bin/env python
##########################
# Annalisa Russo         #
# Coresystems ch - 2013  #
##########################
from boto.ec2.connection import EC2Connection
from boto.ec2.connection import *
from boto import ec2
from boto.exception import EC2ResponseError
import unicodedata
from datetime import datetime,date,timedelta
import time
import iso8601
import optparse
from optparse import OptionParser
from array import *

"""Creates snapshots of selected instances on daily,weekly,monthly basis. Also manages the purge of expired snapshots"""

from  sectorEUIRELANDops import *


path1 = '/opt/applications/backupy/NEW-EU-Operations/backup.log'
f = open(path1, 'w')



if __name__ == "__main__":
    backup_type = False

    parser = optparse.OptionParser()
    options = [
    parser.add_option('-d', '--daily', action="store_true",
        dest="daily", default=False, help="Daily backup"),
    parser.add_option('-w', '--weekly', action="store_true",
        dest="weekly", default=False, help="Critical backup"),
    parser.add_option('-m', '--monthly', action="store_true",
        dest="monthly", default=False, help="Monthly backup")
    ]

    (options, args) = parser.parse_args()
    flags = (options.daily, options.weekly, options.monthly)
    if sum(flags) != 1:
        logger.error("You must select a single backup type")
        exit(2)

    if options.daily:
        backup_type = 'daily'
    if options.weekly:
        backup_type = 'weekly'
    if options.monthly:
        backup_type = 'monthly'

def boto_connection(fn):
    """Decorator for handling EC2 Errors from the API"""
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except EC2ResponseError as error:
            logger.error(error)
    return wrapped

###START OF BACKUP CLASS
class Backup:

    DAILY = 'daily'
    WEEKLY = 'weekly'
    MONTHLY = 'monthly'

    def __init__(self, backup_type):
        self.instances_to_backup= []
        self.backup_type = backup_type

    def monthdelta(self,initial_date, delta):
        """Calculates the date from current date and delta in months"""
        m, y = (initial_date.month+delta) % 12, initial_date.year + (initial_date.month+delta-1) // 12
        if not m:
            m = 12
        feb_leap = 29 if y%4==0 and not y%400==0 else 28
        d = min(initial_date.day, [31,feb_leap,31,30,31,30,31,31,30,31,30,31][m-1])
        new_date = date(y,m,d)
        return new_date

    def oldest_date(self):
        """Returns the earliest date (as a string) to be purged"""
        if self.backup_type == self.DAILY:
            dt = date.today()-timedelta(days=BACKUP_RETAIN[self.DAILY])
            return dt
        if self.backup_type == self.WEEKLY:
            dt = date.today()-timedelta(days=(BACKUP_RETAIN[self.WEEKLY]*7))
            return dt
        if self.backup_type == self.MONTHLY:
            dt = self.monthdelta(date.today(),BACKUP_RETAIN[self.MONTHLY])
            return dt
        raise Exception("backup frequency is not correct!!")


    @boto_connection
    def instance_id_by_name(self,name_tag):
        """returns instance id, searching for the name tag"""
        name_filter = {'tag-key': 'Name','tag-value':name_tag}
        reservations = conn.get_all_instances(filters=name_filter)
        if not reservations:
            raise NameError("DANGER Unrecognized instance %s" % name_tag)
        instances = [i for r in reservations for i in r.instances]
        if len(reservations) > 1:
            raise Exception("DANGER Instance name tag is not unique!")
        return instances[0]

    @boto_connection
    def instances_for_backup(self):
        """Creates a list of only the instances configured for backup"""
        instance = None
        excluded_instances= []
        for excluded in EXCLUDED_INSTANCES:
            try:
                instance = self.instance_id_by_name(excluded)
            except NameError as error:
                logger.error(error)
                exit(2)
            excluded_instances.append(instance)

        reservations = conn.get_all_instances()
        all_instances = [i for r in reservations for i in r.instances]
 
        for exc in excluded_instances:
            for instance in all_instances:
                if instance.id == exc.id:
                    all_instances.remove(instance)
        return all_instances


    @boto_connection
    def volumes_for_instances(self,instance_list):
        """Return a list of all volumes attached to each instance in instance_list"""
        backup_volumes = []
        for instance in instance_list:
            instance_id = unicodedata.normalize('NFKD', instance.id).encode('ascii','ignore')
            filter = {'attachment.instance-id': instance_id}
            volumes = conn.get_all_volumes(filters=filter)
            backup_volumes = backup_volumes + volumes
        return backup_volumes


    @boto_connection
    def block_device_map_for_instance(self,instance_id):
        """Returns the BlockdeviceMap for the instance"""

        map = BlockDeviceMapping()

        filter = {'attachment.instance-id': instance_id}
        volumes = conn.get_all_volumes(filters=filter)

        for volume in volumes:
            volume_id = unicodedata.normalize('NFKD', volume.id).encode('ascii','ignore')
            self.purge_old_snapshots(instance_id,volume_id)

            volume_size = volume.size
            logger.info("Volume-ID [%s] - Volume-Size [%s GB]" % (volume_id,volume_size))

            if 'Name' in volume.tags:
                 snap = self.create_snapshots(volume_id, volume.tags['Name']) #creates the snapshot and returns the snapshotid
            else:
                 name_tag = ("No name instance: %s" % volume_id)
                 snap = self.create_snapshots(volume_id, name_tag) #creates the snapshot and returns the snapshotid

            deviceid = self.get_volume_device(volume_id)

            device = BlockDeviceType()
            device_name = deviceid
            device.snapshot_id = snap

            if deviceid == '/dev/sda1':
                  root_device_name = '/dev/sda1'
                  map[root_device_name] = device
            else:
                  device_name = deviceid
                  map[device_name] = device
        #print map
        return map


    @boto_connection
    def get_vol_snapshots(self,volume_id):
        """get snapshots older than that specified on the settings file"""
        snapshots_list = []
        filter = {'volume-id': volume_id}
        filter.update({'tag-key': 'backup','tag-value':self.backup_type})
        snapshots = conn.get_all_snapshots(filters=filter)
        for snapshot in snapshots:
            start_date = iso8601.parse_date(snapshot.start_time).date()
            if start_date < self.oldest_date():
                snapshots_list = snapshots_list + snapshots
        return snapshots_list


    @boto_connection
    def purge_old_snapshots(self,instance_id,volume_id):
        """purges snapshots older than that specified on the settings file"""
        filter = {'volume-id': volume_id}
        filter.update({'tag-key': 'backup','tag-value':self.backup_type})
        device = self.get_volume_device(volume_id)
        snapshots = conn.get_all_snapshots(filters=filter)
        for snapshot in snapshots:
            start_date = iso8601.parse_date(snapshot.start_time).date()
            if start_date < self.oldest_date():
                logger.info("Snapshot to delete is %s" % snapshot.id)
                conn.delete_snapshot(snapshot.id)

    @boto_connection
    def create_snapshots(self,volume_id,volume_name):
        """Creates the snapshots for each volume with the appropriate tags"""
        device = self.get_volume_device(volume_id)
        logger.info("Creating %s snapshot for %s %s %s" % (self.backup_type,volume_name,volume_id,device))

        #description for daily
        if options.daily:
           b=(time.strftime('%A %d %b'))
           c=volume_name
           backup_description = "{} {} {}".format('daily', b, c)

        #description for weekly
        if options.weekly:
           b=(datetime.now().strftime('%U'))
           c=volume_name
           backup_description = "{} {} {}".format('weekly', b, c)

        #description for monthly
        if options.monthly:
           b=(datetime.now().strftime('%B %Y'))
           c=volume_name
           backup_description = "{} {} {}".format('monthly', b, c)

        waitSnapshot = 10 # wait increment (in seconds) while waiting for snapshot to complete

        snapshot = conn.create_snapshot(volume_id,description=backup_description)
        newSnapshot = snapshot.id

        logger.info("Snapshot-ID [%s] started. Waiting for completion..." % (newSnapshot))

        waitSnapshotTotal = waitSnapshot
        snaps = conn.get_all_snapshots(str(newSnapshot))

        while snaps[0].status != 'completed':
                logger.info("Snapshot status is [%s], wait [%s] secs for the snapshot to complete..." % (snaps[0].status, waitSnapshotTotal))
                time.sleep(waitSnapshot)
                waitSnapshotTotal = waitSnapshotTotal + waitSnapshot

                snaps[0].update(validate=True)
                snaps = conn.get_all_snapshots(str(newSnapshot))

        if snaps[0].status == 'completed':
                logger.info("- Snapshot-ID [%s] Status is [%s] done." % (newSnapshot, snaps[0].status))
        else:
                logger.info("CRASH - Snapshot-ID [%s] Status is [%s] critical." % (newSnapshot, snaps[0].status))

        snapshot.add_tag("backup",backup_type)
        snapshot.add_tag("device",device)
        return newSnapshot

    @boto_connection
    def get_volume_device(self,volume_id):
        res = conn.get_all_instances()
        instances = [i for r in res for i in r.instances]
        vol = conn.get_all_volumes(volume_id)
        for volumes in vol:
                if volumes.attachment_state() == 'attached':
                        filter = {'block-device-mapping.volume-id':volumes.id}
                        volumesinstance = conn.get_all_instances(filters=filter)
                        ids = [z for k in volumesinstance for z in k.instances]
                        for s in ids:
                                attached_device = volumes.attach_data.device
        return attached_device


    @boto_connection
    def get_instance_kernel(self,instance_id):
        """Gets the Kernel-Id for the instance"""
        instkernel = conn.get_instance_attribute(instance_id, 'kernel')
        try:
                str_kernel = str(instkernel).split(':', 2) #convert object to string and split to parse the string elements
                a = str_kernel[1].split("'", 2)
                kernel = a[1]
                logger.info("Kernel-ID [%s]" % (kernel))
        except:
                kernel=None
                logger.warn("Kernel-ID could not be determined [%s]" % (kernel))
        finally:
                return kernel

    @boto_connection
    def create_ami(self,instance_id,instance_name):
        """Creates the AMI structure and registers the new AMI"""
        #instance_name = conn.get_instance_attribute(instance_id, 'name')

        root_device = '/dev/sda1'

        block_map = self.block_device_map_for_instance(instance_id) # all the action starts here
        #description for daily
        if options.daily:
           b=(time.strftime('%A %d %b'))
           c=instance_id
           AMI_description = "{} {} {}".format('daily', b, c)

        #description for weekly
        if options.weekly:
           b=(datetime.now().strftime('%U'))
           c=instance_id
           AMI_description = "{} {} {}".format('weekly', b, c)

        #description for monthly
        if options.monthly:
           b=(datetime.now().strftime('%B %Y'))
           c=instance_id
           AMI_description = "{} {} {}".format('monthly', b, c)

        logger.info("AMI-Name [%s] AMI-Description [%s]" % (AMI_description, AMI_description))

        instkernel = self.get_instance_kernel(instance_id)

        image_id = conn.register_image(name=AMI_description, description=AMI_description, root_device_name=root_device, block_device_map=block_map, architecture='x86_64', kernel_id=instkernel)
        logger.info("AMI Registered Successfully with AMI-ID [%s]" % (image_id))

        #we sleep a little to be sure that the next query for the ami-id will return successfully - we got some errors that AMI-ID is not found, even it was successfully created...
        time.sleep(5)

        images = conn.get_all_images(image_ids=[image_id]) # get again the image id as object, because the first is string and is not valid for add_tag...
        for image in images:
                if instance_name != '':
                    image.add_tag('Name', instance_name)
                else:
                    image.add_tag('Name', instance_id)
        return image_id


    @boto_connection
    def start(self):
        """Starts the backup process"""
        logger.info("Starting backup run for %s backups", self.backup_type)
        instance_list = self.instances_for_backup()

        for instance in instance_list:
            instance_id = unicodedata.normalize('NFKD', instance.id).encode('ascii','ignore')

            try:
                instance_name = instance.tags['Name']
            except:
                instance_name=None

            logger.info("Instance-ID [%s] - Instance Name [%s]"  % (instance_id, instance_name))

            self.create_ami(instance_id, instance_name) # we create the ami for each instance


###END OF BACKUP CLASS

toNagios=(time.strftime('%A %d %b'))
END_BACKUP_DESCRIPTION = "{}".format(toNagios)





backup = Backup(backup_type)
backup.start()




logger.info("---SUCCESS--BACKUP--END---: ["+ END_BACKUP_DESCRIPTION +"] ")

















