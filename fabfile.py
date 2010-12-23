from __future__ import with_statement
from fabric.api import *
from fabric.contrib.console import confirm

import boto
import boto.ec2
import sys
import time


# Ubuntu 10.04 ebs store
AMI = 'ami-8c0c5cc9'

def create_ec2_instance():
    regions = boto.ec2.regions()
    if not confirm("Use the %s region?" % regions[2]):
        print "Regions are %s" %  regions
        abort("Aborting")
    region = regions[2]
    
    c = region.connect()
    print "Connection setup"
    image = c.get_image(AMI)
    print 'Launching EC2 instance ...'
    res = image.run(key_name='tony', security_groups=['default'], instance_type="t1.micro")
    print res.instances[0].update()
    instance = None
    while True:
        print "."
        sys.stdout.flush()
        dns = res.instances[0].dns_name
        if dns:
            instance = res.instances[0]
            break
        time.sleep(5)
        res.instances[0].update()
    print 'Instance started. Public DNS: ', instance.dns_name

def list_instances():
    regions = boto.ec2.regions()
    region = regions[2]
    
    c = region.connect()
    for res in c.get_all_instances():
        for instance in res.instances:
            print instance.dns_name
    

def live():
    env.hosts = ['ec2-50-18-7-83.us-west-1.compute.amazonaws.com']
    env.user = 'ubuntu'

def prepare_instance():
    """
    Prepares a linux system for the mls scripts
    """
    sudo("apt-get -y update")
    sudo("apt-get -y upgrade")
    sudo("apt-get install -y python-pip python-setuptools")
    sudo("pip install BeautifulSoup")
    sudo("pip install --upgrade boto")
    sudo("mv /usr/lib/pymodules/python2.6/boto /tmp")

def deploy_scripts():
    """
    Uploads the mls scripts
    """
    # Upload the boto config file
    put("scripts/.boto", ".boto")
    # Then upload the scripts
    local("tar -czf scripts.tar.gz scripts")
    put("scripts.tar.gz", ".")
    run("tar zxf scripts.tar.gz")
    run("rm scripts.tar.gz")
    local("rm scripts.tar.gz")
    
def start_scripts():
    """
    Start all of the data fetching and parsing scripts on the server
    """
    run("python scripts/fetcher.py >> fetcher.log 2>&1 &")
    run("python scripts/parse.py >> parser.log 2>&1 &")
    run("python scripts/search.py >> searcher.log 2>&1 &")
    
def stop_scripts():
    """
    Kill all python processes on the server
    """
    print "*** WARNING ***: This is about to kill all python processes"
    run("killall python")
    