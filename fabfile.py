from __future__ import with_statement
from fabric.api import *
from fabric.contrib.console import confirm

import boto
import boto.ec2
import sys
import time


# Ubuntu 10.04 ebs store
AMI = 'ami-480df921'

def create_ec2_instance():
    regions = boto.ec2.regions()
    if not confirm("Use the %s region?" % regions[1]):
        print "Regions are %s" %  regions
        abort("Aborting")
    region = regions[1]
    
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
    region = regions[1]
    
    c = region.connect()
    for res in c.get_all_instances():
        for instance in res.instances:
            print instance.dns_name
            
def setup_queues():
    """
    Create SQS queues for MLS app
    """
    sqs = boto.connect_sqs()
    sqs.create_queue('mls_parse_requests')
    sqs.create_queue('mls_fetcher')
    
def setup_buckets():
    """
    Create S3 buckets for MLS app
    """
    s3 = boto.connect_s3()
    s3.create_bucket('mls_data.mls.angerilli.ca')
    
def setup_domains():
    """
    Create SDB domains for MLS app
    """
    sdb = boto.connect_sdb()
    sdb.create_domain("mls_domain")
    
def setup_aws():
    """
    Sets up various amazon web services for the MLS app
    """
    setup_queues()
    setup_buckets()
    setup_domains()
    
def live():
    env.hosts = ['ec2-184-72-160-76.compute-1.amazonaws.com']
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
    
def shutdown():
    sudo("shutdown -h now")
    