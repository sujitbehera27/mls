"""
Downloads MLS listings and sticks them into S3, then sends a message so that parse.py will try to parse the data
and add it to the database
"""
import boto
import sys
import logging
from logging.handlers import SysLogHandler
import urllib
import time
import simplejson
import aws

log = logging.getLogger("fetcher")

sqs = boto.connect_sqs()
mls_queue = sqs.get_queue("mls_fetcher")
parse_queue = sqs.get_queue("mls_parse_requests")
s3 = boto.connect_s3()
bucket = s3.get_bucket('mls_data.mls.angerilli.ca')

def get_http_data(url):
    f = urllib.urlopen(url)
    log.debug("Fetching %s", url)
    return f.read()
    
def get_key(mls):
    timestamp = aws.get_iso_timestamp()
    return "%s/%s.html" % (mls, timestamp)
    
def main(argv):
    # Loop indefinitely, waiting for messages
    # If a message is available, fetch the data, stick it into s3, delete the message, then continue waiting
    log.info("Starting fetcher")
    sleeper = aws.Sleeper(5)
    while True:
        m = mls_queue.read(visibility_timeout=10)
        if m is not None:
            sleeper.reset()
            mls_number = m.get_body()
            log.info("Got request for %s from queue" % mls_number)
            
            # TODO: Check to see if this worked
            data = get_http_data("http://www.realtylink.org/prop_search/Detail.cfm?MLS=%s" % mls_number)
            key = bucket.new_key(get_key(mls_number))
            key.set_contents_from_string(data)
            
            mls, datestr = key.key.split("/")
            date = datestr.split(".")[0]
            
            # Queue up a message requesting that the data which was just fetched be parsed
            msg_body = simplejson.dumps({"mls":mls_number, "key":key.name, "bucket":bucket.name, "date":date})
            request_msg = parse_queue.new_message(msg_body)
            parse_queue.write(request_msg)
            
            mls_queue.delete_message(m)
        else:
            sleeper.sleep()

if __name__=="__main__":
    logging.basicConfig()
    if sys.platform == "darwin":
            # Apple made 10.5 more secure by disabling network syslog:
            address = "/var/run/syslog"
    else:
            address = ('localhost', 514)
    syslog = SysLogHandler(address)
    for handler in logging.getLogger().handlers:
        handler.setFormatter(logging.Formatter("%(asctime)s %(name)-19s %(levelname)-7s - %(message)s"))
    formatter = logging.Formatter('%(name)s: %(levelname)s %(message)s')
    syslog.setFormatter(formatter)
    syslog.setLevel(logging.INFO)
    logging.getLogger().addHandler(syslog)    
    logging.getLogger().setLevel(logging.INFO)
    
    sys.exit(main(sys.argv))