import boto
import sys
import logging
import urllib
import time
import simplejson
import aws

sqs = boto.connect_sqs()
parse_queue = sqs.get_queue("mls_parse_requests")
s3 = boto.connect_s3()
bucket = s3.get_bucket('mls_data.mls.angerilli.ca')
sdb = boto.connect_sdb()
mls_domain = sdb.get_domain("mls")

def get_key(mls):
    timestamp = aws.get_iso_timestamp()
    return "%s/%s.html" % (mls, timestamp)
    
def main(argv):
    for key in bucket.list():
        mls = key.key.split("/")[0]
        if not aws.mls_exists(mls_domain, mls):
            print "Need to parse %s" % mls
            msg_body = simplejson.dumps({"mls":mls, "key":key.name, "bucket":bucket.name})
            request_msg = parse_queue.new_message(msg_body)
            parse_queue.write(request_msg)

if __name__=="__main__":
    logging.getLogger().setLevel(logging.INFO) 
    sys.exit(main(sys.argv))