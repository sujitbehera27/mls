"""
Listens for requests to parse data, then fetches the data to parse out of
S3 and puts the parsed data into a database
"""
import boto
import sys
import logging
from logging.handlers import SysLogHandler
import urllib
import time
import simplejson

log = logging.getLogger("parser")

from lib import realtylink
import aws

sqs = boto.connect_sqs()
parse_queue = sqs.get_queue("mls_parse_requests")
s3 = boto.connect_s3()
bucket = s3.get_bucket('mls_data.mls.angerilli.ca')
sdb = boto.connect_sdb()
mls_domain = sdb.get_domain("mls")

def main(argv):
    log.info("Starting parser")
    sleeper = aws.Sleeper(5)
    # Loop indefinitely, waiting for messages
    # If a message is available, grab the data to parse out of S3
    while True:
        m = parse_queue.read(visibility_timeout=10)
        if m is not None:
            sleeper.reset()
            message_data = simplejson.loads(m.get_body())
                
            log.info("Processing %s with timestamp %s", message_data["mls"], message_data["date"])
            if aws.mls_exists(mls_domain, message_data["mls"], message_data["date"]):
                log.info("already exists")
                continue
            listing_key = bucket.get_key(message_data["key"])
            listing_html = listing_key.get_contents_as_string()
            # Parse it
            listing = realtylink.Listing(message_data["mls"], listing_html)
            
            # TODO: Make this more efficient by using the result from above
            listing_item = aws.mls_exists(mls_domain, message_data["mls"])
            if not listing_item:
                # And insert it into SimpleDB
                listing_item = mls_domain.new_item(hash(message_data["mls"]))
                listing_item["mls"] = listing.mls
                listing_item["description"] = listing.description[:1023]
                listing_item["area"] = listing.area
                listing_item["type"] = listing.type
                listing_item["bedrooms"] = listing.bedrooms
                listing_item["bathrooms"] = listing.bathrooms
                listing_item["age"] = listing.age
                listing_item["maintenance_fee"] = listing.maintenance_fee
                listing_item["features"] = listing.features
                listing_item["address"] = listing.address
                listing_item["region"] = listing.region
                listing_item["city"] = listing.city
                listing_item["unit"] = listing.unit
                listing_item["last_seen"] = aws.get_iso_timestamp()
                if "first_seen" not in listing_item:
                    listing_item["first_seen"] = aws.get_iso_timestamp()
            listing_item.add_value("prices", (listing.price, message_data["date"]))
            log.debug(listing_item)
            # Don't save it or delete the message while debugging
            listing_item.save()
            
            parse_queue.delete_message(m)
        else:
            log.info("Sleeping")
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