import boto
import sys
import logging
import urllib
import time
import simplejson

from lib import realtylink
import aws

sqs = boto.connect_sqs()
parse_queue = sqs.get_queue("mls_parse_requests")
s3 = boto.connect_s3()
bucket = s3.get_bucket('mls_data.mls.angerilli.ca')
sdb = boto.connect_sdb()
mls_domain = sdb.get_domain("mls")

def main(argv):
    # Loop indefinitely, waiting for messages
    # If a message is available, grab the data to parse out of S3
    while True:
        m = parse_queue.read(visibility_timeout=10)
        if m is not None:
            message_data = simplejson.loads(m.get_body())
            logging.info("Processing %s", message_data["mls"])
            if aws.mls_exists(mls_domain, message_data["mls"]):
                print "already exists"
                continue
            listing_key = bucket.get_key(message_data["key"])
            listing_html = listing_key.get_contents_as_string()
            # Parse it
            listing = realtylink.Listing(message_data["mls"], listing_html)
            # And insert it into SimpleDB
            listing_item = mls_domain.new_item(hash(message_data["mls"]))
            listing_item["mls"] = listing.mls
            listing_item["description"] = listing.description
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
            listing_item["city"] = listing.city
            listing_item["city"] = listing.city
            listing_item.add_value("prices", (listing.price, aws.get_iso_timestamp()))
            print listing_item
            #listing_item.save()
            
            # parse_queue.delete_message(m)
        else:
            time.sleep(5)

if __name__=="__main__":
    logging.getLogger().setLevel(logging.INFO) 
    sys.exit(main(sys.argv))