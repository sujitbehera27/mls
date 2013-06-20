"""
Looks for MLS listings to be parsed, and adds them to a list for fetcher.py to grab
"""
import sys
import time
import logging
import redis
import json
from logging.handlers import SysLogHandler

from lib import realtylink

log = logging.getLogger("searcher")

test_results = [('V847348', '$788,000.00'), ('V845315', '$749,000.00'), ('V831476', '$799,000.00'), ('V835285', '$658,000.00'), ('V842022', '$658,000.00'), ('V854413', '$838,000.00'), ('V851993', '$850,000.00'), ('V839052', '$838,000.00'), ('V850019', '$849,000.00'), ('V842451', '$879,000.00'), ('V854759', '$938,000.00'), ('V837202', '$1,038,000.00'), ('V826859', '$998,000.00'), ('V843154', '$998,000.00'), ('V843917', '$998,000.00'), ('V845055', '$1,199,000.00'), ('V852994', '$1,199,000.00'), ('V818446', '$1,129,000.00'), ('V826098', '$1,098,000.00'), ('V838474', '$1,168,000.00'), ('V810654', '$1,299,000.00'), ('V842408', '$1,288,000.00'), ('V843628', '$1,268,000.00'), ('V829481', '$1,227,000.00'), ('V853976', '$1,199,000.00'), ('V837986', '$1,488,000.00'), ('V834877', '$1,690,000.00'), ('V838126', '$1,399,000.00'), ('V835566', '$1,590,000.00'), ('V836286', '$1,758,900.00'), ('V842776', '$1,850,000.00'), ('V843946', '$2,880,000.00'), ('V855651', '$2,398,000.00')]

r = redis.StrictRedis(host='localhost', port=6379, db=0)
DATA_KEY = 'listings'
PROCESSED_LIST = 'processed'

def pad_price(normalized):
    padded = "0" * (8-len(normalized)) + normalized
    return padded
    
def main(argv):
    add_count = 0
    
    already_processed = [int(region) for region in r.lrange(PROCESSED_LIST, 0, r.llen(PROCESSED_LIST))]
    print already_processed
    for city_name, city_id in realtylink.cities.items():
        for region in realtylink.regions[city_name]:
            if int(region) in already_processed:
                print "Skipping region %s, it's already been searched" % region
                continue
            property_type = realtylink.HOUSE
            log.info("Searching %s - %s for %s" % (city_name, region, property_type))
            
            results = realtylink.search(property_type=property_type, 
                                        city=city_id, 
                                        areas=[region])

            for mls, data in results:
                normalized_price = realtylink.fix_price(data['price'])
                print mls, normalized_price, data['address']
                data['city'] = city_id
                data['region'] = region
                jsonified = json.dumps(data)
                r.hset(DATA_KEY, mls, jsonified)
            r.rpush(PROCESSED_LIST, region)
            time.sleep(5)
    
    log.info("Added %s entries to the parse queue" % add_count)

if __name__=="__main__":
    logging.basicConfig()
    for handler in logging.getLogger().handlers:
        handler.setFormatter(logging.Formatter("%(asctime)s %(name)-19s %(levelname)-7s - %(message)s"))
    formatter = logging.Formatter('%(name)s: %(levelname)s %(message)s')
    logging.getLogger().setLevel(logging.INFO)
    
    sys.exit(main(sys.argv))
