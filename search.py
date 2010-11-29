import sys
import boto
import time
import logging
import aws

from lib import realtylink

sdb = boto.connect_sdb()
sqs = boto.connect_sqs()

mls_domain = sdb.get_domain("mls")
mls_queue = sqs.get_queue("mls_fetcher")

test_results = [('V847348', '$788,000.00'), ('V845315', '$749,000.00'), ('V831476', '$799,000.00'), ('V835285', '$658,000.00'), ('V842022', '$658,000.00'), ('V854413', '$838,000.00'), ('V851993', '$850,000.00'), ('V839052', '$838,000.00'), ('V850019', '$849,000.00'), ('V842451', '$879,000.00'), ('V854759', '$938,000.00'), ('V837202', '$1,038,000.00'), ('V826859', '$998,000.00'), ('V843154', '$998,000.00'), ('V843917', '$998,000.00'), ('V845055', '$1,199,000.00'), ('V852994', '$1,199,000.00'), ('V818446', '$1,129,000.00'), ('V826098', '$1,098,000.00'), ('V838474', '$1,168,000.00'), ('V810654', '$1,299,000.00'), ('V842408', '$1,288,000.00'), ('V843628', '$1,268,000.00'), ('V829481', '$1,227,000.00'), ('V853976', '$1,199,000.00'), ('V837986', '$1,488,000.00'), ('V834877', '$1,690,000.00'), ('V838126', '$1,399,000.00'), ('V835566', '$1,590,000.00'), ('V836286', '$1,758,900.00'), ('V842776', '$1,850,000.00'), ('V843946', '$2,880,000.00'), ('V855651', '$2,398,000.00')]

def pad_price(normalized):
    padded = "0" * (8-len(normalized)) + normalized
    return padded
    
def needs_update(mls, price):
    rs = mls_domain.select("SELECT * FROM mls WHERE mls='%s'" % mls)
    for item in rs:
        # If there is an item with a different price, update this item
        if item['price'] != price:
            return True
        else:
            return False
    return True

def main(argv):
    # results = realtylink.search(property_type=realtylink.TOWNHOUSE, 
    #                   city=realtylink.cities["VANCOUVER_WEST"], 
    #                   areas=[realtylink.regions["VANCOUVER_WEST"][0]])
    # print results
    for mls, price in test_results[:1]:
        normalized_price = pad_price(realtylink.fix_price(price))
        print mls, normalized_price
        if needs_update(mls, normalized_price):
            logging.info("Queuing %s" % mls)
            m = mls_queue.new_message(mls)
            mls_queue.write(m)
    
    # Iterate through cities and regions and property types
    # For each result, get the price and MLS #
    # Query SDB to see if the MLS # exists and when it was last updated
    # If necessary, add it to SQS

if __name__=="__main__":
    logging.getLogger().setLevel(logging.DEBUG) 
    sys.exit(main(sys.argv))
