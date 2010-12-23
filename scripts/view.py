"""
View parsed data from the database
"""
import boto
import sys
import logging
import aws
import datetime

from optparse import OptionParser

sdb = boto.connect_sdb()
mls_domain = sdb.get_domain("mls")


def format_result(result):
    price_list = aws.get_price_list(result["prices"])
    if len(price_list) > 1:
        prices = [str(price) for price,timestamp in price_list]
        price = ", ".join(prices)
    else:
        price, _ = price_list[0]
    
    last_seen = datetime.datetime(*aws.from_iso(result["last_seen"])[:6])
    last_seen_str = str(datetime.datetime.now() - last_seen)
    return "%s - %s: %s br %s: %s (last seen %s, %s)" % (result["city"], result["region"], result["bedrooms"], result["type"], price, last_seen_str, result["mls"])

def main(argv):
    parser = OptionParser()
    
    parser.add_option("-l", "--list", action="store_true", dest="list", default=False, help="List all entries")
    parser.add_option("-c", "--count", action="store_true", dest="count", default=False, help="Display a count of all entries")
    parser.add_option("-m", "--mls-only", action="store_true", dest="mls_only", default=False, help="Only display MLS numbers")
    (options, args) = parser.parse_args()

    if options.list:
        rs = mls_domain.select("SELECT * FROM mls")
        for result in rs:
            if options.mls_only:
                print result["mls"]
            else:
                print format_result(result)
    
    if options.count:
        rs = mls_domain.select("SELECT COUNT(*) FROM mls")
        for result in rs:
            print "Count: %s" % result["Count"]

if __name__=="__main__":
    logging.getLogger().setLevel(logging.INFO) 
    sys.exit(main(sys.argv))
