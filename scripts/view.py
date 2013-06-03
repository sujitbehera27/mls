"""
View parsed data from the database
"""
import boto
import sys
import logging
import aws
import datetime
import cPickle as pickle

from optparse import OptionParser

sdb = boto.connect_sdb()
mls_domain = sdb.get_domain("mls")

CACHE = True
if CACHE:
    import memcache
    mc = memcache.Client(['127.0.0.1:11211'])


def cached_query(qry):
    key = str(abs(hash(qry)))
    if CACHE:
        pickled_result = mc.get(key)
    if CACHE and pickled_result:
        result = pickle.loads(pickled_result)
    else:
        print "cache miss for %s" % key
        result = []
        for rs in mls_domain.select(qry):
            row = {}
            for k,v in rs.items():
                row[k] = v
            result.append(row)
        print "put %s in cache" % key
        print len(pickle.dumps(result))
        mc.set(key, pickle.dumps(result))
    return result

def get_count():
    rs = cached_query("SELECT COUNT(*) FROM mls")
    for result in rs:
        return result["Count"]
        
def get_properties():
    rs = cached_query("SELECT mls, prices, area FROM mls")
    properties = []
    for result_row in rs:
        result_row["prices"] = aws.get_price_list(result_row["prices"], convert_to_float=True)
        result_row["current_price"] = result_row["prices"][-1]
        result_row["area"] = float(result_row["area"].replace(" sqft.", ""))
        properties.append(result_row)
    return properties    
        

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
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False, help="Verbose output")
    parser.add_option("-f", "--filter", action="store", dest="filter", help="Filter by MLS number")
    (options, args) = parser.parse_args()

    if options.list:
        if options.filter:
            rs = mls_domain.select("SELECT * FROM mls WHERE mls='%s'" % options.filter)
        else:
            rs = mls_domain.select("SELECT * FROM mls")
        for result in rs:
            if options.mls_only:
                print result["mls"]
            elif options.verbose:
                print result
            else:
                print format_result(result)
    
    if options.count:
        rs = mls_domain.select("SELECT COUNT(*) FROM mls")
        for result in rs:
            print "Count: %s" % result["Count"]

if __name__=="__main__":
    logging.getLogger().setLevel(logging.INFO) 
    sys.exit(main(sys.argv))
