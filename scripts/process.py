import logging
import redis
import sys
import json

log = logging.getLogger("searcher")

r = redis.StrictRedis(host='localhost', port=6379, db=0)
DATA_KEY = 'listings'

def main(args):
    dataset = r.hgetall(DATA_KEY)
    print "%s listings" % len(dataset)
    listings = [json.loads(listing) for listing in dataset.values()]
    total_price = sum([int(listing['price']) for listing in listings])
    print "Total price: %s" % total_price
    print "Average price: %s" % (total_price/len(listings))
    # print r.lrange('processed', 0, r.llen('processed'))

if __name__=="__main__":
    logging.basicConfig()
    for handler in logging.getLogger().handlers:
        handler.setFormatter(logging.Formatter("%(asctime)s %(name)-19s %(levelname)-7s - %(message)s"))
    formatter = logging.Formatter('%(name)s: %(levelname)s %(message)s')
    logging.getLogger().setLevel(logging.INFO)
    
    sys.exit(main(sys.argv))
