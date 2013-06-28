import logging
import redis
import sys
import json
import locale
locale.setlocale(locale.LC_ALL, 'en_US')

log = logging.getLogger("searcher")

r = redis.StrictRedis(host='localhost', port=6379, db=0)
DATA_KEY = 'listings'

def listing_to_str(listing):
    return "%s (%s)" % (listing['address'], listing['mls'])

def format_price(price):
    return "$%s" % locale.format("%d", price, grouping=True)

def keyfunc(listing):
    return listing['price']

def avg(l, key):
    return sum([key(i) for i in l])/len(l) if len(l) > 0 else float('nan')

def main(args):
    dataset = r.hgetall(DATA_KEY)
    print "%s listings" % len(dataset)
    listings = [json.loads(listing) for listing in dataset.values()]
    for listing in listings:
        listing['price'] = int(listing['price'])

    total_price = sum([int(listing['price']) for listing in listings])
    print "Total price: %s" % format_price(total_price)
    print "Average price: %s" % format_price(total_price/len(listings))
    van_west = [listing for listing in listings if listing['city'] == 9]
    van_east = [listing for listing in listings if listing['city'] == 10]
    print "%s listings in Van West, %s listings in Van East" % (len(van_west), len(van_east))

    lowest = min(listings, key=keyfunc)
    highest = max(listings, key=keyfunc)
    print "Lowest price: %s (%s)" % (format_price(lowest['price']), listing_to_str(lowest))
    print "Highest price: %s (%s)" % (format_price(highest['price']), listing_to_str(highest))
    print "----"
    print "Zones:"
    zones = {}
    for listing in listings:
        zone = listing.get('zone', None)
        zones.setdefault(zone, []).append(listing)
    for zone, zone_listings in zones.items():
        min_listing = min(zone_listings, key=keyfunc)
        max_listing = max(zone_listings, key=keyfunc)
        avg_price = avg(zone_listings, key=keyfunc)
        print "%s: %s listings (min: %s, max: %s, avg: %s)" % (zone, len(zone_listings),
            format_price(min_listing['price']), format_price(max_listing['price']), format_price(avg_price))

if __name__=="__main__":
    logging.basicConfig()
    for handler in logging.getLogger().handlers:
        handler.setFormatter(logging.Formatter("%(asctime)s %(name)-19s %(levelname)-7s - %(message)s"))
    formatter = logging.Formatter('%(name)s: %(levelname)s %(message)s')
    logging.getLogger().setLevel(logging.INFO)
    
    sys.exit(main(sys.argv))
