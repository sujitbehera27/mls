import logging
import redis
import sys
import json
import locale
from lib import realtylink
locale.setlocale(locale.LC_ALL, 'en_US')

log = logging.getLogger("searcher")

r = redis.StrictRedis(host='localhost', port=6379, db=0)
DATA_KEY = 'listings'

AVG_INCOME = 71140.0  # From 2012 stat can census data
# From http://www.metrovancouver.org/about/publications/Publications/AverageandMedianHouseholdIncomebyMunicipality.pdf, using 2011 avg incomes
AVG_INCOME_BY_CITY = {
    "VANCOUVER_WEST": 83666.0,
    "VANCOUVER_EAST": 83666.0,
    "WEST_VANCOUVER": 166221.0,
    "BURNABY": 72238.0,
    "COQUITLAM": 83640.0,
    "MAPLE_RIDGE": 82827.0,
    "NORTH_VANCOUVER": 116771.0,
    "NEW_WESTMINSTER": 67870.0,
    "PITT_MEADOWS": 81339.0,
    "PORT_MOODY": 98979.0,
    "PORT_COQUITLAM": 83635.0,
    "RICHMOND": 77782.0,
    "LADNER": 71140.0,  # not included in source above
    "TSAWWASSEN": 104387.0
}


# TODO: Try to figure out average incomes for each area, and compare to prices

def listing_to_str(listing):
    return "%s (%s)" % (listing['address'], listing['mls'])


def format_price(price):
    return "$%s" % locale.format("%d", price, grouping=True)


def format_city_name(name):
    name = name.replace("_", " ")
    name = name.lower()
    return " ".join([n.capitalize() for n in name.split(" ")])


def keyfunc(listing):
    return listing['price']


def avg(l, key):
    return sum([key(i) for i in l])/len(l) if len(l) > 0 else float('nan')


def median(l, key):
    l = sorted(l, key=keyfunc)
    if len(l) % 2 == 0:
        return (key(l[len(l)/2-1]) + key(l[len(l)/2])) / 2.0
    else:
        return key(l[len(l)/2])


def generate_stats(listings, city=None):
    stats = {
        "average": avg(listings, key=keyfunc),
        "total": sum([l['price'] for l in listings]),
        "min": min(listings, key=keyfunc),
        "max": max(listings, key=keyfunc),
        "median": median(listings, key=keyfunc),
        "count": len(listings),
    }
    avg_income = AVG_INCOME_BY_CITY[city] if (city and city in AVG_INCOME_BY_CITY) else AVG_INCOME
    stats['average_multiple'] = stats['average'] / avg_income
    stats['median_multiple'] = stats['median'] / avg_income
    stats['avg_income'] = avg_income
    return stats


def print_stats(stats, prefix="    "):
    print "%s%s listings" % (prefix, stats['count'])
    print "%sAverage price: %s" % (prefix, format_price(stats['average']))
    print "%sMedian price: %s" % (prefix, format_price(stats['median']))
    print "%sIncome multiple (mean, median): %0.2f, %0.2f" % (prefix, stats['average_multiple'], stats['median_multiple'])
    print "%sCheapest property: %s (%s)" % (prefix, format_price(stats['min']['price']), listing_to_str(stats['min']))
    print "%sMost expensive property: %s (%s)" % (prefix, format_price(stats['max']['price']), listing_to_str(stats['max']))


def print_stats_for_city(city, listings):
    print "%s:" % format_city_name(city[0])
    city_listings = [l for l in listings if l['city'] == city[1]]
    stats = generate_stats(city_listings, city[0])
    print_stats(stats)
    print "    Average income: %s" % format_price(stats['avg_income'])
    print "    Houses:"
    print_stats(generate_stats([l for l in city_listings if l['property_type'] == realtylink.HOUSE], city[0]), prefix=" "*8)
    print "    Condos:"
    print_stats(generate_stats([l for l in city_listings if l['property_type'] == realtylink.APARTMENT], city[0]), prefix=" "*8)
    print "    Townhouses:"
    print_stats(generate_stats([l for l in city_listings if l['property_type'] == realtylink.TOWNHOUSE], city[0]), prefix=" "*8)


def main(args):
    dataset = r.hgetall(DATA_KEY)
    listings = [json.loads(listing) for listing in dataset.values()]
    for listing in listings:
        listing['price'] = int(listing['price'])

    print "Overall:"
    print_stats(generate_stats(listings))

    for city in sorted(realtylink.cities.items()):
        print ""
        print_stats_for_city(city, listings)


if __name__ == "__main__":
    logging.basicConfig()
    for handler in logging.getLogger().handlers:
        handler.setFormatter(logging.Formatter("%(asctime)s %(name)-19s %(levelname)-7s - %(message)s"))
    formatter = logging.Formatter('%(name)s: %(levelname)s %(message)s')
    logging.getLogger().setLevel(logging.INFO)

    sys.exit(main(sys.argv))
