import time

class Sleeper(object):
    """
    Sleeps for an increasing amount of time until it is reset
    """
    def __init__(self, base_delay):
        self.base_delay = base_delay
        self.current_delay = base_delay
        
    def reset(self):
        self.current_delay = self.base_delay
    
    def sleep(self):
        time.sleep(self.current_delay)
        if self.current_delay < (8*self.base_delay):
            self.current_delay = self.current_delay * 2

def from_iso(iso_timestamp):
    return time.strptime(iso_timestamp, "%Y-%m-%dT%H:%M:%S")

def get_iso_timestamp():
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())

def get_price_list(price_result):
    price_list = []
    if not isinstance(price_result, list):
        prices = [price_result]
    else:
        prices = price_result

    for val in prices:
        if isinstance(val, unicode):
            price, timestamp = eval(val)
        elif isinstance(val, tuple):
            price, timestamp = val
        else:
            raise Exception("Unkown val type: %s, %s" % (val, type(val)))
        price_list.append((price, timestamp))

    return price_list
    
def mls_exists(domain, mls, date = None):
    if date:
        # Check if the record exists or not, and if it does, check to see whether the time it was parsed
        # matches the timestamp of the value in s3
        rs = domain.select("SELECT * FROM mls WHERE mls='%s'" % mls)
        for result in rs:
            price_list = get_price_list(result["prices"])
            
            for price, timestamp in price_list:
                if date == timestamp:
                    return True
        return False
    else:
        rs = domain.select("SELECT * FROM mls WHERE mls='%s'" % mls)
        for result in rs:
            return result
        return False