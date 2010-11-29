import time

def from_iso(iso_timestamp):
    return time.strptime(iso_timestamp, "%Y-%m-%dT%H:%M:%S")

def get_iso_timestamp():
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())

def mls_exists(domain, mls):
    rs = domain.select("SELECT * FROM mls WHERE mls='%s'" % mls)
    for result in rs:
        return True
    return False