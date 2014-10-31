from BeautifulSoup import BeautifulSoup
import urllib
import logging
import re

# Definitions
# TODO: Add more definitions
APARTMENT = 1
TOWNHOUSE = 2
HOUSE = 5

cities = {
    "VANCOUVER_WEST": 9,
    "VANCOUVER_EAST": 10,
    "WEST_VANCOUVER": 19,
    "BURNABY": 11,
    "COQUITLAM": 16,
    "MAPLE_RIDGE": 18,
    "NORTH_VANCOUVER": 20,
    "NEW_WESTMINSTER": 12,
    "PITT_MEADOWS": 913,
    "PORT_MOODY": 15,
    "PORT_COQUITLAM": 17,
    "RICHMOND": 13,
    "LADNER": 852,
    "TSAWWASSEN": 851,
    "ABBOTSFORD": 117,
    "CLOVERDALE": 114,
    "LANGLEY": 116,
    "MISSION": 118,
    "NORTH_DELTA": 111,
    "NORTH_SURREY": 112,
    "SURREY": 113,
    "WHITE_ROCK": 115,
}
metro_area = {
    "VANCOUVER_WEST": "GV",
    "VANCOUVER_EAST": "GV",
    "WEST_VANCOUVER": "GV",
    "BURNABY": "GV",
    "COQUITLAM": "GV",
    "MAPLE_RIDGE": "GV",
    "NORTH_VANCOUVER": "GV",
    "NEW_WESTMINSTER": "GV",
    "PITT_MEADOWS": "GV",
    "PORT_MOODY": "GV",
    "PORT_COQUITLAM": "GV",
    "RICHMOND": "GV",
    "LADNER": "GV",
    "TSAWWASSEN": "GV",
    "ABBOTSFORD": "FV",
    "CLOVERDALE": "FV",
    "LANGLEY": "FV",
    "MISSION": "FV",
    "NORTH_DELTA": "FV",
    "NORTH_SURREY": "FV",
    "SURREY": "FV",
    "WHITE_ROCK": "FV",
}
regions = {
    "VANCOUVER_WEST": (21, 22, 23, 24, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 39, 40,  41, 42, 43, 44, 10105, 853),
    "VANCOUVER_EAST": (235, 241, 432, 237, 234, 244, 855, 246, 240, 238, 236, 243, 245, 242, 233, 239, 247),
    "WEST_VANCOUVER": (506, 944, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 370, 371, 372, 373, 374, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 434),
    "BURNABY": (248, 249, 250, 251, 253, 254, 255, 256, 257, 258, 259, 260, 261, 856, 857, 858, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282),
    "COQUITLAM": (324, 325, 326, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 342, 343, 344, 428, 429, 430),
    "MAPLE_RIDGE": (879, 881, 882, 884, 886, 887, 889, 890, 891, 892, 893, 894, 359),
    "NORTH_VANCOUVER": (895, 910, 393, 394, 395, 396, 397, 398, 399, 400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 419, 420, 421, 422, 423, 424, 425, 426),
    "NEW_WESTMINSTER": (1539, 1540, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294, 295),
    "PITT_MEADOWS": (880, 883, 885, 888, 909, 914),
    "PORT_MOODY": (435, 436, 877, 314, 315, 316, 317, 318, 323, 319, 320, 321, 322),
    "PORT_COQUITLAM": (1541, 878, 345, 346, 347, 348, 349, 350, 351, 352, 433),
    "RICHMOND": (915, 916, 917, 918, 919, 920, 921, 922, 923, 924, 925, 926, 927, 928, 929, 930, 931, 932, 933, 934, 935, 936, 937, 938, 939, 940, 941, 942, 943, 310),
    "TSAWWASSEN": (859, 860, 861, 862, 863, 864, 865, 866),
    "LADNER": (867, 868, 869, 870, 871, 872, 873, 874, 875, 876),
    "ABBOTSFORD": (157, 158, 159, 160, 161, 162, 163, 900, 901),
    "CLOVERDALE": (140, 141, 896),
    "LANGLEY": (148, 149, 150, 151, 152, 153, 156, 154, 155, 911, 912),
    "MISSION": (170, 164, 165, 166, 167, 168, 169, 902, 903),
    "NORTH_DELTA": (119, 120, 121, 122),
    "NORTH_SURREY": (123, 124, 125, 126, 127, 128, 129, 130),
    "SURREY": (131, 132, 134, 136, 137, 138, 139),
    "WHITE_ROCK": (142, 143, 144, 145, 146, 147, 897, 898, 899),
}

proxyList = []


def getHttpData(url):
    f = urllib.urlopen(url)
    data = f.read()
    return data


def fix_price(price_string):
    return price_string.replace("$", "").replace(",", "").split(".")[0]


def find_mls_numbers(html):
    results = {}

    # A two step process because the page layout makes it a pain to associate
    # the address with an MLS number and price as we find it
    # First pre-process to find the mls numbers + price
    links = html.findAll("a", href=re.compile("Detail.cfm|mortgage.cfm"))
    p = re.compile(".*?MLS\=(\w\d+)", re.DOTALL|re.MULTILINE|re.IGNORECASE|re.UNICODE)
    price_p = re.compile(".*?p\=(.*?)\&.*?mls_num=(\w\d+)")
    for link in links:
        result = p.match(str(link))
        if result and result.group(1) not in results:
            results[result.group(1)] = None
        result = price_p.match(str(link))
        if result:
            results[result.group(2)] = {"price":fix_price(result.group(1)), "mls":result.group(2)}

    # Then, go back through to find the address
    cells = html.findAll("td", rowspan="2")
    for cell in cells:
        try:
            address = cell.font.string.strip()
            mls = cell.parent.findNextSibling("tr", align="middle").findAll("td")[0].font.string.strip()
            bedrooms = cell.parent.findNextSibling("tr", align="middle").findAll("td")[2].font.string.strip()
            baths = cell.parent.findNextSibling("tr", align="middle").findAll("td")[3].font.string.strip()
            results[mls]['address'] = address
            results[mls]['bedrooms'] = bedrooms.replace("&nbsp;", "")
            results[mls]['baths'] = baths
        except Exception as e:
            print "Error finding address: %s" % e

    return results


# API
def search(price=(0, 100000000), age=(0, 200), min_bathrooms=0, min_bedrooms=0, property_type=APARTMENT, areas=[], city=None):
    results = []
    if not city:
        raise Exception("Must specify a city")
    city_id = cities[city]
    if areas == []:
        areas = [26]
    areas = [str(area) for area in areas]
    parameters = {}
    parameters["RSPP"] = 5
    parameters["AIDL"] = ",".join(areas)
    parameters["SRTB"] = "P_Price"
    parameters["BCD"] = metro_area[city]
    parameters["imdp"] = city_id
    parameters["ERTA"] = "False"
    parameters["MNAGE"] = age[0]
    parameters["MXAGE"] = age[1]
    parameters["MNBT"] = min_bathrooms
    parameters["MNBD"] = min_bedrooms
    parameters["PTYTID"] = property_type
    parameters["MNPRC"] = price[0]
    parameters["MXPRC"] = price[1]
    parameters["SCTP"] = "RS"

    url = "http://www.realtylink.org/prop_search/Summary.cfm?" + "&".join(["%s=%s" % (k, v) for k, v in parameters.items()])
    html = getHttpData(url)
    data = BeautifulSoup(html)
    #logging.debug("Summary: %s", unicode(str(data), errors='ignore'))
    countCells = data.findAll("td", colspan="6", valign="center")
    if len(countCells) < 1:
        logging.error("Couldn't find result count cell")
        return []
    countCell = countCells[0]
    summaries = countCell.findAll("b")
    if len(summaries) < 1:
        logging.error("Couldn't find result count")
        return []
    summary_expr = re.compile(".*?Now Viewing Results (\d+) \- (\d+) of (\d+).*?", re.DOTALL|re.MULTILINE|re.IGNORECASE|re.UNICODE)
    result = summary_expr.match(str(summaries[0]))
    if result:
        first_result, last_result, total_results = int(result.group(1)), int(result.group(2)), int(result.group(3))
    elif not result and "Now Viewing Result 1" in str(summaries[0]):
        first_result, last_result, total_results = 1, 1, 1
    else:
        logging.error("Couldn't decode result summary")
        return []
    logging.info("Search resulted in %d listings", total_results)

    results_left = total_results
    next_page_index = -4
    while results_left > 0:
        if results_left != total_results:
            parameters["Page"] = "Next"
            next_page_index += 5
            parameters["rowp"] = next_page_index
            url = "http://www.realtylink.org/prop_search/Summary.cfm?" + "&".join(["%s=%s" % (k,v) for k,v in parameters.items()])
            html = getHttpData(url)
            data = BeautifulSoup(html)
        mls_numbers = find_mls_numbers(data)
        if len(mls_numbers) == 0:
            logging.error("Could not find any results at %s" % url)
            return
        logging.info("On this page: %s", str(mls_numbers))
        results_left -= len(mls_numbers)
        results.extend(mls_numbers.items())
    return results
    
class Listing:
    def __init__(self, mls, html):
        self.mls = mls        
        self.price = 0
        self.unit = 0
        self.address = ""
        self.unit = ""
        self.region = ""
        self.city = ""
        self.area = 0
        self.age = 0
        self.bedrooms = 0
        self.bathrooms = 0
        self.type = ""
        self.description = ""
        self.maintenance_fee = 0
        self.features = ""
        self.nameMap = {"Finished Floor Area":"area", "Property Type":"type","Bedrooms":"bedrooms",
                        "Bathrooms":"bathrooms","Age":"age", "Maintenance fee":"maintenance_fee", "Features":"features"}
        
        
        self.parse_data(BeautifulSoup(html))
        for name in self.nameMap.values():
            attr = getattr(self, name)
            logging.debug("%s: %s", name, attr)
        logging.debug("Price: %s", self.price)
        logging.debug("Unit: %s", self.unit)
        logging.debug("Address: %s", self.address)
        logging.debug("Region: %s", self.region)
        logging.debug("City: %s", self.city)
        logging.debug("Description: %s", self.description)
    
    def __str__(self):
        return "%s: %s bedrooms, %s bathrooms - %s" % (self.mls, self.bedrooms, self.bathrooms, self.price)
        
    def parse_data(self, html):
        tables = html.findAll('table', align="center", width=500)
        if len(tables) < 1:
            logging.error("Couldn't find details table")
            return
        table = tables[0]
        
        found_image = False
        descr_table_img = html.findAll('img', alt=self.mls)
        if len(descr_table_img) == 0:
            logging.debug("Looking for alternate picture")
            descr_table_img = html.findAll('img', alt='NO PICTURE AVAILABLE FOR THIS PROPERTY')
        if len(descr_table_img) > 0:
            descr_table = descr_table_img[0].parent.parent.parent
            for td in descr_table.findAll("td"):
                if found_image:
                    descr = td.font
                    if descr is not None and descr.string is not None:
                        self.description = descr.string.strip()
                    break
                if td.findAll("img", alt=self.mls) or td.findAll('img', alt='NO PICTURE AVAILABLE FOR THIS PROPERTY'):
                    found_image = True
                    
        rows = table.findAll('tr', align="left")
        for row in rows:
            cells = row('td')
            for i in range(0,len(cells),2):
                if len(cells) > i+1:
                    valid = True
                    for cell in (cells[i], cells[i+1]):
                        if cell.font is None or cell.font.string is None:
                            valid = False
                    if valid:
                        name = cells[i].font.string.strip().replace(":", "")
                        data = cells[i+1].font.string.strip()
                        if name in self.nameMap:
                            if name == "Bathrooms" and "Total" in data:
                                full, half = re.findall(":(\d*)", data)
                                if not full:
                                    full = 0
                                if not half:
                                    half = 0
                                bathroom_count = int(full) + (int(half)*.5)
                                data = str(bathroom_count)
                            setattr(self, self.nameMap[name], data) 
                    i = i+1
        tables = html.findAll('table', width="563")
        if len(tables) < 1:
            logging.error("Couldn't find price table")
            return
        for table in tables:
            table = tables[0]
            data = table.findAll('font', size=2)
            if len(data) < 1:
                logging.error("Couldn't find font tag")
                continue
            data = data[0]
            fields = data.findAll('b')
            if len(fields) < 2:
                logging.error("Couldn't find price and address fields")
                continue
            address_fields = fields[0].string.strip().split(",")
            (unit, address) = self.parse_address(address_fields[0].strip().replace("#", ""))
            self.unit = unit
            self.address = address.strip()
            self.city = address_fields[2].strip()
            self.region = address_fields[1].strip()
            self.price = fields[1].string.strip()
            
    def parse_address(self, text):
        numberCount = 0
        inNumber = False
        numbers = []
        for c in text:
            if c.isdigit():
                if not inNumber:
                    numberCount = numberCount + 1
                    numbers.append(c)
                else:
                    numbers[numberCount-1] = numbers[numberCount-1] + c
                inNumber = True
            else:
                inNumber = False
        if numberCount > 1:
            return (numbers[0], text.replace(numbers[0], ""))
        else:
            return (None, text)