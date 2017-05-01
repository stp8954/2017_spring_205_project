from xml.etree import ElementTree
from urllib import urlencode
from confluent_kafka import Producer
import json
import time
import datetime 
from urllib2 import HTTPError
import urllib2

NEXTBUS_SERVICE_URL = "http://webservices.nextbus.com/service/publicXMLFeed"


def url_fetcher(url):
    """
    Fetch url using urllib2 library 
    """
    return urllib2.urlopen(url)

_url_fetcher = url_fetcher

_cache = None

def fetch_xml(url):
    """ Parse the fetched data into xml tree """
    return ElementTree.parse(_url_fetcher(url))

def make_nextbus_url(command, a = None, *args):
    real_args = []
    real_args.append(('command', command))
    if a is not None:
        real_args.append(('a', a))
    real_args.extend(args)
    return '?'.join([NEXTBUS_SERVICE_URL, urlencode(real_args, True)])

def fetch_nextbus_url(*args, **kwargs):
    url = make_nextbus_url(*args, **kwargs)
    return fetch_xml(url)

def get_all_vehicle_locations(agency_tag):
    etree = fetch_nextbus_url("vehicleLocations", agency_tag, ('t', 0))
    return map(lambda elem : Vehicle.from_elem(elem,agency_tag), etree.findall("vehicle"))

def _standard_repr(self):
    return "%s(%s)" % (self.__class__.__name__, self.__dict__)


class Vehicle:
    """
    This class holds the vehicle data fetched from NextBus API 
    """
    __repr__ = _standard_repr

    def __init__(self):
       self.id = None
       self.route_tag = None
       self.direction_tag = None
       self.latitude = None
       self.longitude = None
       self.seconds_since_report = None
       self.predictable = None
       self.heading = None
       self.leading_vehicle_id = None 


    @classmethod
    def from_elem(cls, elem, atag):
        self = cls()
        self.id = elem.get("id")
        self.route_tag = elem.get("routeTag")
        self.direction_tag = elem.get("dirTag")
        self.latitude = float(elem.get("lat"))
        self.longitude = float(elem.get("lon"))
        self.seconds_since_report = int(elem.get("secsSinceReport"))
        self.heading = float(elem.get("heading"))
        self.leading_vehicle_id = elem.get("leadingVehicleId")
        self.predictable = (elem.get("predictable") == "true")
        self.a_tag = atag
        self.timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        if self.route_tag == "null":
            self.route_tag = None
        if self.direction_tag == "null":
            self.direction_tag = None
        return self

#create kafka Producer 
p = Producer({'bootstrap.servers': 'localhost'})

# region tags to fetch bus data for . These tags belong to the bus services servicing area in and around SFO
tags = ('actransit','camarillo','west-hollywood','dumbarton','escalon','foothill','glendale','south-coast','lametro','lametro-rail','sf-mission-bay','moorpark','omnitrans','pvpta','sf-muni','simi-valley','sct','thousand-oaks','unitrans','ucsf','vista')
topic = 'raw_loc_data'
count = 0

while(True):
    for tag in tags:
        try:
            # get all vehivle location 
            locs = get_all_vehicle_locations(tag)
            for loc in locs:
                #Convert vehicle data to json
                j = json.dumps(loc.__dict__)
                #push the data to kafka topic
                p.produce(topic,j)
                count = count + 1
            p.flush()
            print(count)

        except HTTPError as e:
            print(e)

        #Next bus throttles if connections are made within 10ms. So sleep for 10ms before next call
        time.sleep(10)
            
    time.sleep(10)
