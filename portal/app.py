#!/usr/bin/env python
from flask import Flask, render_template, session, request
from flask_socketio import SocketIO, emit, join_room, leave_room, \
    close_room, rooms, disconnect

from confluent_kafka import Consumer, KafkaError
import json
from pymongo import MongoClient 
from pykafka import KafkaClient
import geohash as gh 
from datetime import datetime

async_mode = "threading"

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, async_mode=async_mode)
thread = None

client = MongoClient('mongodb://127.0.0.1:27017')
db = client.bus_locationdb
collection = db.bus_aggregate
fmt = '%Y-%m-%d %H:%M:%S'


def background_thread2():
    """Background task created when a client connects to the socket via SocketIO
    It then subscribes to kafka and pipes messages from kafka to the client
    ."""
    count = 0
    #create kafka consumer
    c = Consumer({'bootstrap.servers': 'localhost:9092',  'group.id': 'mygroup','default.topic.config': {'auto.offset.reset': 'earliest' }})
    c.subscribe(['raw_loc_data'])

    while True:
        #socketio.sleep(1)
        count += 1
        msg = c.poll(timeout = 1.0)
        if msg is None:
            continue;
        #if msg is not null, extract lat , lon and bus id data from the msg and send to client via SocketIO
        if not msg.error():
            print(msg.value())
            d = json.loads(msg.value())

            lat = float(d['latitude']) 
            lon = float(d['longitude']) 
            bus_id = d['id']
            a_tag = d['a_tag']

            #if a_tag == 'sf-muni':
            socketio.emit('my_response',
                      {'data': "{0}:{1},{2}".format(bus_id,lat,lon), 'count': count, 'bus_id': bus_id, 'lat':lat, 'lon':lon},
                      namespace='/test')
            print("{0}:{1},{2}".format(bus_id,lat,lon))


client = KafkaClient(hosts="127.0.0.1:9092")
topic = client.topics['raw_loc_data']

#Not used for this application
def background_thread():
    """This is a dummy method to work with a different kafka api"""
    count = 0
    consumer = topic.get_simple_consumer(auto_commit_enable= False,auto_offset_reset= -1, reset_offset_on_start= True)
    while True:
        for msg in consumer:
            count += 1
            if msg is None:
                continue;
            
            d = json.loads(msg.value)
            lat = float(d['latitude']) 
            lon = float(d['longitude']) 
            bus_id = d['id']
            a_tag = d['a_tag']
            print("{0}:{1},{2}".format(bus_id,lat,lon))
            if a_tag == 'sf-muni':
                socketio.emit('my_response',
                      {'data': "{0}:{1},{2}".format(bus_id,lat,lon), 'count': count, 'bus_id': bus_id, 'lat':lat, 'lon':lon},
                      namespace='/test')
                print("{0}:{1},{2}".format(bus_id,lat,lon))


# this methos queries mongodb for aggregate data for the datetime in the query string. It then constructs an HTML with code for 
#displaying heat maps on google maps
@app.route('/heatmap',methods=['GET'])
def get_aggregates():
    start =  datetime.strptime(request.args.get('datetime'),fmt)
    points = collection.find({"$and":[{"aggtime": start},{"hashlen": 7} ]})
    output = []
    heatdata = ""
    for p in points:
        loc = gh.decode_exactly(p['geohash7'])
        str = "{" +"location: new google.maps.LatLng({1}, {2}), weight: {0}".format( p['count'] * 20,loc[0],loc[1])+ "},"
        heatdata += str
    return """
    <!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Heatmaps</title>
    <style>
      #map {
        height: 100%;
      }
      html, body {
        height: 100%;
        margin: 0;
        padding: 0;
      }
    </style>
  </head>

  <body>
    
    <div id="map"></div>
    <script>

      var map, heatmap;

      function initMap() {
        map = new google.maps.Map(document.getElementById('map'), {
          zoom: 13,
          center: {lat: 37.775, lng: -122.434},
          mapTypeId: 'satellite'
        });

        heatmap = new google.maps.visualization.HeatmapLayer({
          data: getPoints(),
          map: map,
          opacity: 1
        });
      }


      function getPoints() {
        return [
          #HEATDATA#
        ];
      }
    </script>
    <script async defer
        src="https://maps.googleapis.com/maps/api/js?key=AIzaSyDA4bPlz1-ity0j9rUXInpx4nvUxJVpQ28&libraries=visualization&callback=initMap">
    </script>
  </body>
</html>
    """.replace("#HEATDATA#",heatdata)

@app.route('/')
def index():
    return render_template('index.html', async_mode=socketio.async_mode)

#listens for connect command from remote client.
@socketio.on('connect', namespace='/test')
def test_connect():
    global thread
    if thread is None:
        thread = socketio.start_background_task(target=background_thread2)
    emit('my_response', {'data': 'Connected', 'count': 0})


@socketio.on('disconnect', namespace='/test')
def test_disconnect():
    print('Client disconnected', request.sid)

# launch the main app
if __name__ == '__main__':
    socketio.run(app,host='0.0.0.0', port=80, debug=True)
