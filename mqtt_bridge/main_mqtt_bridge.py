#!python3

mqttclient_log=False #MQTT client logs showing messages
Log_worker_flag=True

import paho.mqtt.client as mqtt
import threading

import redis
import schedule

from command import command_input
import command
from mqtt_client import *

redis_conn = redis.Redis()

options=command.options

# === MAIN PROGRAM ===
if __name__ == "__main__" and len(sys.argv)>=2:
    options=command_input(options)
else:
    print("Need broker name and topics to continue.. exiting")
    raise SystemExit(1)

if not options["cname"]:
    # create random client id if one is not given
    r=random.randrange(1,10000)
    cname="logger-"+str(r)
else:
    cname="logger-"+str(options["cname"])

logging.basicConfig(level=options["loglevel"])

#print("logging level ",options["loglevel"])

logging.info("creating client name:"+cname)
client=initialise_clients(cname,mqttclient_log,cleansession=False,
username=options['username'],
password=options['password'],
broker=options['broker'],
port=options['port'],
topics=options['topics']) #create and initialise client object

client.redis_conn=redis_conn #make queue available as part of client

#try:
res=client.connect(client.broker,client.port)      #connect to broker
logging.info("connecting to broker " + client.broker)
logging.info("connecting to port " + str(client.port))
client.loop_start() #start loop

#except:
#    logging.warning("connection failed")
   
#loop and wait until interrupted 

while not client.connected_flag and not client.bad_connection_flag: #wait in loop
    print("In wait loop")
    time.sleep(1)

if client.bad_connection_flag:
    client.loop_stop()    #Stop loop
    sys.exit()

try:
    while True:
        time.sleep(1)
        pass
except KeyboardInterrupt:
    print("interrrupted by keyboard")

client.loop_stop() #start loop
time.sleep(3)

