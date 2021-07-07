#!python3

mqttclient_log=False #MQTT client logs showing messages
Log_worker_flag=True
import paho.mqtt.client as mqtt
import threading
from collections import deque
from command import command_input
import command
from mqtt_client import *
import parquet_logger

msg_queue = deque() # cache all messages received from the broker

options=command.options

def log_worker():
    """runs in own thread to log data from queue"""
    log=parquet_logger.Parquet_logger(log_dir=log_dir,MAX_LOG_SIZE=options["log_max_size"])
    while Log_worker_flag:
        #print("worker running ",csv_flag)
        time.sleep(0.01)
        #time.sleep(2)
        while msg_queue:
            jdata = msg_queue.popleft()
            topic=jdata["topic"]
            del jdata["topic"]
            data = [jdata]
            if data is None:
                continue
            log.log_message(data,topic=topic)
            # if csv_flag:
            #      log.log_data(results)
            # else:
            #     log.log_json(results)
    log.close_file()

# === MAIN PROGRAM ===
if __name__ == "__main__" and len(sys.argv)>=2:
    options=command_input(options)
else:
    print("Need broker name and topics to continue.. exiting")
    raise SystemExit(1)

#verbose=options["verbose"]

if not options["cname"]:
    # create random client id if one is not given
    r=random.randrange(1,10000)
    cname="logger-"+str(r)
else:
    cname="logger-"+str(options["cname"])
    
print("logging level ",options["loglevel"])
logging.basicConfig(level=options["loglevel"])
log_dir=options["log_dir"]

logging.info("creating client name:"+cname)
client=initialise_clients(cname,mqttclient_log,cleansession=False,
username=options['username'],
password=options['password'],
broker=options['broker'],
port=options['port'],
topics=options['topics']) #create and initialise client object

#client.last_message=dict()
client.q=msg_queue #make queue available as part of client

# TODO: extra config
# if options["JSON"]: #
#     csv_flag=False
# if options["csv"]:
#     csv_flag=True
#     options["JSON"]=False
#     print("Logging csv format")
# if options["JSON"]:
#     print("Logging JSON format")
# if options["storechangesonly"]:
#     print("starting storing only changed data")
# else:
#     print("starting storing all data")
    
##
print("Log Directory =",log_dir)
#TODO: enable writing with schema file
# if options["header_flag"]: #

#     file_name=options["fname"]
#     headers={}
#     headers=getheader(file_name)
#     log.set_headers(headers)
#     print("getting headers from ",file_name)

# Enable thread for logging messages
Log_worker_flag=True
t = threading.Thread(target=log_worker) #start logger
t.start() #start logging thread
###
logging.debug(client.broker + " / " + str(client.port))
try:
    res=client.connect(client.broker,client.port)      #connect to broker
    logging.info("connecting to broker " + client.broker)
    client.loop_start() #start loop

except:
    logging.warning("connection failed")
    client.bad_count +=1
    client.bad_connection_flag=True #old clients use this
#loop and wait until interrupted   
try:
    while True:
        time.sleep(1)
        pass

except KeyboardInterrupt:
    print("interrrupted by keyboard")

client.loop_stop() #start loop
Log_worker_flag=False #stop logging thread
time.sleep(3)

