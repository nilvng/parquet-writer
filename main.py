#!python3

mqttclient_log=False #MQTT client logs showing messages
Log_worker_flag=True

import paho.mqtt.client as mqtt
import threading

import redis
from collections import deque
import schedule

from command import command_input
import command
from mqtt_client import *
import parquet_logger

msg_queue = deque() # cache all messages received from the broker

redis_conn = redis.Redis()

options=command.options
def multi_lpop(conn, keyname,count):
  p = conn.pipeline()
  p.multi()
  p.lrange(keyname, 0, count - 1)
  p.ltrim(keyname, count, -1)
  return p.execute()

def job_parquetWriter(log,conn,keyname):
    # measure the amount of message we gonna write, which is the current size of queue
    pop_at = conn.llen(keyname)

    logging.info("I'm working on queue length: " + str(pop_at))
    
    if pop_at <= 0:
        return

    msgs = multi_lpop(conn,keyname,pop_at)[0]
    # pop and write each message to its corresponding file
    for m in msgs:
        jdata = json.loads(m)
        topic=jdata["topic"]
        del jdata["topic"]

        data = [jdata]
        if data is None:
            continue

        log.log_message(data,topic=topic)
    
    log.close_file()

def log_worker():
    """runs in own thread to log data from queue"""
    while Log_worker_flag:
        time.sleep(1)
        schedule.run_pending()

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
client.redis_conn=redis_conn #make queue available as part of client

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
print("Log Interval =",options["interval"])

#TODO: enable writing with schema file
# if options["header_flag"]: #

#     file_name=options["fname"]
#     headers={}
#     headers=getheader(file_name)
#     log.set_headers(headers)
#     print("getting headers from ",file_name)

# Enable thread for logging messages

###
try:
    res=client.connect(client.broker,client.port)      #connect to broker
    logging.info("connecting to broker " + client.broker)
    client.loop_start() #start loop

except:
    logging.warning("connection failed")
    client.bad_count +=1
    client.bad_connection_flag=True #old clients use this
#loop and wait until interrupted 

logger=parquet_logger.Parquet_logger(log_dir=log_dir,MAX_LOG_SIZE=options["log_max_size"])
Log_worker_flag=True
t = threading.Thread(target=log_worker) #start logger
t.start() #start logging thread
schedule.every(options["interval"]).seconds.do(job_parquetWriter,log=logger,conn=redis_conn,keyname=options["message_queue_name"])

try:
    while True:
        time.sleep(1)
        pass

except KeyboardInterrupt:
    print("interrrupted by keyboard")

client.loop_stop() #start loop
Log_worker_flag=False #stop logging thread
logger.close_file()
time.sleep(3)

