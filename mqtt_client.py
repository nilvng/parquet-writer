import paho.mqtt.client as mqtt
import json
import os
import time
import sys, getopt,random
import logging
import collections

import command

class MQTTClient(mqtt.Client):#extend the paho client class
   run_flag=False #global flag used in multi loop
   def __init__(self,cname,**kwargs):
      super(MQTTClient, self).__init__(cname,**kwargs)
      self.topic_ack=[] #used to track subscribed topics
      self.subscribe_flag=False
      self.bad_connection_flag=False
      self.bad_count=0
      self.count=0
      self.connected_flag=False
      self.connect_flag=False #used in multi loop
      self.sub_topic=""
      self.sub_topics=[] #multiple topics
      self.sub_qos=0
      self.broker=""
      self.port=1883
      self.keepalive=60
      self.cname=""
      self.delay=10 #retry interval

def initialise_clients(cname,mqttclient_log=False,cleansession=True,flags=""):
    #flags set
   logging.debug("initialising clients")
   client= MQTTClient(cname,clean_session=cleansession)
   client.cname=cname
   client.on_connect= on_connect        #attach function to callback
   client.on_message=on_message        #attach function to callback
#    if mqttclient_log:
#       client.on_log=on_log
   return client

def initialise_clients(cname,mqttclient_log=False,cleansession=True,flags="",\
                        username="",password="",broker="",port="",topics=[("",0)]):
    logging.debug("initialising clients")
    client= MQTTClient(cname,clean_session=cleansession)
    client.cname=cname
    client.on_connect= on_connect        #attach function to callback
    client.on_message=on_message
    
    if username!="":
       client.username_pw_set(username, password)
    client.sub_topics=topics
    client.broker=broker
    client.port=port
    return client

def on_connect(client, userdata, flags, rc):
   """
   set the bad connection flag for rc >0, Sets connected_flag if connected ok
   also subscribes to topics
   """
   logging.debug("Connected flags"+str(flags)+"result code "\
    +str(rc)+"client1_id")

   if rc==0:
      client.connected_flag=True #old clients use this
      client.bad_connection_flag=False
      if client.sub_topic!="": #single topic
          logging.info("subscribing "+str(client.sub_topic))
          topic=client.sub_topic
          if client.sub_qos!=0:
              qos=client.sub_qos
              client.subscribe(topic,qos)
      elif client.sub_topics!="":
        client.subscribe(client.sub_topics)
        #logging.info("Connected and subscribed to "+ " ".join(client.sub_topics))

   else:
     client.bad_connection_flag=True #
     client.bad_count +=1
     client.connected_flag=False #
def on_message(client,userdata, msg):
    topic=msg.topic
    m_decode=str(msg.payload.decode("utf-8","ignore"))
    message_handler(client,m_decode,topic)
    logging.debug("message received "+ m_decode)
    
def message_handler(client,msg,topic):
    """ 
        1/ append the message to dict to have metadata:time and topic
        2/ try to convert to json so that we can extract its field to the new dict
        2.b/ if it is not json then append it to key "message"
     """
    data=collections.OrderedDict()
    
    # Generate metadata for the message
    tnow=time.time()
    #data["time_ms"]=int(tnow*1000)
    #data["time"]=currentTime()
    data["topic"]=topic

    # If msg is a usual json => each of its keys is column
    try:
        msg_json=json.loads(msg)# convert to Dict before saving
    except:
        msg_json=msg
        logging.warning("message is not json")

    logging.info("queuing message..")
    if isinstance(msg_json,dict): # json.loads also parse int
        keys=msg_json.keys()
        for key in keys:
            data[key]=msg_json[key]
    else:  
        if isinstance(msg_json,list):
            if (isinstance(msg_json[0],dict)):
                for e in msg_json:
                    e["topic"]=topic
                    save_message(client,e)
                    return

        else:
            data["message"]=msg
    save_message(client,data)
    #TODO: enable store changes only

    # if command.options["storechangesonly"]:
    #     if has_changed(client,topic,msg):
    #         client.q.put(data) #put messages on queue
    # else:
    #     client.q.put(data) #put messages on queue
def save_message(client,data):
    str_data = json.dumps(data)
    keyname = command.options["message_queue_name"]
    client.redis_conn.lpush(keyname,str_data)
    logging.debug(f"done {str_data} saved to Redis")
    
def has_changed(client,topic,msg):
    #print("has changed ",options["testmode"])
    if topic in client.last_message:
        if client.last_message[topic]==msg:
            return False
    client.last_message[topic]=msg
    return True

def currentTime():
    s=time.localtime(tnow)

    year=str(s[0])
    month=s[1]
    if month <10:
        month="0"+str(month)
    day =s[2]
    if day<10:
        day="0"+str(day)
    hours=s[3]
    if hours<10:
        hours="0"+str(hours)
    m=s[4]
    if m<10:
        m="0"+str(m)
    sec=s[5]
    if sec<10:
        sec="0"+str(sec)

    ltime =str(year) + "-" + str(month) + "-" + str(day) + "_" + str(hours)
    ltime=ltime + ":" + str(m) + ":" + str(sec)
###