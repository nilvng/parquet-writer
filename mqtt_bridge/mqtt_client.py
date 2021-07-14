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
        self.connected_flag=False
        self.bad_connection_flag=False
        self.bad_count=0
        self.count=0
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
    +str(rc))

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
        else:
            logging.warning("Mising subscribed topics!")


   else:
        print("Bad connection Returned code=",rc)
        client.bad_connection_flag=True #
        client.bad_count +=1
        client.connected_flag=False #

def on_message(client,userdata, msg):
    topic=msg.topic
    
    m_decode=str(msg.payload.decode("utf-8","ignore"))

    if not m_decode:
        return

    logging.debug("message received "+ m_decode)
    
    message_handler(client,m_decode,topic)
    
def message_handler(client,msg,topic):
    """ 
        1/ append the message to dict to have metadata:time and topic
        2/ try to convert to json so that we can extract its field to the new dict
        2.b/ if it is not json then append it to key "message"
     """
    
    # Generate metadata for the message
    tnow=time.time()

    # If msg is a usual json => each of its keys is column
    try:
        msg_json=json.loads(msg,parse_int=float)# convert to Dict before saving
    except json.JSONDecodeError:
        msg_json=msg
        logging.warning("message is not json")

    logging.info("queuing message..")

    if isinstance(msg_json,dict): # json.loads also parse int
        data = compose_data(mess_dict=msg_json,topic=topic)
        save_message(client,data)
    else:  
        if isinstance(msg_json,list):
            if (isinstance(msg_json[0],dict)):
                for e in msg_json:
                    data = compose_data(mess_dict=e,topic=topic)
                    save_message(client,data)
                return

        else:
            mess_dict = {"data": msg_json}
            data = compose_data(mess_dict,topic=topic)
            save_message(client,data)
    
    logging.warning("Cannot save this msg: " + msg)

def compose_data(mess_dict,topic):
    #raw_data=collections.OrderedDict()
    raw_data={}
    raw_data["topic"]=topic

    raw_data.update(mess_dict)
    return {k:v for k,v in sorted(raw_data.items())}

def save_message(client,data):
    # convert nested type within data to string
    for key in data.keys():
            if (isinstance(data[key],(dict, list) )):
                data[key] = str(data[key])
    # convert data to string to save to redis
    str_data = json.dumps(data)

    keyname = command.options["message_queue_name"]
    client.redis_conn.lpush(keyname,str_data)
    logging.debug(f"done {str_data} saved to Redis")
    