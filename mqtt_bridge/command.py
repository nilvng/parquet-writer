#!python3
###demo code provided by Steve Cope at www.steves-internet-guide.com
##email steve@steves-internet-guide.com
###Free to use for any purpose
import sys, getopt
options=dict()
#available logging Levels=["DEBUG","INFO","WARNING","ERROR","CRITICAL"]

##EDIT HERE ###############
options["username"]=""
options["password"]=""
options["broker"]="10.6.18.15"
options["port"]=1884
options["message_queue_name"]= "msg_queue"
options["cname"]=""
options["topics"]=[("",0)]
options["storechangesonly"]=True
options["keepalive"]=60
options["loglevel"]="INFO"
options["log_dir"]="mlogs"

def command_input(options={}):
    topics_in=[]
    qos_in=[]

    valid_options="-b <broker> -p <port>-t <topic> -q QOS -h <help>\
-n Client ID or Name -u Username -P Password -s <store all data>\
-l <log directory default= mlogs> -f <redis_key_name>\
-d <logging_level>"
    print_options_flag=False
    try:
      opts, args = getopt.getopt(sys.argv[1:],"b:sdk:p:t:q:l:n:u:P:l:f:r:")
    except getopt.GetoptError:
      print (sys.argv[0],valid_options)
      sys.exit(2)
    qos=0

    for opt, arg in opts:
        if opt == "-b":
             options["broker"] = str(arg)
        elif opt == "-k":
             options["keepalive"] = int(arg)
        elif opt =="-p":
            options["port"] = int(arg)
        elif opt =="-t":
            topics_in.append(arg)
        elif opt =="-q":
             qos_in.append(int(arg))
        elif opt =="-n":
             options["cname"]=arg
        elif opt =="-d":
            options["loglevel"]=str(arg).upper()
        elif opt == "-P":
             options["password"] = str(arg)
        elif opt == "-u":
             options["username"] = str(arg)
        elif opt =="-s":
            options["storechangesonly"]=False
        elif opt =="-l":
            options["log_dir"]=str(arg)
        elif opt == "-r":
            options["message_queue_name"]=str(arg)



    lqos=len(qos_in)
    for i in range(len(topics_in)):
        if lqos >i:
            topics_in[i]=(topics_in[i],int(qos_in[i]))
        else:
            topics_in[i]=(topics_in[i],0)

    if topics_in:
        options["topics"]=topics_in #array with qos
    return options
