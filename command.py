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
options["broker"]="127.0.0.1"
options["port"]=1883
options["message_queue_name"]= "msg_queue"
options["verbose"]=True
options["cname"]=""
options["topics"]=[("",0)]
options["storechangesonly"]=True
options["keepalive"]=60
options["loglevel"]="DEBUG"
options["log_dir"]="mlogs"
options["log_max_size"]=10000
options["interval"]=10
options["JSON"]=True
options["csv"]=False
options["header_file"]="data.csv"
options["fname"]="data.csv"
options["header_flag"]=False

def command_input(options={}):
    topics_in=[]
    qos_in=[]

    valid_options=" --help <help> -h or -b <broker> -p <port>-t <topic> -q QOS -v <verbose> -h <help>\
 -d logging debug  -n Client ID or Name -u Username -P Password -s <store all data>\
-l <log directory default= mlogs> -r <Record size bytes default=10000> -f <filename>\
-I <length of an interval, default=20 sec> -j <log in JSON format> -c <log in csvformat>"
    print_options_flag=False
    try:
      opts, args = getopt.getopt(sys.argv[1:],"h:b:jcsdk:p:t:q:l:I:vn:u:P:l:r:f:")
    except getopt.GetoptError:
      print (sys.argv[0],valid_options)
      sys.exit(2)
    qos=0

    for opt, arg in opts:
        if opt == '-h':
             options["broker"] = str(arg)
        elif opt == "-b":
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
        elif opt =="-f":
             options["fname"]=arg
             options["header_flag"]=True
        elif opt =="-d":
            options["loglevel"]="DEBUG"
        elif opt == "-P":
             options["password"] = str(arg)
        elif opt == "-u":
             options["username"] = str(arg)
        elif opt =="-v":
            options["verbose"]=True
        elif opt =="-s":
            options["storechangesonly"]=False
        elif opt =="-l":
            options["log_dir"]=str(arg)
        elif opt =="-r":
            options["log_max_size"]=int(arg)
        elif opt =="-I":
            options["interval"]=int(arg)
        elif opt =="-j":
            options["JSON"]=True
        elif opt =="-c":
            options["csv"]=True


    lqos=len(qos_in)
    for i in range(len(topics_in)):
        if lqos >i:
            topics_in[i]=(topics_in[i],int(qos_in[i]))
        else:
            topics_in[i]=(topics_in[i],0)

    if topics_in:
        options["topics"]=topics_in #array with qos
    return options
