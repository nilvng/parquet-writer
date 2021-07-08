#!python3

import sys, getopt
options=dict()

##EDIT HERE ###############
options["log_dir"]="mlogs"
options["log_max_size"]=10000
options["redis_key"]="ms_queue"

def command_input(options={}):
    topics_in=[]
    qos_in=[]

    valid_options="-l <log directory default= mlogs> \
-s <Record size bytes default=10000> \
-k <redis_key_name>"

    try:
      opts, args = getopt.getopt(sys.argv[1:],"l:k:s:")
    except getopt.GetoptError:
      print (sys.argv[0],valid_options)
      sys.exit(2)

    for opt, arg in opts:
        if opt == "-k":
            options["redis_key"]=str(arg)
        elif opt =="-l":
            options["log_dir"]=str(arg)
        elif opt == "s":
            options["log_max_size"]=int(arg) # TODO: not available

    return options
