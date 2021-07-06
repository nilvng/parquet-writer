import os, json, logging
class Parquet_logger():

    def __init__(self,log_dir='ologs',MAX_LOG_SIZE=1000,TIME_INTERVAL=10):
        self.MAX_LOG_SIZE = MAX_LOG_SIZE
        self.TIME_INTERVAL = TIME_INTERVAL
        self.log_root_dir = log_dir
        self.topics={} # key = topic name
        
        self.create_log_dir(self.log_root_dir)
        logging.info("Log interval= " + str(TIME_INTERVAL))

    def __flushlogs(self,fo):
        """ confirm writing buffer to disk """
        fo.flush()
        os.fsync(fo.fileno())
    
    def create_log_dir(self, log_dir):
        try:
            os.stat(log_dir) # get status
        except:
            os.mkdir(log_dir) # if cannot get, create new dir

    def close_file(self):
        """close all files of every topics after writing to each?"""
        for key in self.topics:
            fo=self.topics[key][0]
            if not fo.closed:
                fo.close()

    def write(self, fo, data, writer):
        """ simply write any data to file - fo with writer if any """
        try:
            fo.write(data) # write to txt file
        except BaseException as e:
            logging.error("Error on data: %s" % str(e))
            return False

        self.__flushlogs(fo)

    def create_log_file(self,dir,topic,columns,fo="",count=0):
        """ create log file with unique filename, and at specific dir"""
        log_numbr="{0:003d}".format(count)
        #TODO: change to parquet
        #filename = "log"+""+".parquet"
        filename = "log"+str(log_numbr)+".txt"
        # remove outdated file with that filename
        try:
            os.stat(filename)
            os.remove(filename)
        except:
            pass
        filename=dir+"/"+filename
        logging.info("creating log file")

         # close previous log file
        if count==0:
            pass
        else:
            fo.close()
        
        fo=open(filename,"w")
        count+=1
         # TODO: parquet writer here with column params is the schema read somewhere e.g. csv
        writer = None
        self.topics[topic]=[fo,dir,filename,count,writer]

        return (fo,writer)

    def log_data(self,data,topic=""):
        columns=0 #needed as json data causes error
        if topic=="":
            topic=data["topic"]
            del data["topic"]
        
        jdata=json.dumps(data)+"\n" # convert dict to json

        if topic in self.topics:
            fo=self.topics[topic][0] #retrieve pointer
            writer=self.topics[topic][4] #retrieve pointer
            # TODO: validate with file threshold (time_interval, or file size)
            # Create new log file for the next log 
            file=self.topics[topic][2]
            if os.stat(file).st_size>self.MAX_LOG_SIZE:
                dir=self.topics[topic][1]
                count=self.topics[topic][3]
                fo,writer=self.create_log_file(dir,topic,columns,fo,count)
                self.topics[topic][0]=fo
                self.topics[topic][4]=writer
                
            self.write(fo,jdata,writer)
        else:
            # new topic!
            s_topics=topic.split('/')
            dir=self.log_root_dir
            for t in s_topics:
                # recursively create dir
                dir+="/"+t
                self.create_log_dir(dir)
            #create log file before actually writing into it
            self.create_log_file(dir,topic,columns,fo="",count=0)

            fo=self.topics[topic][0] #retrieve pointer
            writer=self.topics[topic][4] #retrieve pointer
            self.write(fo,jdata,writer)