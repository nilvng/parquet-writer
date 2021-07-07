import os, json, logging
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

class Parquet_logger():

    def __init__(self,log_dir='ologs',MAX_LOG_SIZE=1000,TIME_INTERVAL=10):
        self.MAX_LOG_SIZE = MAX_LOG_SIZE
        self.TIME_INTERVAL = TIME_INTERVAL
        self.log_root_dir = log_dir
        self.topics={} # key = topic name; values=[fo,dir,current filename,count,writer]
        
        self.create_log_dir(self.log_root_dir) # create the base dir for the root topic
        logging.info("Log size= " + str(MAX_LOG_SIZE))
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
            pwriter=self.topics[key][3]
            #if not pwriter.closed:
            pwriter.close()
            logging.debug("parquet size: " + str(os.path.getsize(self.topics[key][1]) ))

    def write(self, data, writer):
        """ simply write any data to file - fo with writer if any """
        try:
            writer.write_table(data) # write to txt file
        except BaseException as e:
            logging.error("Error on data: %s" % str(e))
            return False

        #self.__flushlogs(fo)

    def create_log_file(self,dir,topic,schema,fo="",count=0):
        """ create log file with unique filename, and at specific dir"""
        log_numbr="{0:003d}".format(count)
        #TODO: change to parquet
        #filename = "log"+""+".parquet"
        filename = "log"+str(log_numbr)+".parquet"
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
            pass
        
        count+=1
         # TODO: parquet writer here with column params is the schema read somewhere e.g. csv
        writer = pq.ParquetWriter(filename,schema)
        
        self.topics[topic]=[dir,filename,count,writer]
        return writer

    def generate_file_name(self,dir):
        return dir + "/" + "logs.parquet"

    def log_data(self,data,topic=""):
        """
        write data to parquet using pyarrow, so we need to convert data to dataframe and then table and then we can write all in one
        data: array of dict
        topic: 
        """
        dir=self.log_root_dir + "/"+ topic

        if not topic in self.topics:
            s_topics=topic.split('/')
            for t in s_topics:
                # recursively create dir
                dir+="/"+t
                self.create_log_dir(dir)
        else:
            pass
            
        msg_df = pd.DataFrame(data=data)
        msg_table = pa.Table.from_pandas(msg_df)
        outf = self.generate_file_name(dir=dir)
        pq.write_table(msg_table, outf)

    def generate_same(self,dir):
        return dir + "/" + "logs.parquet"

    def log_message(self,data,topic=""):
        #columns=0 #needed as json data causes error
        # if topic=="":
        #     topic=data["topic"]
        #     del data["topic"]
        
        msg_df = pd.DataFrame(data=data)
        msg_table = pa.Table.from_pandas(msg_df,preserve_index=False)# convert dict to json
        
        dir=self.log_root_dir

        if not topic in self.topics:
            s_topics=topic.split('/')
            for t in s_topics:
                # recursively create dir
                dir+="/"+t
                self.create_log_dir(dir)
            self.create_log_file(dir,topic,msg_table.schema,fo="",count=0)
        else:
            dir=self.log_root_dir + "/"+ topic
            file=self.topics[topic][2]
            # logging.debug("check against max size: " + str(os.path.getsize(self.topics[topic][1]) ))
            # if os.stat(file).st_size>self.MAX_LOG_SIZE: # exceed max log file size
            #     writer=self.topics[topic][3]
            #     writer.close()
            #     count=self.topics[topic][2]
            #     writer=self.create_log_file(dir,topic,columns,fo,count)
    
        writer=self.topics[topic][3] #retrieve pointer
        self.write(msg_table,writer)