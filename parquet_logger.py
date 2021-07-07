import os, json, logging
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

class Parquet_logger():

    def __init__(self,log_dir='ologs',MAX_LOG_SIZE=1000,TIME_INTERVAL=10):
        self.MAX_LOG_SIZE = MAX_LOG_SIZE
        self.log_root_dir = log_dir
        self.topics={} # key = topic name; values=[dir,current filename,count,writer,fo]
        
        self.create_log_dir(self.log_root_dir) # create the base dir for the root topic


    def __flushlogs(self,fo):
        """ release buffer to disk """
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
            fo=self.topics[key][4]
            if not fo.closed:
                pwriter.close()
                fo.close()
            logging.debug("parquet size: " + str(os.path.getsize(self.topics[key][1]) ))

    def write(self, data, writer, fo):
        """ simply write any data to file - fo with writer if any """
        try:
            writer.write_table(data) # write to txt file
        except BaseException as e:
            logging.error("Error on data: %s" % str(e))
            return False

        self.__flushlogs(fo)

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
        logging.info("creating log file: " + filename)

         # close previous log file
        if count==0:
            pass
        else:
            fo.close()
        
        fo=open(filename, 'wb')
        count+=1
         # TODO: parquet writer here with column params is the schema read somewhere e.g. csv
        writer = pq.ParquetWriter(fo,schema)
        
        self.topics[topic]=[dir,filename,count,writer,fo]
        return (fo, writer)

    def generate_file_name(self,dir):
        return dir + "/" + "logs.parquet"

    def log_data(self,data,topic=""):
        """
        write data to parquet using pyarrow, 
        so 
        1. we need to convert data to dataframe and then table and 
        2. then we can write all in one
        data: array of dict e.g. [message1,message2]
        topic: mqtt topic will be the directory of the message
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

    def log_message(self,data,topic=""):
        """
        Write a MQTT message to file
        data: [message]
        topic: MQTT topic
        """
        # Preprocess message before actually write it to file
        msg_df = pd.DataFrame(data=data)
        msg_table = pa.Table.from_pandas(msg_df,preserve_index=False)# convert dict to json
        schema =msg_table.schema

        dir=self.log_root_dir

        # Make sure file and directory is ready to be written
        if not topic in self.topics:
            s_topics=topic.split('/')
            for t in s_topics:
                # recursively create dir
                dir+="/"+t
                self.create_log_dir(dir)
            self.create_log_file(dir,topic,schema=schema,fo="",count=0)
        else:
            dir=self.log_root_dir + "/"+ topic
            fo=self.topics[topic][4]
            #logging.debug("check against max size: " + str(os.stat(file).st_size))
            if fo.closed: # exceed max log file size
                count=self.topics[topic][2]
                writer=self.create_log_file(dir,topic,schema=schema,fo=fo,count=count)

        # Actually write to file
        writer=self.topics[topic][3] #retrieve pointer
        fo=self.topics[topic][4]
        self.write(data=msg_table,writer=writer,fo=fo)