import os, json, logging
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

from datetime import datetime
class Parquet_logger():

    def __init__(self, log_dir='ologs', MAX_LOG_SIZE=1000):
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
            pwriter=self.topics[key][2]
            fo=self.topics[key][3]
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

    def create_log_file(self,dir,topic,schema,fo=""):
        """ create log file with unique filename, and at specific dir"""
        tstmp = datetime.now().strftime(r"%Y%m%d_%H%M%S")
        #TODO: change to parquet
        #filename = "log"+""+".parquet"
        filename = "log"+str(tstmp)+".parquet"
        # remove outdated file with that filename
        try:
            os.stat(filename)
            os.remove(filename)
        except:
            pass
        filename=dir+"/"+filename
        logging.info("creating log file: " + filename)

         # close previous log file
        if fo != "" and not fo.closed:
            fo.close()
        
        fo=open(filename, 'wb')
         # parquet writer here with schema
        writer = pq.ParquetWriter(fo,schema)
        
        self.topics[topic]=[dir,filename,writer,fo]
        return (fo, writer)

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

    def log_message(self,mdata,topic=""):
        """
        Write a MQTT message to file
        data: message
        topic: MQTT topic
        """
        # Preprocess message before actually write it to file
        jdata = json.loads(mdata,parse_int=float)
        if not topic:
            topic=jdata["topic"]
            try:
                del jdata["topic"]
            except KeyError e:
                logging.error(f"Cannot find topic in received message: {mdata}")

        data = [jdata]
        msg_df = pd.DataFrame(data=data)
        
        msg_df = msg_df.infer_objects()
        msg_table = pa.Table.from_pandas(msg_df,preserve_index=False).replace_schema_metadata()# convert dict to json
        schema =msg_table.schema.remove_metadata()

        dir=self.log_root_dir

        # Make sure file and directory is ready to be written
        if not topic in self.topics:
            s_topics=topic.split('/')
            for t in s_topics:
                # recursively create dir
                dir+="/"+t
                self.create_log_dir(dir)
            self.create_log_file(dir,topic,schema=schema)
        else:
            dir=self.log_root_dir + "/"+ topic
            fo=self.topics[topic][3]
            # new interval or not
            if fo.closed: 
                fo, writer = self.create_log_file(dir,topic,schema=schema,fo=fo)

        # Actually write to file
        writer=self.topics[topic][2] #retrieve pointer
        fo=self.topics[topic][3]
        self.write(data=msg_table,writer=writer,fo=fo)