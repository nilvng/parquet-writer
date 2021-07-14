Log_worker_flag=True
import sys,logging,time,json
import redis

from command import command_input
import command
import parquet_logger

#start_time = time.time()
options=command.options

def multi_lpop(conn, keyname,count):
    p = conn.pipeline()
    p.multi()
    p.lrange(keyname, 0, count - 1)
    p.ltrim(keyname, count, -1)
    return p.execute()

def get_message_size(conn,keyname):
    """
    Get the amount of messages the program will write
    The amount should be sufficient to write in a given interval
    """
    pop_at = conn.llen(keyname)
    max_allowed_interval = options["interval_length"] / 1.5 * 400.0
    
    if pop_at > max_allowed_interval:
        return max_allowed_interval
    
    return pop_at

def job_parquetWriter(logger,conn,keyname):
    # measure the amount of message we gonna write, which is the current size of queue
    pop_at = get_message_size(conn,keyname)
    logging.info("I'm working on queue length: " + str(pop_at))
    
    if pop_at <= 0:
        logging.info("message queue is empty")
        return

    msgs = multi_lpop(conn,keyname,pop_at)[0]
    if not msgs:
        return
    for m in msgs:     # pop and write each message to its corresponding file
        logger.log_message(mdata=m)
    
    logger.close_file()

# === MAIN PROGRAM ===
if __name__ == "__main__":
    options=command_input(options)

log_dir=options["log_dir"]
redis_conn = redis.Redis()
logger=parquet_logger.Parquet_logger(log_dir=log_dir)

try:
    job_parquetWriter(logger=logger,conn=redis_conn,keyname=options["redis_key"])
except:
    #TODO: release stucked message to redis
    
#print(time.time() - start_time)