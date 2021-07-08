Log_worker_flag=True

import time

import redis

from command import command_input
import command
import parquet_logger

options=command.options

log_dir=options["log_dir"]
print("Log Directory =",log_dir)

def multi_lpop(conn, keyname,count):
  p = conn.pipeline()
  p.multi()
  p.lrange(keyname, 0, count - 1)
  p.ltrim(keyname, count, -1)
  return p.execute()

def job_parquetWriter(logger,conn,keyname):
    # measure the amount of message we gonna write, which is the current size of queue
    pop_at = conn.llen(keyname)

    logging.info("I'm working on queue length: " + str(pop_at))
    
    if pop_at <= 0:
        logging.info("message queue is empty")
        return

    msgs = multi_lpop(conn,keyname,pop_at)[0]
    if not msgs:
        return
    # pop and write each message to its corresponding file
    for m in msgs:
        jdata = json.loads(m)
        topic=jdata["topic"]
        del jdata["topic"]

        data = [jdata]
        if data is None:
            continue

        logger.log_message(data,topic=topic)
    
    logger.close_file()

def log_worker():
    """runs in own thread to log data from queue"""
    while Log_worker_flag:
        time.sleep(1)
        schedule.run_pending()

# === MAIN PROGRAM ===
if __name__ == "__main__" and len(sys.argv)>=2:
    options=command_input(options)
else:
    print("Need broker name and topics to continue.. exiting")
    raise SystemExit(1)

logger=parquet_logger.Parquet_logger(log_dir=log_dir,MAX_LOG_SIZE=options["log_max_size"])

Log_worker_flag=True
t = threading.Thread(target=log_worker) #start logger
t.start() #start logging thread
schedule.every(options["interval"]).seconds.do(job_parquetWriter,log=logger,conn=redis_conn,keyname=options["message_queue_name"])

# the end
Log_worker_flag=False #stop logging thread
time.sleep(3)
