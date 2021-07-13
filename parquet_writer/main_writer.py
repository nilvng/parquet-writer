Log_worker_flag=True
import sys,logging,time,json
import redis

from command import command_input
import command
import parquet_logger

options=command.options

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

# === MAIN PROGRAM ===
if __name__ == "__main__":
    options=command_input(options)

log_dir=options["log_dir"]
redis_conn = redis.Redis()
logger=parquet_logger.Parquet_logger(log_dir=log_dir)

job_parquetWriter(logger=logger,conn=redis_conn,keyname=options["redis_key"])