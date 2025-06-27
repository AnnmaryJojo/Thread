# Integrated Code

import csv
import threading
from queue import Queue
import os
import math
import time
import logging
import sys
import couchbase
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
import couchbase.subdocument as SD
import configparser as cp
from datetime import datetime

# ---------------------------------------------------
# Configuration and Logging Setup
# ---------------------------------------------------
currTime = datetime.today().strftime("%Y%m%d%H%M%S")
config = cp.ConfigParser()
config.read('DBConfig.txt')

couchbaseIP = config.get('Couchbase', 'couchbaseIP')
couchbaseUserName = config.get('Couchbase', 'couchbaseUserName')
couchbasePassword = config.get('Couchbase', 'couchbasePassword')
couchbaseBucketName = config.get('Couchbase', 'couchbaseBucketName')
NUMBER_OF_THREADS = int(config.get('Other', 'numberOfThreads'))
#pageSize = int(config.get('Other', 'pageSize'))
couchbaseDBConnString = 'couchbase://' + couchbaseIP

LOG_DIR = '/tmp/'
scriptName = "couchbase_script"
logFileName = os.path.join(LOG_DIR, f"{scriptName}.{currTime}.log")
logging.basicConfig(filename=logFileName, level=logging.INFO)

# ----------------------------------------------------
# Couchbase Deletion Logic per IMSI
# ----------------------------------------------------
def userDump(imsi, thread_id):
    try:
        logging.info(f"{datetime.now()}:: Starting thread {thread_id} for IMSI: {imsi}")
        cluster = Cluster.connect(couchbaseDBConnString, ClusterOptions(PasswordAuthenticator(couchbaseUserName, couchbasePassword)))
        bucket = cluster.bucket(couchbaseBucketName)
        collection = bucket.default_collection()

        current_epoch = int(time.time())
        cutoff_epoch = current_epoch - 7200
        main_doc_key = f'POLICYDATASUBSIDS_imsi-{imsi}'

        try:
            doc_content = collection.get(main_doc_key).value
            past_subdoc_keylist = []
            for item, doc_sub_content in doc_content.items():
                try:
                    TTL = int(doc_sub_content['expiryTTL'])
                    if TTL < cutoff_epoch:
                        past_subdoc_keylist.append(item)
                        if len(past_subdoc_keylist) == 16:
                            collection.mutate_in(main_doc_key, [SD.remove(path) for path in past_subdoc_keylist])   # removes expired subdocs using mutate_in
                            past_subdoc_keylist = []
                except Exception as e:
                    logging.warning(f"{datetime.now()}:: Subdoc error in {main_doc_key}: {e}")
            if past_subdoc_keylist:
                collection.mutate_in(main_doc_key, [SD.remove(path) for path in past_subdoc_keylist])
        except Exception as e:
            logging.error(f"{datetime.now()}:: Error processing {main_doc_key}: {e}")

        logging.info(f"{datetime.now()}:: Finished thread {thread_id} for IMSI: {imsi}")
    except Exception as e:
        logging.error(f"{datetime.now()}:: Couchbase connection error: {e}")
        print(f"Couchbase Connection failed: {e}")
    return 'DeleteDone'

# ---------------------------------------------------
# Multithreaded CSV Processor
# ---------------------------------------------------
def threaded_csv_processor(csv_path, thread_count=None, chunk_size=None):
    def estimate_lines(filepath):
        with open(filepath, 'r') as f:
            return sum(1 for _ in f) - 1  # Reads file line-by-line and returns number of data rows (excluding header).

    def producer(csv_file, queue, chunk_size):
        with open(csv_file, newline='') as f:
            reader = csv.DictReader(f)
            chunk = []
            for row in reader:
                try:
                    imsi = int(row['imsi'])
                    chunk.append(imsi)
                    if len(chunk) >= chunk_size:
                        queue.put(chunk)
                        chunk = []
                except:
                    continue
            if chunk:
                queue.put(chunk)
        for _ in range(actual_thread_count):
            queue.put(None)

    def consumer(queue, thread_id):
        while True:
            chunk = queue.get()
            if chunk is None:
                break
            for imsi in chunk:
                userDump(imsi, thread_id)

    total_lines = estimate_lines(csv_path)
    estimated_threads = min(32, max(4, total_lines // 2000))
    estimated_chunk = max(50, total_lines // (estimated_threads * 5))
    global actual_thread_count
    actual_thread_count = thread_count or NUMBER_OF_THREADS or estimated_threads
    actual_chunk_size = chunk_size or estimated_chunk

    print(f"\nStarting processing with {actual_thread_count} threads and chunk size {actual_chunk_size}")

    queue = Queue(maxsize=actual_thread_count * 4)
    threads = []
    for i in range(actual_thread_count):
        t = threading.Thread(target=consumer, args=(queue, i))
        threads.append(t)
        t.start()

    producer(csv_path, queue, actual_chunk_size)

    for t in threads:
        t.join()

    print("✅ Processing Completed.")

# ---------------------------------------------------
# Main Entry Point
# ---------------------------------------------------
if __name__ == "__main__":
    try:
        import argparse
        parser = argparse.ArgumentParser(description="Multithreaded IMSI Couchbase Subdoc Remover")
        parser.add_argument("csv_path", help="Path to CSV with 'imsi' column")
        parser.add_argument("--threads", type=int, help="Number of threads to use")
        parser.add_argument("--chunk", type=int, help="Chunk size per thread")
        args = parser.parse_args()

        logging.info("Script execution started.")
        threaded_csv_processor(args.csv_path, args.threads, args.chunk)
        logging.info("Script execution completed successfully.")

        print(f"Please check logs at {logFileName}")
    except Exception as e:
        logging.error(f"Fatal Error: {e}")
        print(f"An error occurred. Check log at {logFileName}")
