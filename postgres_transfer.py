# # Add data to the database from pandas

from sqlalchemy import create_engine
from datetime import datetime
import logging
import pandas as pd
import json
import shutil
import os
import sys
import time
from subprocess import check_output, Popen
import signal

# create logger with 'spam_application'
logger = logging.getLogger('hubitat_transfer_logger')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('transfer.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

def reset_node_red(pid_val):
    """
    Sends SIGTERM to the pid value if it exists. Returns new pid value
    """
    if pid_val:
        logger.info("Killing old node-red preocess")
        os.kill(pid_val, signal.SIGTERM)
        time.sleep(5)
    logger.info("Starting new node-red process")
    resp = Popen('node-red')
    return resp.pid

nr_PID = None
try:
    pid_val = check_output(["pidof",name]).decode('utf-8')
except Exception as e:
    print("Could not find pid value for process name")
    pid_val = None

SLEEP_TIME = 15 # time in minutes
nr_PID = reset_node_red(nr_PID)

while True:

    postgresname = 'postgresql://postgres:mysecretpassword@localhost:5432/hubitatlogs'

    engine = create_engine(postgresname, echo=False)

    rootdir = '/home/tony/node-red-logs/'

    filename = rootdir + 'eventsocket.txt'
    if os.path.exists(filename):

        if os.path.exists(filename):
            with open(filename) as f:
                rawdata = f.read().splitlines()
        else:
            rawdata = []


        rawdata = [json.loads(i) for i in rawdata]
        df = pd.DataFrame(rawdata)
        df.index = pd.to_datetime(df.timestamp)
        df.drop('timestamp', axis=1, inplace=True)

        unique_name = df.name.unique()
        df.name.unique()


        float_dict = ['humidity', 'pressure', 'temperature']

        for nameval in unique_name:
            aa = df[df.name==nameval]
            if nameval in float_dict:
                aa.value = aa.value.astype(float)
            aa.to_sql(nameval, con=engine, if_exists='append')

        timenow = datetime.now()
        timenow = timenow.strftime('%Y%m%d_%H%M%S')

        backupfile = rootdir + 'backup/' + timenow + '.txt'

        shutil.move(filename, backupfile)
        logger.info(f"{len(df)} rows have been added to the database")

    else:
        logger.info(f"File note found - restarting node red program now")
        nr_PID = reset_node_red(nr_PID)

    time.sleep(SLEEP_TIME*60)

