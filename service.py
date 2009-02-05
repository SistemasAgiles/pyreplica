#!/usr/bin/env python

# win32 basic sample (TODO: make a real win32 service)

# configure these paths:
LOGFILE = None # stdout o 'C:/pyreplica/pyreplica.log'
PIDFILE = 'C:/pyreplica/pyreplica.pid'
CONFPATH = 'C:/pyreplica'
DEBUG_LEVEL = 2	#  default debug level: 1: normal, 2: verbose

import sys, os

from daemon import Log, Replicator
        
if __name__ == "__main__":
    #redirect outputs to a logfile
    if LOGFILE:
        sys.stdout = sys.stderr = Log(open(LOGFILE, 'a+'))

    # start replication threads
    config_files = [f for f in os.listdir(CONFPATH) if f.endswith(".conf")]
    threads = []

    for config_file in config_files:
        thread = Replicator(os.path.join(CONFPATH,config_file))
        threads.append(thread)
        thread.start()
   
    for thread in threads:
        # wait until it terminates
        thread.join()
    print "Threads killed ok"
