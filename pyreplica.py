#! /usr/bin/python
# a simple postgresql replicator
#
# based on notify.py - psycopg2 example of getting notifies
#
# Copyright (C) 2007 Mariano Reingart
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTIBILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.

# configuration variables:

DSN0 = None 	# origin (master), may be passwd in argv or environ
DSN1 = None 	# replica (slave), may be passed in argv or environ
DEBUG = 2	# 1: normal, 2: verbose
TIMEOUT = 60 	# time (in seconds) between selects

# don't modify anything below tis line (except for experimenting)

import sys
import psycopg2
import select
import time
import os

# find connection strings
if len(sys.argv)==3:
    DSN0 = sys.argv[1] 	
    DSN1 = sys.argv[2] 	
else:
    DSN0 = os.environ.get("DSN0",DSN0)
    DSN1 = os.environ.get("DSN1",DSN1)

if not DSN0 or not DSN1:
    print "Usage: %s DSN0 DSN1" % sys.argv[0]
    print "or pass DSN0 and DSN1 via the enviroment, or configure this script"
    print "where DNS are valid postgresql connection strings:"
    print " * DSN0: origin. example: \"dbname=master user=postgres host=remotehost\""
    print " * DSN1: replica. example: \"dbname=replica user=postgres host=localhost\""
    sys.exit(1)    


def debug(message,level=1):
    "Print a debug message"
    if DEBUG>=level:
        print time.asctime(),message
        # flush buffers
        sys.stdout.flush()       

def replicate(cur0,cur1):
    "Process remote replication log (cur0: master, cur1: slave)"
    try:
        # First, obtain last replicated id ("revision")
        cur1.execute("SELECT last_value FROM replica_log_id_seq")
        id = cur1.fetchone()[0]
        # Query replica data from last id
        cur0.execute("SELECT id,sql FROM replica_log WHERE id>%s ORDER BY id ASC",(id,))
        for row in cur0:
            # Execute replica queries
            debug("Executing: %s" % row[1], level=2)
            cur1.execute(row[1])
            id = row[0]
            # set up last replicated id
            cur1.execute("SELECT setval('replica_log_id_seq'::regclass,%s)",(id,))
            cur1.connection.commit()
    except Exception, e:
        cur1.connection.rollback()
        raise

debug("DSN0: %s" % DSN0, level=3)
debug("DSN1: %s" % DSN1, level=3)

debug("Opening origin (master) connection")
con0 = psycopg2.connect(DSN0)
debug("Opening replica (slave) connection")
con1 = psycopg2.connect(DSN1)
debug("Encoding for this connections are %s %s" % 
  (con0.encoding,con1.encoding), level=2)

con0.set_isolation_level(0)
cur0 = con0.cursor()
cur1 = con1.cursor()

# process previous logs:
replicate(cur0,cur1)

# main loop:
try:
    cur0.execute('LISTEN "replicas"')
    while 1:
        debug("Waiting for 'NOTIFY'")
        if select.select([cur0],[],[], TIMEOUT)==([],[],[]):
            debug("Timeout(''keepalive'')!")
        else:
            if cur0.isready():
                debug("Got NOTIFY: %s" % str(cur0.connection.notifies.pop()))
                replicate(cur0,cur1)
except Exception, e:
    debug("FATAL ERROR: %s" % str(e))
    raise
