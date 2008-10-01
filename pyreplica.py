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
USERNAME = 'replica'

# don't modify anything below tis line (except for experimenting)

import sys
import psycopg2,psycopg2.extensions
import tpc
import select
import time
import os
import md5

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
    con0 = cur0.connection
    con1 = cur1.connection
    
    # create a unique TPC transaction id
    tpc_xid = con0.xid(0, 'pyreplica %s' % md5.md5(con0.dsn).hexdigest(),'')

    # test if there is a prepared transaction in origin or replica
    tpc_xid0_pepared = tpc_xid in con0.tpc_recover()
    tpc_xid1_pepared = tpc_xid in con1.tpc_recover()
    if tpc_xid0_pepared and tpc_xid1_pepared:
        # commit both
        print "commit both"
        con0.tpc_commit(tpc_xid)
        con1.tpc_commit(tpc_xid)
        print "commit con0 and con1"
    elif tpc_xid0_pepared:
        # rollback origin (replica prepare failed)
        con0.tpc_rollback(tpc_xid)
        print "rollback con0"
    elif tpc_xid1_pepared: 
        # commit replica (origin commit was successful)
        con1.tpc_commit(tpc_xid)
        print "commit con1"

    # begin TPC transactions on both databases
    con0.tpc_begin(tpc_xid)
    con1.tpc_begin(tpc_xid)
    try:
        # Query un-replicated data (lock rows to prevent data loss)
        cur0.execute("SELECT id,sql FROM replica_log "
                     "WHERE NOT replicated AND username<>%s " 
                     "ORDER BY id ASC FOR UPDATE" ,(USERNAME,))
        for row in cur0:
            # Execute replica queries
            debug("Executing: %s" % row[1], level=2)
            cur1.execute(row[1])
        if cur0.rowcount:
            # mark replicated data
            cur0.execute("UPDATE replica_log SET replicated=TRUE WHERE NOT replicated")
            # prepare first phase of TPC transaction
            con0.tpc_prepare()
            con1.tpc_prepare()
            # commit second phase of TPC transaction
            con0.tpc_commit()
            con1.tpc_commit()
    except Exception, e:
        # something failed, try to resolve next time
        # (the connections may be in a unrecoverable status or disconected)
        raise

debug("DSN0: %s" % DSN0, level=3)
debug("DSN1: %s" % DSN1, level=3)

# create two-phase-commit connections:
debug("Opening origin (master) connection")
con0 = psycopg2.connect(DSN0,connection_factory=tpc.TwoPhaseCommitConnection)
debug("Opening replica (slave) connection")
con1 = psycopg2.connect(DSN1,connection_factory=tpc.TwoPhaseCommitConnection)
debug("Encoding for this connections are %s %s" % 
  (con0.encoding,con1.encoding), level=2)

cur0 = con0.cursor()
cur1 = con1.cursor()

# process previous logs:
replicate(cur0,cur1)

# main loop:
try:
    # set isolation level for LISTEN 
    con0.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur0.execute('LISTEN "replicas"')
    # set isolation level to prevent read problems
    con0.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
    con1.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
    while 1:
        debug("Waiting for 'NOTIFY'",level=3)
        if select.select([cur0],[],[], TIMEOUT)==([],[],[]):
            debug("Timeout(''keepalive'')!",level=3)
        else:
            if cur0.isready():
                debug("Got NOTIFY: %s" % str(cur0.connection.notifies.pop()),level=3)
                replicate(cur0,cur1)
        
        con0.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur0.execute('SELECT now()')
        debug("Keepalive0: %s" % cur0.fetchone()[0])
        con0.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        cur1.execute('SELECT now()')
        debug("Keepalive1: %s" % cur1.fetchone()[0])
except Exception, e:
    debug("FATAL ERROR: %s" % str(e))
    raise
