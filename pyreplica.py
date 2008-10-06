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

def debug(message,level=1):
    "Print a debug message"
    if DEBUG>=level:
        print time.asctime(),message
        # flush buffers
        sys.stdout.flush()       

def replicate(cur0, cur1, skip_user, debug):
    "Process remote replication log (cur0: master cursor, cur1: slave cursor)"
    con0 = cur0.connection
    con1 = cur1.connection
    
    # create a unique TPC transaction id 
    # (they diffier because they can apply to the same backend)
    tpc_xid0 = con0.xid(0, 'pyreplica0 %s' % md5.md5(con0.dsn).hexdigest(),'')
    tpc_xid1 = con1.xid(0, 'pyreplica1 %s' % md5.md5(con0.dsn).hexdigest(),'')

    # test if there is a prepared transaction in mater or slave
    tpc_xid0_pepared = tpc_xid0 in con0.tpc_recover()
    tpc_xid1_pepared = tpc_xid1 in con1.tpc_recover()
    if tpc_xid0_pepared and tpc_xid1_pepared:
        # commit both
        con0.tpc_commit(tpc_xid0)
        con1.tpc_commit(tpc_xid1)
        debug("tpc_commit con0 and con1", level=3)
    elif tpc_xid0_pepared:
        # rollback origin (replica prepare failed)
        con0.tpc_rollback(tpc_xid0)
        debug("tpc_rollback con0", level=3)
    elif tpc_xid1_pepared: 
        # commit replica (origin commit was successful)
        con1.tpc_commit(tpc_xid1)
        debug("tpc_commit con1", level=3)

    # begin TPC transactions on both databases
    con0.tpc_begin(tpc_xid0)
    con1.tpc_begin(tpc_xid1)
    try:
        # ignore some user? (multimaster setup)
        sql = skip_user and " AND username<>%s " or ""
        args = skip_user and (skip_user,) or ()
        # Query un-replicated data (lock rows to prevent data loss)
        cur0.execute("SELECT id,sql FROM replica_log "
                     "WHERE NOT replicated %s " 
                     "ORDER BY id ASC FOR UPDATE" % sql ,args)
        for row in cur0:
            # Execute replica queries in slave
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
        else:
            # rollback prepared TPC transaction
            con0.tpc_rollback()
            con1.tpc_rollback()
    except Exception:
        # something failed, try to resolve next time
        # (the connections may be in a unrecoverable status or disconected)
        raise

def connect(dsn0, dsn1, debug):
    "Connect to databases (dsn0: master, dsn1: slave)"
    debug("DSN0: %s" % dsn0, level=3)
    debug("DSN1: %s" % dsn1, level=3)
    try:
        # create two-phase-commit connections:
        debug("Opening origin (master) connection")
        con0 = psycopg2.connect(dsn0,connection_factory=tpc.TwoPhaseCommitConnection)
        debug("Opening replica (slave) connection")
        con1 = psycopg2.connect(dsn1,connection_factory=tpc.TwoPhaseCommitConnection)
        debug("Encoding for this connections are %s %s" % 
          (con0.encoding,con1.encoding), level=2)

        return con0, con1
    except Exception,e:
        debug("connect(): FATAL ERROR: %s" % str(e))
        raise
    
def main_loop(dsn0, dsn1, is_killed, skip_user, keepalive, debug=debug):
    """Open connections, listen for signals and process replica logs (forever)
     * dsn0: master database connection string
     * dsn1: slave database connection string
     * is_killed: function to check if this thread has been killed (os signal)
     * skip_user: do not replay logs of this user (useful in multimaster setup)
     * keepaive: if true, do a simple query to keep alive the db connection 
     * debug: function to write to the logfile
    """
    con0, con1 = connect(dsn0, dsn1, debug)
    cur0 = con0.cursor()
    cur1 = con1.cursor()

    # process previous logs:
    replicate(cur0, cur1, skip_user, debug)

    # main loop:
    try:
        # set isolation level for LISTEN 
        con0.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur0.execute('LISTEN "replicas"')
        # set isolation level to prevent read problems
        con0.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        con1.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        while not is_killed():
            debug("Waiting for 'NOTIFY'",level=3)
            if select.select([cur0],[],[], TIMEOUT)==([],[],[]):
                debug("Timeout!",level=3)
            else:
                if cur0.isready():
                    debug("main_loop():Got NOTIFY: %s" % str(cur0.connection.notifies.pop()),level=3)
                    replicate(cur0,cur1,skip_user,debug)
            if keepalive:
                cur0.execute('SELECT now()')
                con0.commit()
                debug("main_loop():Keepalive0: %s" % cur0.fetchone()[0])
                cur1.execute('SELECT now()')
                con1.commit()
                debug("main_loop():Keepalive1: %s" % cur1.fetchone()[0])
        raise SystemExit()
    except Exception, e:
        debug("main_loop():FATAL ERROR: %s" % str(e))
        # some cleanup
        try:
            if con0 and not con0.closed:
                con0.close()
        except:
            pass
        try:
            if con1 and not con1.closed:
                con1.close()
        except:
            pass
        raise 

if __name__=="__main__":
    # find connection strings from argv or environ
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
    main_loop(DSN0,DSN1,is_killed=lambda:False, skip_user='', keepalive = True)
    sys.exit(1)
