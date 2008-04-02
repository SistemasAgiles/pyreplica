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

# put in DSN your DSN string

#if len(sys.argv) > 1:
#    DSN = sys.argv[1]

DSN0 = 'dbname=adan user=postgres' # origin
DSN1 = 'dbname=replica user=postgres' # replica

# don't modify anything below tis line (except for experimenting)

import sys
import psycopg2
import select

print "Opening origin connection using dns:", DSN0
con0 = psycopg2.connect(DSN0)
print "Opening replica connection using dns:", DSN1
con1 = psycopg2.connect(DSN1)
print "Encoding for this connection is", con0.encoding

con0.set_isolation_level(0)
cur0 = con0.cursor()
cur1 = con1.cursor()

cur0.execute('LISTEN "replicas"')

while 1:
    print "Waiting for 'NOTIFY'"
    if select.select([curs],[],[], 1)==([],[],[]):
        print "Timeout"
    else:
        if cur0.isready():
            print "Got NOTIFY: %s" % str(cur0.connection.notifies.pop())
            try:
                # First, obtain last replicated id ("revision")
                cur1.execute("SELECT last_value FROM replica_log_id_seq")
                id = cur1.fetchone()[0]
                # Query replica data from last id
                cur0.execute("SELECT id,sql FROM replica_log WHERE id>%s",(id,))
                for row in cur0:
                    # Execute replica queries
                    print "EXECUTING ", row[1]
                    cur1.execute(row[1])
                    id = row[0]
                # set up last replicated id
                cur1.execute("SELECT setval('replica_log_id_seq'::regclass,%s)",(id,))
                con1.commit()
                con0.commit()
            except Exception, e:
                print "Error while replicating:",str(e)
                cur0.rollback()
                cur0.rollback()
