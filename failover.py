# -*- coding: latin1 -*-
"Script para failover (limpiar transaccones y switchear a un servidor en caso de fallas"

from psycopg2.extensions import connection, ISOLATION_LEVEL_SERIALIZABLE, \
         ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
import tpc
import socket
import thread

con0 = con1 = None
def pg_connect(dsn, factory=tpc.TwoPhaseCommitConnection):
    try:
        return psycopg2.connect(dsn, connection_factory=factory)
    except Exception, e:
        print str(e)
        pass

def debug(msg, level=1):
    print msg

dns0 = 'host=lenny.salta dbname=mariano user=mariano password='
dns1 = 'host=carl.salta dbname=mariano user=mariano password='
con0 = pg_connect(dns0)
con1 = pg_connect(dns1)

if con0 is None and con1 is None:
    raise "No hay conexión a las bases de datos"

if con0 is not None and con1 is None:
    if raw_input("No hay con1 (slave), ¿Borrar Transacciones en con0 (master)?  (Si/no)")=="Si":
        for xid0 in con0.tpc_recover():
            debug("con0.tpc_rollback %s" % xid0[1])
            con0.tpc_rollback(xid0)
        debug("done.")
    if raw_input("¿Setear con0 (master) como unico?  (Si/no)")=="Si":
        cur0 = con0.cursor()
        cur0.execute("INSERT INTO replica_status (master_only) VALUES (True)")
        con0.commit()
        debug("cur0 master only!")
    exit(0)
elif con0 is None and con1 is not None:
    if raw_input("No hay con0 (master), ¿Borrar Transacciones en con1 (slave)?  (Si/no)")=="Si":
        for xid0 in con1.tpc_recover():
            debug("con1.tpc_rollback %s" % xid0[1])
            con1.tpc_rollback(xid0)
        debug("done.")
    if raw_input("¿Setear con1 (master) como unico?  (Si/no)")=="Si":
        cur1 = con1.cursor()
        cur1.execute("INSERT INTO replica_status (master_only) VALUES (True)")
        con1.commit()
        debug("cur1 master only!")
    exit(0)

print "analizar, los dos server están arriba!"
exit(1)

# busco las transacciones preparadas en ambos servers y las comiteo o rollback según corresponda
xids0 = con0.tpc_recover() 
xids1 = con1.tpc_recover()

for tpc_xid0 in xids0:
    # regenero el transaction id slave
    fmt_id0,gid0,branch0 = tpc_xid0
    if gid0[-1]=='b':
        continue # gid del server slave
    tpc_xid1 = con1.xid(0,gid0[:-1] + 'b','')
    if tpc_xid1 in xids1:
        # commit both
        con0.tpc_commit(tpc_xid0)
        con1.tpc_commit(tpc_xid1)
        debug("tpc_commit con0 and con1 %s" % tpc_xid0[1], level=3)
    elif not tpc_xid1 in xids1:
        # rollback origin (replica prepare failed)
        con0.tpc_rollback(tpc_xid0)
        debug("tpc_rollback con0 %s" % tpc_xid0[1], level=3)


for tpc_xid1 in xids1:
    # regenero el transaction id master
    fmt_id1,gid1,branch1 = tpc_xid1
    if gid0[-1]=='a':
        continue # gid del server master
    tpc_xid0 = con1.xid(0,gid1[:-1] + 'a','')
    if tpc_xid0 not in xids0: 
        # commit replica (origin commit was successful)
        # esto no debería pasar, debería estar rollbackeada a mano
        con1.tpc_commit(tpc_xid1)
        debug("tpc_commit con1 %s" % tpc_xid1[1], level=3)

print "done!"