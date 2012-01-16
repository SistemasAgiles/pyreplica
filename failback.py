# -*- coding: latin1 -*-
"Script para failback (resincronizar en caso de que un servidor se haya caido)"

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

if con0 is None or con1 is None:
    raise "No hay conexión a las bases de datos"

cur0 = con0.cursor()
cur1 = con1.cursor()

# leo del log del master los comandos sql no replicados: 
sql = "SELECT id, sql, txid FROM replica_log WHERE NOT slave1 ORDER BY ts, id"
cur0.execute(sql)
for (id, sql, txid) in cur0:
    debug("replicating: id=%d txid=%d %s" % (id, txid, sql))
    cur1.execute(sql)
    
# marco los datos como replicados
sql = "UPDATE replica_log SET slave1=true " \
      "WHERE NOT slave1 "
cur0.execute(sql)

# cambio el estado de master único
cur0.execute("INSERT INTO replica_status (master_only) VALUES (False)")
cur1.execute("INSERT INTO replica_status (master_only) VALUES (False)")

con0.commit()
con1.commit()
con0.rollback()
con1.rollback()

debug("resynchronized!")
