# -*- coding: latin1 -*-
"Script para failback (resincronizar en caso de que un servidor se haya caido)"

from psycopg2.extensions import connection, ISOLATION_LEVEL_SERIALIZABLE, \
         ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
import tpc
import socket
import thread
import time

con0 = con1 = None
def pg_connect(dsn, factory=tpc.TwoPhaseCommitConnection):
    try:
        return psycopg2.connect(dsn, connection_factory=factory)
    except Exception, e:
        print str(e)
        pass

def debug(msg, level=1):
    print msg
    
dns0 = 'host=lenny dbname=elecciones user=mariano password=lkfhlvxcho8380320s'
dns1 = 'host=carl dbname=elecciones user=mariano password=lkfhlvxcho8380320s'
con0 = pg_connect(dns0)
con1 = pg_connect(dns1)

if con0 is None or con1 is None:
    raise "No hay conexión a las bases de datos"

cur0 = con0.cursor()
cur1 = con1.cursor()

#sql = "set search_path to 'internas_salta09'"
#cur0.execute(sql)
#cur1.execute(sql)

# leo del log del master los comandos sql no replicados: 
while 1:
    sql = "SELECT id_estado, count(*), (SELECT SUM(votos1) FROM planillas_det WHERE planillas_det.id_planilla IN (SELECT p1.id_planilla FROM planillas p1 WHERE p1.id_estado=planillas.id_estado) ), (SELECT SUM(votos2) FROM planillas_det WHERE planillas_det.id_planilla IN (SELECT p1.id_planilla FROM planillas p1 WHERE p1.id_estado=planillas.id_estado) ) FROM planillas GROUP BY id_estado ORDER BY id_estado"
    cur0.execute(sql)
    cur1.execute(sql)
    filas0 = cur0.fetchall()
    filas1 = cur1.fetchall()
    for i in range(len(filas0)):
        print i, filas0[i], filas1[i], filas0[i]==filas1[i] and 'OK' or 'DIFF'
    print
    print
    time.sleep(2)
