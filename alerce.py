#! /usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2009 Mariano Reingart <mariano@nsis.com.ar>
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

"Alerce: 2PC distributed sync replication - dbapi compilant"

__author__ = "Mariano Reingart <mariano@nsis.com.ar>"
__copyright__ = "Copyright (C) 2009 Mariano Reingart"
__license__ = "LGPL 3.0"
__version__ = "1.0"

from psycopg2.extensions import connection, ISOLATION_LEVEL_SERIALIZABLE, \
         ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from psycopg2 import DataError, DatabaseError, Error, IntegrityError, \
     InterfaceError, InternalError, NotSupportedError, OperationalError, \
     ProgrammingError, Binary
import tpc
import socket
import thread

class Connection:
    "2PC Distributed Syncronic Replication connection" 
    def __init__(self, dsn0, dsn1):
        self.dsn0 = dsn0
        self.dsn1 = dsn1
        self.gid0 = "%s-%08x-%08x-%s" % (socket.gethostname(), thread.get_ident(), id(self), 'a')
        self.gid1 = "%s-%08x-%08x-%s" % (socket.gethostname(), thread.get_ident(), id(self), 'b')
        self.connect()
    
    def pg_connect(self, dsn, factory=tpc.TwoPhaseCommitConnection):
        try:
            if dsn:
                print "conectando", dsn
                return psycopg2.connect(dsn, connection_factory=factory)
        except psycopg2.Error:
            pass
    
    def degenerate(self):
        "Evaluar si la conexión Slave ha sido dada de baja"
        if self.test_master_only(self.con0):
            return True
        else:
            self.error("¡No se puede conectar a la bd secundaria!")
        
    def promote(self):
        "Evaluar si una conexión Slave ha sido promovida a Master"
        if self.test_master_only(self.con1):
            self.con0, self.con1 = self.con1, None
            self.gid0, self.gid1 = self.gid1, self.gid0
            return True
        else:
            self.error("¡No se puede conectar a la bd principal!")

    def test_master_only(self, con):
        "Comprueba que una conexión sea maestra y única"
        cur = con.cursor()
        cur.execute("SELECT master_only FROM replica_status ORDER BY ts DESC LIMIT 1")
        master_only = cur.fetchone()[0]
        return master_only
         
    def connect(self):
        self.con0 = self.pg_connect(self.dsn0)
        self.con1 = self.pg_connect(self.dsn1)
        
        if not self.con0 and not self.con1:
            self.error("¡No se puede conectar a las bd!")
        elif not self.con1: # si no hay esclavo, trato de degenerar
            self.degenerate()
        elif not self.con0: # si no hay maestro, trato de promover el esclavo
            self.promote()
        
        self.xid0 = self.con0.xid(None,self.gid0,None)
        if self.con1:
            self.xid1 = self.con1.xid(None,self.gid1,None)
        self.in_tx = False
        
        # reviso las transacciones en dos fases preparadas
        tpc_xid0_pepared = self.xid0 in self.con0.tpc_recover()
        tpc_xid1_pepared = self.con1 and self.xid1 in self.con1.tpc_recover()
        
        if tpc_xid0_pepared: 
            self.error("Transacción Preparada para %s, confirmar o borrar!" % self.gid0)
        if tpc_xid1_pepared: 
            self.error("Transacción Preparada para %s, confirmar o borrar!" % self.gid1)
        self.begin()
        self.cur0 = self.con0.cursor()
        if self.con1:
            self.cur1 = self.con1.cursor()

    def begin(self):
        # comienzo una transacción implícita (al comenzar, luego de commit)
        if not self.in_tx:
            self.con0.tpc_begin(self.xid0)
            if self.con1:
                self.con1.tpc_begin(self.xid1)
            self.in_tx = True

    def debug(self, msg, level=3):
        ##print msg
        pass

    def error(self, msg):
        raise InterfaceError(msg)
        
    def cursor(self):
        # create a real cursor in the first database (query)
        return self.con0.cursor()
    
    def commit(self):
        if not self.in_tx:
            self.error("Transacción en estado indefinido - reconectar (CI)")
        # replicate data:
        if self.con1:
            self.cur0.execute("SELECT id, sql FROM replica_log "
                 "WHERE txid = txid_current() " 
                 "ORDER BY id ")   
            for row in self.cur0:
                sql = row[1]
                self.debug("Replicating: %s" % sql, level=2)
                self.cur1.execute(sql)
            # marco los datos replicados (para failback)
            sql = "UPDATE replica_log SET slave1=true " \
                  "WHERE txid = txid_current() AND NOT slave1 "
            self.cur0.execute(sql)
            self.debug("Updating log: %s" % sql, level=2)
        try:
            # prepare remote transactions:
            self.con0.tpc_prepare()
            try:
                if self.con1:
                    self.con1.tpc_prepare()
            except: 
                self.con0.tpc_rollback()
                raise
            # commit second phase of TPC transaction
            self.con0.tpc_commit()
            if self.con1:
                self.con1.tpc_commit()
            self.in_tx = False
            self.begin()
        except psycopg2.Error:
            self.in_tx = None # estado indefinido, reconectar!
            raise

    def rollback(self):
        if self.in_tx:
            self.con0.tpc_rollback()
            if self.con1:
                self.con1.tpc_rollback()
            self.in_tx = False
            self.begin()
        elif self.in_tx is None:
            self.error("Transacción en estado indefinido - reconectar (RB)")
        
    def close(self):
        self.con0.close()
        if self.con1:
            self.con1.close()
    
    def reconnect(self):
        try:
            self.connect()
            return True
        except:
            return False

def connect(dsns):
    return Connection(dsns[0],dsns[1])

if __name__=="__main__":
    c = connect(('dbname=master user=postgres password=pg83',
                   'dbname=slave user=postgres password=pg83'))
    cur=c.cursor()
    cur.execute("insert into test (t) values (%s)", ["prueba"])
    c.commit()
    cur.execute("select * from test")
    for row in cur:
        print row
