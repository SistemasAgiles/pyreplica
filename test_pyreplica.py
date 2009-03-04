#!/usr/bin/env python
# simple test cases for pyreplica
#
# Copyright (C) 2008 Mariano Reingart <mariano@nsis.com.ar>
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

import datetime
import pyreplica
import psycopg2
import unittest

DSN = "dbname='%s' user='postgres' port=5432 host=localhost"
DSN0 = DSN % "master"
DSN1 = DSN % "slave"

DEBUG = False

class PyReplicaTests(unittest.TestCase):
    test_table = "table1"
        
    def setUp(self):
        # connect to postgres
        con = psycopg2.connect(DSN % "postgres")
        cur = con.cursor()
        # create both test databases
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        try:
            cur.execute("DROP DATABASE master")
            cur.execute("DROP DATABASE slave")
        except: 
            pass
        cur.execute("CREATE DATABASE master")
        cur.execute("CREATE DATABASE slave")
        # connect to master/slave
        self.con0, self.con1 = pyreplica.connect(DSN0,DSN1, self.debug)
        self.cur0 = self.con0.cursor()
        self.cur1 = self.con1.cursor()
        # create test table
        sql = """CREATE TABLE %s (
            id SERIAL PRIMARY KEY, 
            t TEXT, 
            d TIMESTAMP DEFAULT now(),
            f FLOAT8 DEFAULT random(),
            n NUMERIC(100)  DEFAULT random(),
            ba BYTEA
            )""" % self.test_table
        self.cur0.execute(sql)
        self.cur1.execute(sql)
        # install pyreplica
        self.cur0.execute(open("master-install.sql").read())
        self.con0.commit()
        #self.cur1.execute(open("slave-install.sql").read())
        self.con1.commit()
        
    def tearDown(self):
        self.con0.rollback()
        self.con1.rollback()
        self.con0.close()
        self.con1.close()
        con = psycopg2.connect(DSN % "postgres")
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        # drop both test databases
        cur.execute("DROP DATABASE master")
        cur.execute("DROP DATABASE slave")

    def insert(self,cur,commit=True,**kwargs):
        cur.execute("INSERT INTO %s (%s) VALUES (%s)" % (self.test_table,
            ', '.join(["%s" % k for k in kwargs.keys()]),
            ', '.join(["%%(%s)s" % k for k in kwargs.keys()])), kwargs)
        if DEBUG: print cur.query
        cur.execute("SELECT * FROM %s WHERE id = currval('%s_id_seq')" % 
            (self.test_table,self.test_table) )
        if commit: cur.connection.commit()
        return cur.fetchone()[0]
    
    def update(self,cur,id,**kwargs):
        fields = kwargs.keys()
        kwargs['id'] = id
        cur.execute("UPDATE %s SET %s WHERE id=%%(id)s" % (self.test_table, 
            ', '.join(["%s=%%(%s)s" % (k,k) for k in fields])), kwargs)
        if DEBUG: print cur.query
        cur.connection.commit()
    
    def delete(self,cur,id):
        cur.execute("DELETE FROM %s WHERE id=%%s" % self.test_table, (id,))
        cur.connection.commit()

    def rowcount(self, cur):
        "return the rowcount from test table"
        cur.execute("SELECT COUNT(*) FROM %s " % self.test_table)
        return cur.fetchone()[0]
    
    def diff(self,phase):
        "compare tables in master and slave"
        self.cur0.execute("SELECT * FROM %s ORDER BY id" % self.test_table)
        self.cur1.execute("SELECT * FROM %s ORDER BY id" % self.test_table)
        self.assertTrue(self.cur0.rowcount==self.cur1.rowcount, 
            "%s: row count diffiers: %d != %d" % 
                (phase,self.cur0.rowcount,self.cur1.rowcount))
        for i in range(self.cur0.rowcount):
            row0 = self.cur0.fetchone()
            row1 = self.cur1.fetchone()
            #self.assertTrue(id0==id1, "%s: id %s != %s" % (phase,id0,id1))
            self.assertTrue(row0==row1, "%s: %s != %s" % (phase,row0,row1))

    def debug(self,message,level=1):
        if not level: # conflict warning?
            # we can be inside a TPC transaction, store warning and continue
            self.warnings.append(message)
        if DEBUG: print message

    def replicate(self, must_conflict = False):
        self.warnings = []
        pyreplica.replicate(self.cur0, self.cur1, debug=self.debug)
        # check for warning messages
        if not must_conflict:
            self.assertFalse(self.warnings, 
                "There were unexpected conflict warnings:\n%s" %
                '\n'.join(self.warnings))
        else:
            self.assertTrue(self.warnings, 
                "There were no conflict but they were expected")
    
    def test_basic(self):
        "Test normal insert, update, delete"
        id = self.insert(self.cur0,t='spam')
        self.replicate()
        self.diff("INSERT")
        self.update(self.cur0,id,t='eggs',f=3.14,d=datetime.datetime.now())
        self.replicate()
        self.diff("UPDATE")
        self.delete(self.cur0,id)
        self.replicate()
        self.diff("DELETE")
        id = self.insert(self.cur0,t='spam')
        self.replicate()
        
    def test_conflicts(self):
        "Test normal insert, update, delete with conflicts"
        id = self.insert(self.cur0,t='spam')
        self.replicate()
        self.diff("INSERT")
        self.update(self.cur1,id,t='bacon',f=2.0,d=datetime.datetime.now())
        self.update(self.cur0,id,t='eggs',f=None,d=None) 
        self.replicate(must_conflict=True)
        self.diff("INSERT CONFLICT")
        self.delete(self.cur1,id)        # delete on slave so it must conflict
        self.delete(self.cur0,id)
        self.replicate(must_conflict=True)
        self.diff("DELETE CONFLICT")

    def test_integrity(self):
        "Test integrity error"
        id = self.insert(self.cur0,t='spam')
        id = self.insert(self.cur1,t='spam')
        self.assertRaises(psycopg2.IntegrityError,self.replicate)
        self.con0.tpc_rollback()
        self.con1.tpc_rollback()

    def test_multiple(self):
        "Test multiple (bulk) insert, update and deletes "
        for x in xrange(1000):
            i = self.insert(self.cur0,t='spam')
        self.replicate()
        self.diff("INSERT MULTIPLE")
        self.cur0.execute("UPDATE %s SET t=%%s" % self.test_table, ("bacon",))
        self.cur0.connection.commit()
        self.replicate()
        self.diff("UPDATE MULTIPLE")
        self.cur0.execute("DELETE FROM %s" % self.test_table, ())
        self.cur0.connection.commit()
        self.replicate()
        self.diff("DELETE MULTIPLE")
        
    def test_bytea(self):
        "Test bytea datatype"
        self.insert(self.cur0, 
            ba=(psycopg2.Binary(''.join([chr(i) for i in xrange(0,255)])),))
        self.replicate()
        self.diff("BYTEA INSERT")

    def test_long_transaction(self):
        "Test long/overlapping transactions"
        # open new connection (to do parallel transactions)
        con0, con1 = pyreplica.connect(DSN0,DSN1, self.debug)
        cur0 = con0.cursor()
        # insert, but keep the transaction open
        i = self.insert(cur0, commit=False, t='spam')
        # insert and commit transaction
        i = self.insert(self.cur0,commit=True, t='eggs')
        self.replicate()
        self.diff("INSERT LONG 2")
        self.assertTrue(self.rowcount(self.cur1)==1,"It showld be one rows")
        con0.commit()
        # clean up
        con0.close()
        con1.close()
        self.replicate()
        self.diff("INSERT LONG 1")
        self.assertTrue(self.rowcount(self.cur1)==2,"It showld be two rows")


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(PyReplicaTests)
    #suite.debug()
    unittest.TextTestRunner(verbosity=2).run(suite)

