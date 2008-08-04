#!/usr/bin/env python
# simple test cases for two phase commit extensions to psycopg2
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

import unittest

import psycopg2
import tpc

from psycopg2.extensions import ISOLATION_LEVEL_SERIALIZABLE, \
         ISOLATION_LEVEL_AUTOCOMMIT,ISOLATION_LEVEL_READ_COMMITTED
from psycopg2 import ProgrammingError


# for compatibility with psycopg2 tests
class tests:
    dsn = "dbname='test' user='postgres' password='psql' port=5432"
gid = 'test-gid-1234'



class TwoPhaseTests(unittest.TestCase):

    def setUp(self):
        self.connect()
        self.curs=self.conn.cursor()
        self.clean()
        # set psycopg2 default isolation level
        self.conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
        ##self.curs.execute("CREATE TABLE table1 ( data TEXT )")

    def tearDown(self):
        self.clean()
            
    def connect(self):    
        self.conn = psycopg2.connect(tests.dsn,
                    connection_factory=tpc.TwoPhaseCommitConnection)
        self.xid = self.conn.xid(0,gid,'')

    def clean(self):
        self.assertEqual(self.conn.isolation_level, ISOLATION_LEVEL_READ_COMMITTED) 
        # clean table 1
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        # rollback any prepared transaction
        err=False
        for xid in self.conn.tpc_recover():
            print "rolling back xid[1]"
            self.curs.execute("ROLLBACK PREPARED %s",(xid[1],))
            err=True
        if err:
            raise RuntimeError("Unhandled prepared TPC transaction")
        self.curs.execute("DELETE FROM table1")           
       
    def insert(self):
        self.curs.execute("INSERT INTO table1 (data) VALUES ('1234')")
        
    def rowcount(self):
        self.curs.execute("SELECT * FROM table1 ")
        return self.curs.rowcount
        
    def test_one_phase_commit(self):
        "Test to commit a one phase transaction"
        self.conn.tpc_begin(self.xid)
        self.insert()
        self.conn.tpc_commit()
        self.assertEqual(self.rowcount(), 1) 
        
    def test_one_phase_rollback(self):
        "Test to rollback a one phase transaction"
        self.conn.tpc_begin(self.xid)
        self.insert()
        self.conn.tpc_rollback()
        self.assertEqual(self.rowcount(), 0) 

    def test_two_phase_commit(self):
        "Test to commit a complete two phase transaction"
        self.conn.tpc_begin(self.xid)
        self.insert()
        self.conn.tpc_prepare()
        self.conn.tpc_commit()
        self.assertEqual(self.rowcount(), 1) 

    def test_two_phase_rollback(self):
        "Test to rollback a complete two phase transaction"
        self.conn.tpc_begin(self.xid)
        self.conn.tpc_prepare()
        self.conn.tpc_rollback()
        self.assertEqual(self.rowcount(), 0)
        
    def test_recovered_commit(self):
        "Test to commit a recovered transaction"
        self.conn.tpc_begin(self.xid)
        self.insert()
        self.conn.tpc_prepare()
        self.connect() # reconnect
        self.assertEqual(self.conn.tpc_recover(), [self.xid])
        self.conn.tpc_commit(self.xid)
        self.assertEqual(self.rowcount(), 1) 

    def test_recovered_rollback(self):
        "Test to rollback a recovered transaction"
        self.conn.tpc_begin(self.xid)
        self.insert()
        self.conn.tpc_prepare()
        self.connect() # reconnect
        self.assertEqual(self.conn.tpc_recover(), [self.xid])
        self.conn.tpc_rollback(self.xid)
        self.assertEqual(self.rowcount(), 0) 

    def test_single_phase_commit(self):
        "Test to commit a single phase (normal) transaction"
        self.insert()
        self.conn.commit()
        self.assertEqual(self.rowcount(), 1) 

    def test_single_phase_rollback(self):
        "Test to rollback a single phase (normal) transaction"
        self.insert()
        self.conn.rollback()
        self.assertEqual(self.rowcount(), 0)
        
    def test_dbapi20_tpc(self):
        "Test basic dbapi 2.0 conformance"
        self.assertEqual(len(self.conn.tpc_recover()),0)
    
        # tpc_commit outside tpc transaction 
        self.assertRaises(ProgrammingError, self.conn.tpc_commit)

        # commit or rollback inside tpc transaction 
        self.conn.tpc_begin(self.xid)
        self.assertRaises(ProgrammingError, self.conn.commit)
        self.assertRaises(ProgrammingError, self.conn.rollback)
        self.conn.tpc_rollback()

        # transaction not prepared
        self.assertRaises(ProgrammingError, self.conn.tpc_commit,self.xid)
        self.assertRaises(ProgrammingError, self.conn.tpc_rollback,self.xid)
        

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TwoPhaseTests)
    #suite.debug()
    unittest.TextTestRunner(verbosity=2).run(suite)

