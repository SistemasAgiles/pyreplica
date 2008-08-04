#! /usr/bin/python
# -*- coding: latin-1 -*-
# two phase commit extensions to psycopg2
# compilant with PEP249 
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

from psycopg2.extensions import connection, ISOLATION_LEVEL_SERIALIZABLE, \
         ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import ProgrammingError

class TwoPhaseCommitConnection(connection):
    def __init__(self,*args,**kwargs):
        self.__tpc_xid = None
        self.__tpc_prepared = None
        self.__tpc_prev_isolation_level = None
        connection.__init__(self,*args,**kwargs)

    def xid(self,format_id, global_transaction_id, branch_qualifier):
        """Create a Transaction IDs (only global_transaction_id is used in pg)
        format_id and branch_qualifier are not used in postgres
        global_transaction_id may be any string identifier supported by postgres
        returns a tuple (format_id, global_transaction_id, branch_qualifier)"""
        return (format_id, global_transaction_id, branch_qualifier)

    def tpc_begin(self,xid):
        "Begin a two-phase transaction"
        # store previous isolation level
        self.__tpc_prev_isolation_level = self.isolation_level
        # set isolation level to begin a TPC transaction
        # (actually in postgres at this point it is a normal one)
        self.set_isolation_level(ISOLATION_LEVEL_SERIALIZABLE)
        # store actual TPC transaction id
        self.__tpc_xid = xid
        self.__tpc_prepared = False
        
    def tpc_prepare(self):
        "Prepare a two-phase transaction"
        if not self.__tpc_xid:
            raise ProgrammingError("tpc_prepare() outside a TPC transaction is not allowed!")
        # Prepare the TPC
        curs = self.cursor()
        try:
            curs.execute("PREPARE TRANSACTION %s", (self.__tpc_xid[1],))
            self.__tpc_prepared = True
        finally:
            curs.close()

    def tpc_commit(self,xid=None):
        "Commit a prepared two-phase transaction"
        try:
            if not xid:
                # use current tpc transaction
                tpc_xid = self.__tpc_xid
                isolation_level = self.__tpc_prev_isolation_level
            else:
                # use a recovered tpc transaction
                tpc_xid = xid
                isolation_level = self.isolation_level
                if not xid in self.tpc_recover():
                    raise ProgrammingError("Requested TPC transaction is not prepared!")
            if not tpc_xid:
                raise ProgrammingError("Cannot tpc_commit() without a TPC transaction!")
            if self.__tpc_prepared or (xid != self.__tpc_xid and xid):
                # a two-phase commit:
                # set isolation level for the commit
                self.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                curs = self.cursor()
                try:
                    curs.execute("COMMIT PREPARED %s", (tpc_xid[1],))
                finally:
                    curs.close()
                    # return to previous isolation level
                    self.set_isolation_level(isolation_level)
            else:
                try:
                    # a single-phase commit
                    connection.commit(self)
                finally:
                    # return to previous isolation level
                    self.set_isolation_level(isolation_level)
        finally:
            # transaction is done, clear xid
            self.__tpc_xid = None

    def tpc_rollback(self,xid=None):
        "Commit a prepared two-phase transaction"
        try:
            if not xid:
                # use current tpc transaction
                tpc_xid = self.__tpc_xid 
                isolation_level = self.__tpc_prev_isolation_level
            else:
                # use a recovered tpc transaction
                isolation_level = self.isolation_level
                tpc_xid = xid
                if not xid in self.tpc_recover():
                    raise ProgrammingError("Requested TPC transaction is not prepared!")
            if not tpc_xid:
                raise ProgrammingError("Cannot tpc_rollback() without a TPC prepared transaction!")
            if self.__tpc_prepared or (xid != self.__tpc_xid and xid):
                # a two-phase rollback
                # set isolation level for the rollback
                self.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                curs = self.cursor()
                try:
                    curs.execute("ROLLBACK PREPARED %s;", (tpc_xid[1],))
                finally:
                    curs.close()
                    # return to previous isolation level
                    self.set_isolation_level(isolation_level)
            else:
                # a single-phase rollback
                try:
                    connection.rollback(self)
                finally:
                    # return to previous isolation level
                    self.set_isolation_level(isolation_level)                    
        finally:
            # transaction is done, clear xid
            self.__tpc_xid = None

    def tpc_recover(self):
        "Returns a list of pending transaction IDs"
        curs = self.cursor()
        xids = []
        try:
            # query system view that stores open (prepared) TPC transactions 
            curs.execute("SELECT gid FROM pg_prepared_xacts;");
            xids.extend([self.xid(0,row[0],'') for row in curs])
        finally:
            curs.close()
        # return a list of TPC transaction ids (xid)
        return xids

    def commit(self):
        if self.__tpc_xid:
            raise ProgrammingError("Cannot commit() inside a TPC transaction!")
        connection.commit(self)

    def rollback(self):
        if self.__tpc_xid:
            raise ProgrammingError("Cannot rollback() inside a TPC transaction!")
        connection.rollback(self)


