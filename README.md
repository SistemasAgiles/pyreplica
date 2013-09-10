pyreplica
=========

PyReplica is a simple Python-based PostgreSQL master-slave asynchronous replicator, using a master plpython trigger, signals, sequences, two-phase commits and a python client script (influenced by slony & londiste, but much more simple and easy).

It supports lazy async multi-master setup too (replication on both ways, each database is a master and a slave at the same time). Warning: you (or your application) must get rid of sequence handling and conflict resolution.

Includes Alerce, a dbapi2 compilant python interface that implements master/slave synchonous replication for postgresql using pyreplica (plpy trigger and two phase commit)

Project site:
-------------

Main repo: https://code.google.com/p/pyreplica
GitHub mirror: https://github.com/reingart/pyreplica

Presentations:
--------------

 * Español (ES):  http://docs.google.com/present/view?id=dd9bm82g_19gb36zrgz
 * English (EN):  http://docs.google.com/present/view?id=dd9bm82g_402fjtsdmdd
 * Português (BR):  http://docs.google.com/present/view?id=dd9bm82g_403spk7fpff

More info at:
-------------

 * http://www.sistemasagiles.com.ar/trac/wiki/PyReplicaEn
 * http://www.sistemasagiles.com.ar/trac/wiki/PyReplicaEs

Mailing List / Lista de correo:
-------------------------------

 http://lists.pgfoundry.org/mailman/listinfo/pyreplica-general
