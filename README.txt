PyReplica README (wiki format):

PyReplica is a simple Python-based PostgreSQL master-slave asynchronous replicator, using a master plpython trigger, signals, sequences, and a python client script (influenced by slony & londiste, but much more simple and easy). 

It is programmed in Python, aimed to be simple and flexible, allowing:
 * Easy installation (just run a sql script on the server, and copy a python daemon on client, no compilation required)
 * Easy administration (almost no management needed for normal usage, no new command set or framework to learn)
 * Easy (manual) customization (simple and extensible python scripts, allowing to filter and/or transform replicated data)
 * Efficient (low memory and network footprint, no polling)
 * Multiplatform: runs on linux and windows (tested on Debian and Windows XP)

It does not do:
 * Automatic Fail over
 * Conflict resolution (as a master-slave system, they shouldn't happen)
 * Repliction of schema changes (CREATE/ALTER/etc. commands should be done manually in all servers, although replica_log table can be used to propagate them)
 * Support Large objects by now (oid based replica could be supported in next releases)

It consist on a plpythonu master log trigger (py_log_replica) and slave client script (pyreplica.py)
The trigger stores a replication log (DML INSERT,UPDATE,DELETE instructions on affected tables on a replica_log table) and signals a NOTIFY message to replicas.
The slave client script connects to both databases (master and slave), listen to NOTIFY signals on master database, and replays the replicated log in the slave database when this signal arrives. It uses a sequence and transactions to prevent data loss. 

The trigger detects changes and stores it using the table primary key. So, if the table has no primary key, it can't be replicated.

As it uses NOTIFY signals, replication is almost instantaneous and efficient (no polling). If client is down and NOTIFY signals are lost, when the client script gets online again, automatically it replays the "lost" replicated data.

Simple benchmarks shows that this trigger is only 50% slower than a C based one (as in slony-I), with the benefits that it can be easily ported, installed, maintained and customized. (see benchmarks.txt)

See INSTALL.txt for installation procedure.
