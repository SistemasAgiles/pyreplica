PyReplica README (wiki format):

PyReplica is another "master to multiple slaves" trigger-based replication system for PostgreSQL databases.

It is programmed in Python, aimed to be simple and flexible, allowing:
 * Easy installation
 * Easy administration
 * Easy (manual) customization
 * Efficient (low memory and network footprint)

It does not do:
 * Automatic Fail over
 * Conflict resolution (as a master-slave system, they shouldn't happen)

It consist on a plpythonu master log trigger (py_log_replica) and slave client script (pyreplica.py)
The trigger stores a replication log (DML INSERT,UPDATE,DELETE instructions on affected tables on a replica_log table) and signals a NOTIFY message to replicas.
The slave client script connects to both databases (master and slave), listen to NOTIFY signals on master database, and replays the replicated log in the slave database when this signal arrives. It uses a sequence and transactions to prevent data loss. 

The trigger detects changes and stores it using the table primary key. So, if the table has no primary key, it can't be replicated.

As it uses NOTIFY signals, replication is almost instantaneous and efficient (no polling). If client is down and NOTIFY signals are lost, when the client script gets online again, automatically it replays the "lost" replicated data.

Simple benchmarks shows that this trigger is only 50% slower than a C based one (as in slony-I), with the benefits that it can be easily ported, installed, maintained and customized. (see benchmarks.txt)

See INSTALL.txt for installation procedure.
