-- upgrade from < 1.0.2

-- stop pyreplica

ALTER TABLE replica_log ADD COLUMN replicated BOOLEAN DEFAULT FALSE;
UPDATE replica_log SET replicated=TRUE WHERE ID< (see current replica_log_id_seq value in slave);

-- restart postgresql and pyreplica