-- == Replica (slave) db ==

-- create sequence (to track logs proccessed):
CREATE SEQUENCE replica_log_id_seq START 0 MINVALUE 0;

-- setup permission (if applicable):
--GRANT ALL ON replica_log_id_seq TO someone; 