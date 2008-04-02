CREATE TABLE test
(
  id serial NOT NULL,
  t text,
  n float8,
  f timestamp,
  b bool,
  CONSTRAINT pk PRIMARY KEY (id)
) WITHOUT OIDS;


CREATE OR REPLACE FUNCTION benchmark(iterations int4)
  RETURNS "interval" AS
$BODY$
   DECLARE
      s timestamp;
      e timestamp;
      i int := 0;
   BEGIN
      s := timeofday();
      LOOP
         i:=i+1;
         IF i>iterations THEN 
            EXIT;
         END IF;
         INSERT INTO test (t,n,f,b) VALUES (random()::text, random(), now(), True);
      END LOOP;
      e := timeofday();
      RETURN extract(epoch from e) - extract(epoch from s);
   END;
   $BODY$
  LANGUAGE 'plpgsql' VOLATILE;


DROP TRIGGER test_trigger ON test;
delete from test;
delete from replica_log;
vacuum full analyze;
CREATE TRIGGER test_trigger
  AFTER INSERT OR UPDATE OR DELETE
  ON test
  FOR EACH ROW
  EXECUTE PROCEDURE py_log_trigger('test', 'id');

select benchmark(100000);
select benchmark(100000);
select benchmark(100000);
select benchmark(100000);


DROP TRIGGER test_trigger ON test;
delete from test;
delete from replica_log;
vacuum full analyze;
create trigger test_trigger after insert or update or delete on test for each row execute procedure
test_plpgsql (); 

select benchmark(100000);
select benchmark(100000);
select benchmark(100000);
select benchmark(100000);


DROP TRIGGER test_trigger ON test;
delete from test;
delete from replica_log;
vacuum full analyze;
create trigger test_trigger after insert or update or delete on test for each row execute procedure
logTrigger ('_lalala', 188617, 'kvvvv'); -- schema, oid, k for key else

select benchmark(100000);
select benchmark(100000);
select benchmark(100000);
select benchmark(100000);



DROP TRIGGER test_trigger ON test;
delete from test;
delete from sl_log_1;
delete from sl_log_2;
delete from replica_log;
vacuum full analyze;
create trigger test_trigger after insert or update or delete on test for each row execute procedure
logTrigger ('public', 188617, 'kvvvv'); -- schema, oid, k for key else
