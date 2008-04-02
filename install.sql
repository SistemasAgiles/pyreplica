CREATE OR REPLACE FUNCTION py_log_replica()
  RETURNS "trigger" AS
$BODY$

  # function to convert value from python to postgres representation
  def mogrify(v):
    if v is None: return 'NULL' 
    if isinstance(v,basestring) return repr(v)
    return "'%s'" % repr(v) # to get rid of bool that are passed as ints (solved in pg8.3)

  # retrieve or prepare plan for faster processing
  if SD.has_key("plan"):
      plan = SD["plan"]
  else:
      plan = plpy.prepare("INSERT INTO replica_log (sql) VALUES ($1)", [ "text" ])
      SD["plan"] = plan

  new = TD['new']
  old = TD['old']
  event = TD['event']

  # arguments passed in CREATE TRIGGER (specify relname and primary keys)
  args = TD['args'] 
  relname = args[0]
  primary_keys = args[1:]

  # make sql according with trigger DML action
  if event == 'INSERT':
    sql = 'INSERT INTO "%s" (%s) VALUES (%s)' % (
            relname,
            ', '.join(['"%s"' % k for k in new.keys()]),
            ', '.join([mogrify(v) for v in new.values()]),
          )
  elif event == 'UPDATE':
    sql = 'UPDATE "%s" SET %s WHERE %s' % (
            relname,
            ', '.join(['"%s"=%s' % (k,mogrify(v)) for k,v in new.items() if old[k]<>v]),
            ' AND '.join(['"%s"=%s' % (k,mogrify(v)) for k,v in old.items() if k in primary_keys]),
          )
  elif event == 'DELETE':
    sql = 'DELETE FROM "%s" WHERE %s' % (
            relname,
            ' AND '.join(['"%s"=%s' % (k,mogrify(v)) for k,v in old.items() if k in primary_keys]),
          )

  # store trigger log sql
  plpy.execute(plan, [ sql ], 0)

  # notify listeners that new data is available
  r = plpy.execute('NOTIFY "replicas"', 0)

  #plpy.debug (sql)
  

  ## $BODY$
  LANGUAGE 'plpythonu' VOLATILE;

-- create log table (where sql replica queries are stored):

CREATE TABLE replica_log (
 id SERIAL,
 sql TEXT,
 ts TIMESTAMP DEFAULT now()
) WITHOUT OIDS ;

-- for each table that needs replication, create a trigger like this:
-- CREATE TRIGGER test_trig AFTER INSERT OR UPDATE OR DELETE ON test FOR EACH ROW EXECUTE PROCEDURE py_log_trigger('test', 'id1', 'id2');
-- where 'test' is the table name and ('id1','id2') is the primary key 
