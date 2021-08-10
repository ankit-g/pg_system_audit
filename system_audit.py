"""
    https://wiki.postgresql.org/wiki/Audit_trigger
"""
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import inspect
from sqlalchemy.orm import sessionmaker
from faker import Faker
from sqlalchemy import text
from sqlalchemy import inspect
from config import *

faker = Faker()
DATABASE_URI_PROD = f"postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DATABASE}"

engine = create_engine(DATABASE_URI_PROD, echo = True)

CREATE_LOGGED_ACTIONS_TABLE = """

    create schema audit;
    revoke create on schema audit from public;

    create table audit.logged_actions (
	schema_name text not null,
	table_name text not null,
	user_name text,
	action_tstamp timestamp with time zone not null default current_timestamp,
	action TEXT NOT NULL check (action in ('I','D','U')),
	original_data jsonb,
	new_data jsonb,
	query text
    ) with (fillfactor=100);

    revoke all on audit.logged_actions from public;

    grant select on audit.logged_actions to public;

    create index logged_actions_schema_table_idx 
    on audit.logged_actions(((schema_name||'.'||table_name)::TEXT));

    create index logged_actions_action_tstamp_idx 
    on audit.logged_actions(action_tstamp);

    create index logged_actions_action_idx 
    on audit.logged_actions(action);

"""

INSTALL_TRIGGER_FUNCTION = """

    CREATE OR REPLACE FUNCTION audit.if_modified_func() RETURNS trigger AS $body$
    DECLARE
	v_old_data JSONB;
	v_new_data JSONB;
    BEGIN

	if (TG_OP = 'UPDATE') then
	    v_old_data := TO_JSONB(OLD.*);
	    v_new_data := TO_JSONB(NEW.*);
	    insert into audit.logged_actions (schema_name,table_name,user_name,action,original_data,new_data,query) 
	    values (TG_TABLE_SCHEMA::TEXT,TG_TABLE_NAME::TEXT,session_user::TEXT,substring(TG_OP,1,1),v_old_data,v_new_data, current_query());
	    RETURN NEW;
	elsif (TG_OP = 'DELETE') then
	    v_old_data := TO_JSONB(OLD.*);
	    insert into audit.logged_actions (schema_name,table_name,user_name,action,original_data,query)
	    values (TG_TABLE_SCHEMA::TEXT,TG_TABLE_NAME::TEXT,session_user::TEXT,substring(TG_OP,1,1),v_old_data, current_query());
	    RETURN OLD;
	elsif (TG_OP = 'INSERT') then
	    v_new_data := TO_JSONB(NEW.*);
	    insert into audit.logged_actions (schema_name,table_name,user_name,action,new_data,query)
	    values (TG_TABLE_SCHEMA::TEXT,TG_TABLE_NAME::TEXT,session_user::TEXT,substring(TG_OP,1,1),v_new_data, current_query());
	    RETURN NEW;
	else
	    RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,now();
	    RETURN NULL;
	end if;

    EXCEPTION
        WHEN data_exception THEN
            RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
            RETURN NULL;
        WHEN unique_violation THEN
            RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
            RETURN NULL;
        WHEN others THEN
            RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
            RETURN NULL;

    END;
    $body$
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog, audit;



"""


INSTALL_TRIGGER_FOR = """
    CREATE TRIGGER {table_name}_audit
    AFTER INSERT OR UPDATE OR DELETE ON {table_name}
    FOR EACH ROW EXECUTE PROCEDURE audit.if_modified_func();
"""

INSTALL_JSON_DIFF_FUNCTION = """

    CREATE OR REPLACE FUNCTION jsonb_diff_val(val1 JSONB,val2 JSONB)
    RETURNS JSONB AS $$
    DECLARE
      result JSONB;
      v RECORD;
    BEGIN
       result = val1;
       FOR v IN SELECT * FROM jsonb_each(val2) LOOP
	     IF result @> jsonb_build_object(v.key,v.value)
		    THEN result = result - v.key;
	     ELSIF result ? v.key THEN CONTINUE;
	     ELSE
		    result = result || jsonb_build_object(v.key,'null');
	     END IF;
       END LOOP;
       RETURN result;
    END;
    $$ LANGUAGE plpgsql;

"""


TEST_TRIGGER_FOR = """
 -- Tested with a table named "t"
 drop table if exists t;
 create table t (x int not null primary key, y text);

 -- needs to be applied to all tables that we want to monitor

 -- this is a test trigger to show how we can audit all changes to the relevant tables, including inserts
 CREATE TRIGGER t_if_modified_trg 
 AFTER INSERT OR UPDATE OR DELETE ON t
 FOR EACH ROW EXECUTE PROCEDURE if_modified_func();


 -- Some sample updates, deletes, and inserts to illustrate the points
 select * from t; select * from audit.logged_actions;

 insert into t (x,y) values (1,'asdf'),(2,'werwer'),(3,null);

 select * from t; select * from logged_actions;

 -- You may have noticed that the times output in the prior query are in your local time. 
 -- They're stored as UTC, but Pg is converting them for display according to the 'timezone' GUC.
 SHOW timezone;

 -- See?
 SET timezone = 'UTC';
 SELECT * FROM logged_actions;
 RESET timezone;
 -- Another way to achieve the same effect:
 SELECT *, action_tstamp AT TIME ZONE 'UTC' AS action_tstamp_utc FROM logged_actions;

 update t set y='eeeeee' where x=2;
 select * from t; select * from logged_actions;

 update t set y='yuyuyuy' where x=3;
 select * from t; select * from logged_actions;

 delete from t where x=1;
 select * from t; select * from logged_actions;

 -- should be a pk violation
 update t set x=4 where x=2;
 select * from t; select * from logged_actions;
"""


def main():
    engine.execute(text(CREATE_LOGGED_ACTIONS_TABLE))
    engine.execute(text(INSTALL_JSON_DIFF_FUNCTION))
    engine.execute(text(INSTALL_TRIGGER_FUNCTION))
    for t in inspect(engine).get_table_names():
        try:
            engine.execute(text(INSTALL_TRIGGER_FOR.format(table_name=t)))
        except Exception as ex:
            print(ex)
    #engine.execute(text(TEST_TRIGGER_FOR))


if __name__ == '__main__':
    main()


