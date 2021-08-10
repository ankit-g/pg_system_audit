# pg_system_audit
record  CUD operations in postgres in JSON format for reporting and FINANCIAL Audits.


SELECT action,
       query,
       Jsonb_pretty(Jsonb_diff_val(new_data, original_data)),
       action_tstamp
FROM   audit.logged_actions action_tstamp; 
