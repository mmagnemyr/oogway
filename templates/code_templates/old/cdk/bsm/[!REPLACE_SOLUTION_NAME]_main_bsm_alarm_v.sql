CREATE OR REPLACE VIEW odl_monitoring.[!REPLACE_SOLUTION_NAME]_main_bsm_alarm_v AS 
SELECT
  code
, timestamp last_state_created_time
FROM
  odl_monitoring.v_latestalarm
WHERE (task_name LIKE '[!REPLACE_SOLUTION_NAME]_main%')
ORDER BY last_state_created_time DESC
LIMIT 1
