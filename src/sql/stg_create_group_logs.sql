DROP TABLE IF EXISTS VT2603040CA38D__STAGING.group_log;

CREATE TABLE VT2603040CA38D__STAGING.group_log (
    group_id INT,
    user_id INT,
    user_id_from INT,
    event VARCHAR(20),
    datetime TIMESTAMP
)
ORDER BY datetime
PARTITION BY datetime::date
GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);