DROP TABLE IF EXISTS VT2603040CA38D__DWH.s_auth_history;

CREATE TABLE VT2603040CA38D__DWH.s_auth_history
(
    hk_l_user_group_activity BIGINT NOT NULL,
    user_id_from INT,
    event VARCHAR(20),
    event_dt TIMESTAMP,
    load_dt DATETIME,
    load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);

INSERT INTO VT2603040CA38D__DWH.s_auth_history(
    hk_l_user_group_activity, 
    user_id_from,
    event,
    event_dt,
    load_dt,
    load_src
)
SELECT DISTINCT
    luga.hk_l_user_group_activity,
    gl.user_id_from,
    gl.event,
    gl.datetime as event_dt,
    now() as load_dt,
    's3' as load_src
FROM VT2603040CA38D__STAGING.group_log as gl
LEFT JOIN VT2603040CA38D__DWH.h_groups as hg ON gl.group_id = hg.group_id
LEFT JOIN VT2603040CA38D__DWH.h_users as hu ON gl.user_id = hu.user_id
LEFT JOIN VT2603040CA38D__DWH.l_user_group_activity as luga 
    ON hg.hk_group_id = luga.hk_group_id 
    AND hu.hk_user_id = luga.hk_user_id
WHERE luga.hk_l_user_group_activity IS NOT NULL;