DROP TABLE IF EXISTS VT2603040CA38D__DWH.l_user_group_activity;

CREATE TABLE VT2603040CA38D__DWH.l_user_group_activity
(
    hk_l_user_group_activity BIGINT PRIMARY KEY,
    hk_user_id BIGINT NOT NULL,
    hk_group_id BIGINT NOT NULL,
    load_dt DATETIME,
    load_src VARCHAR(20),
    CONSTRAINT fk_l_user_group_activity_user FOREIGN KEY (hk_user_id) 
        REFERENCES VT2603040CA38D__DWH.h_users (hk_user_id),
    CONSTRAINT fk_l_user_group_activity_group FOREIGN KEY (hk_group_id) 
        REFERENCES VT2603040CA38D__DWH.h_groups (hk_group_id)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);