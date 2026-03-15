WITH user_group_log AS (
    SELECT 
        luga.hk_group_id,
        COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users
    FROM VT2603040CA38D__DWH.l_user_group_activity AS luga
    INNER JOIN VT2603040CA38D__DWH.s_auth_history AS sah 
        ON luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
    WHERE sah.event = 'add'
        AND luga.hk_group_id IN (
            SELECT hk_group_id
            FROM VT2603040CA38D__DWH.h_groups
            ORDER BY registration_dt
            LIMIT 10
        )
    GROUP BY luga.hk_group_id
)

SELECT 
    hk_group_id,
    cnt_added_users
FROM user_group_log
ORDER BY cnt_added_users DESC;