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
),

user_group_messages AS (
    SELECT 
        lgd.hk_group_id,
        COUNT(DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
    FROM VT2603040CA38D__DWH.l_user_message AS lum
    INNER JOIN VT2603040CA38D__DWH.l_groups_dialogs AS lgd 
        ON lum.hk_message_id = lgd.hk_message_id
    WHERE lgd.hk_group_id IN (
        SELECT hk_group_id
        FROM VT2603040CA38D__DWH.h_groups
        ORDER BY registration_dt
        LIMIT 10
    )
    GROUP BY lgd.hk_group_id
)

SELECT 
    ugl.hk_group_id,
    ugl.cnt_added_users,
    COALESCE(ugm.cnt_users_in_group_with_messages, 0) AS cnt_users_in_group_with_messages,
    ROUND(COALESCE(ugm.cnt_users_in_group_with_messages, 0) * 1.0 / NULLIF(ugl.cnt_added_users, 0), 3) AS group_conversion
FROM user_group_log AS ugl
LEFT JOIN user_group_messages AS ugm ON ugl.hk_group_id = ugm.hk_group_id
ORDER BY group_conversion DESC;