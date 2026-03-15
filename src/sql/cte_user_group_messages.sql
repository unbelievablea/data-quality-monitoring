WITH user_group_messages AS (
    SELECT 
        luga.hk_group_id,
        COUNT(DISTINCT luga.hk_user_id) AS cnt_users_in_group_with_messages
    FROM VT2603040CA38D__DWH.l_user_message AS lum
    INNER JOIN VT2603040CA38D__DWH.l_groups_dialogs AS lgd 
        ON lum.hk_message_id = lgd.hk_message_id
    INNER JOIN VT2603040CA38D__DWH.l_user_group_activity AS luga 
        ON lgd.hk_group_id = luga.hk_group_id 
        AND lum.hk_user_id = luga.hk_user_id
    GROUP BY luga.hk_group_id
)

SELECT 
    hk_group_id,
    cnt_users_in_group_with_messages
FROM user_group_messages
ORDER BY cnt_users_in_group_with_messages DESC
LIMIT 10;