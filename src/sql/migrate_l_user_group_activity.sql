INSERT INTO VT2603040CA38D__DWH.l_user_group_activity(
    hk_l_user_group_activity, 
    hk_user_id,
    hk_group_id,
    load_dt,
    load_src
)
SELECT DISTINCT
    hash(hu.hk_user_id, hg.hk_group_id) as hk_l_user_group_activity,
    hu.hk_user_id,
    hg.hk_group_id,
    now() as load_dt,
    's3' as load_src
FROM VT2603040CA38D__STAGING.group_log as gl
LEFT JOIN VT2603040CA38D__DWH.h_users as hu ON gl.user_id = hu.user_id
LEFT JOIN VT2603040CA38D__DWH.h_groups as hg ON gl.group_id = hg.group_id
WHERE hu.hk_user_id IS NOT NULL 
  AND hg.hk_group_id IS NOT NULL
  AND hash(hu.hk_user_id, hg.hk_group_id) NOT IN (
      SELECT hk_l_user_group_activity 
      FROM VT2603040CA38D__DWH.l_user_group_activity
  );