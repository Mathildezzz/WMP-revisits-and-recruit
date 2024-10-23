delete from tutorial.mz_wmp_member_visits_log;  -- for the subsequent update
insert into tutorial.mz_wmp_member_visits_log

WITH mapping as (
SELECT gio_id,
       CAST(gio_crm_id_mapping.member_detail_id AS BIGINT) AS member_detail_id,
       mbr.join_date
FROM (
        select
              gio_id
             ,prop_value as member_detail_id
             ,row_number() over ( partition by gio_id order by update_time desc) as rk
         from stg.gio_id_user_account_all 
         where prop_key = 'id_CRM_memberid'
            and sign = 1 
            and prop_value similar to '[0-9]+'  --??????member_id,??
       ) gio_crm_id_mapping
LEFT JOIN (SELECT member_detail_id,
                 DATE(join_time) AS join_date,
                 eff_reg_channel
            FROM edw.d_member_detail
          ) mbr
        ON CAST(gio_crm_id_mapping.member_detail_id AS  BIGINT) = CAST(mbr.member_detail_id AS BIGINT)
where rk = 1
)

select distinct 
    mapping.member_detail_id
    ,event.gio_id
    ,event.event_time
    ,page_type
from ods.wc_page_view event
INNER JOIN mapping
   ON event.gio_id = mapping.gio_id
  AND DATE(event.event_time) >= DATE(mapping.join_date);