delete from tutorial.mz_wmp_recruit;  -- for the subsequent update
insert into tutorial.mz_wmp_recruit
WITH wecom AS (
         -- 添加SA，event_key = 'external_user_add'
        select
            a.event_key, 
            timestamp with time zone 'epoch' + a.event_time * interval '1 second' as event_time, 
            json_extract_path_text(a.attributes, 'store_codes', true) as store_codes,
            json_extract_path_text(a.attributes, 'unionId', true) as unionId ,
            json_extract_path_text(a.attributes, 'external_userId', true) as external_userId,
            json_extract_path_text(a.attributes, 'add_way', true) as add_way, 
            json_extract_path_text(a.attributes, 'code_name', true) as code_name,
            b.crm_member_id,
            c.wecom_channel_source as latest_wecom_channel_source -- CDP中的“最近以此添加企微场景”
        from stg.gio_event_local as a
        left join
            (
                select
                    prop_value as crm_member_id,
                    gio_id
                from stg.gio_id_user_account_all
                where 1 = 1
                and prop_key = 'id_CRM_memberid'
            ) as b
            on a.gio_id = b.gio_id 
        left join edw.d_dl_sa_external_user_tag as c
            on json_extract_path_text(a.attributes, 'unionId', true) = c.union_id 
        where 1 = 1
            and a.event_key = 'external_user_add'  
            and timestamp with time zone 'epoch' + a.event_time * interval '1 second' < current_date
),


wecom_current_status AS (
select
    -- shopper attributes
    pr.member_detail_id
    ,rel.external_user_unionid
    
    -- store staff attributes
    ,rel.staff_ext_id
    ,staff.staff_name
    ,staff.lego_store_code
    ,staff.store_name
    ,staff.distributor
    ,staff.partner
    ,staff.region
    ,staff.channel

    -- friend relationship creation attributes
    ,rel.relation_created_at
    ,rel.relation_source as created_type  --0：在职添加，1：继承/内部成员共享

    
    -- friend relationship deletion attributes
    ,rel.relation_deleted
    
from edw.f_sa_staff_external_user_relation_detail rel
left join edw.d_sa_staff_info staff
    on rel.staff_ext_id = staff.staff_ext_id
-- 目前CA金表获取会员ID使用的是edw.f_crm_thirdparty_bind_detail，建议先保持一致，便于与FR看到的统计表对齐。未来会迁移到以下CRM银表：
-- left join edw.f_platform_relationship pr
--     on pr.platform = 'WMP' 
--     and pr.wmp_union_id = rel.external_user_unionid
left join edw.f_crm_thirdparty_bind_detail pr
    on pr.id_type = 'unionId'
    and pr.thirdparty_app_id = 4
    and pr.id_value = rel.external_user_unionid
where 1=1
and staff.lego_store_code is not null
AND relation_deleted = 0
),

CTE AS (
SELECT 
       CASE WHEN extract('year' from DATE(join_time)) <= 2022 THEN '2022 and before' ELSE CAST(extract('year' from DATE(join_time)) AS TEXT) END AS reg_year,
       COUNT(DISTINCT mbr.member_detail_id) AS  reg_member,
       COUNT(DISTINCT CASE WHEN wecom.crm_member_id IS NOT NULL THEN mbr.member_detail_id ELSE NULL END)                     AS add_wecom_member,
       COUNT(DISTINCT CASE WHEN wecom_current_status.member_detail_id IS NOT NULL THEN mbr.member_detail_id ELSE NULL END)   AS add_wecom_member_still_added,
       COUNT(DISTINCT CASE WHEN wecom.latest_wecom_channel_source = '乐高小程序' THEN mbr.member_detail_id ELSE NULL END)    AS add_wecom_through_wmp_member,
       COUNT(DISTINCT CASE WHEN wecom.latest_wecom_channel_source = '乐高小程序' AND wecom_current_status.member_detail_id IS NOT NULL THEN mbr.member_detail_id ELSE NULL END)    AS add_wecom_through_wmp_member_and_still_added
 FROM edw.d_member_detail mbr
 LEFT JOIN wecom
        ON mbr.member_detail_id::integer = wecom.crm_member_id::integer
 LEFT JOIN wecom_current_status
        ON mbr.member_detail_id::integer = wecom_current_status.member_detail_id::integer
 WHERE eff_reg_channel  = 'BRANDWMP'
 GROUP BY 1
 )
 
 SELECT reg_year,
        reg_member,
        CAST(add_wecom_member AS FLOAT)/reg_member AS wecom_added_ratio,
        
        add_wecom_member,
        add_wecom_member_still_added,
        CAST(add_wecom_member_still_added AS FLOAT)/add_wecom_member AS wecom_still_added_ratio,
        add_wecom_through_wmp_member,
        add_wecom_through_wmp_member_and_still_added,
        CAST(add_wecom_through_wmp_member_and_still_added AS FLOAT)/add_wecom_through_wmp_member AS wecom_through_wmp_still_added_ratio
 FROM CTE;
 
