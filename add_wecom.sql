delete from tutorial.mz_add_wecom;  -- for the subsequent update
insert into tutorial.mz_add_wecom

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
            and crm_member_id IS NOT NULL
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
)

SELECT  extract('year' FROM DATE(event_time)) AS add_wecom_year,
        CASE WHEN mbr.eff_reg_channel LIKE '%LCS%' THEN 'LCS' ELSE mbr.eff_reg_channel END AS eff_reg_channel,
        CASE WHEN latest_wecom_channel_source = '乐高小程序' THEN 1 ELSE 0 END                                               AS add_wecom_through_wmp,
        
        COUNT(DISTINCT wecom.crm_member_id)                                                                                  AS member_count,
        COUNT(DISTINCT CASE WHEN wecom_current_status.member_detail_id IS NOT NULL THEN wecom.crm_member_id ELSE NULL END)   AS add_wecom_member_still_added,
        COUNT(DISTINCT CASE WHEN ltd_lcs_converted.omni_channel_member_id IS NOT NULL THEN wecom.crm_member_id ELSE NULL END) AS ltd_lcs_converted,
        COUNT(DISTINCT CASE WHEN ytd_lcs_converted.omni_channel_member_id IS NOT NULL THEN wecom.crm_member_id ELSE NULL END) AS ytd_lcs_converted
  FROm wecom
LEFT JOIN wecom_current_status
    ON wecom.crm_member_id::integer = wecom_current_status.member_detail_id::integer
LEFT JOIN (SELECT member_detail_id,
                  join_time,
                  eff_reg_channel
             FROM edw.d_member_detail 
          ) mbr
        ON wecom.crm_member_id::integer = mbr.member_detail_id::integer
 LEFT JOIN (        
            select DISTINCT
                case when tr.type_name in ('CRM_memberid', 'DY_openid', 'TMALL_kyid') then coalesce(cast(mbr.id as varchar), cast(tr.type_value as varchar)) else null end as omni_channel_member_id -- 优先取member_detail_id，缺失情况下再取渠道内部id
                from edw.f_omni_channel_order_detail as tr
            left join edw.f_crm_member_detail as mbr
                  on cast(tr.crm_member_detail_id as varchar) = cast(mbr.member_id as varchar)
                where 1 = 1
                and source_channel in ('LCS')
                and date(tr.order_paid_date) < current_date
                and ((tr.source_channel = 'LCS' and sales_type <> 3) or (tr.source_channel in ('TMALL', 'DOUYIN', 'DOUYIN_B2B') and tr.order_type = 'normal')) -- specific filtering for LCS, TM and DY
                and if_eff_order_tag = TRUE
          ) ltd_lcs_converted
        ON CAST(wecom.crm_member_id::integer AS text) = ltd_lcs_converted.omni_channel_member_id::text
 LEFT JOIN (        
            select DISTINCT
                date(tr.order_paid_date) as order_paid_date,
                case when tr.type_name in ('CRM_memberid', 'DY_openid', 'TMALL_kyid') then coalesce(cast(mbr.id as varchar), cast(tr.type_value as varchar)) else null end as omni_channel_member_id -- 优先取member_detail_id，缺失情况下再取渠道内部id
                from edw.f_omni_channel_order_detail as tr
                 left join edw.f_crm_member_detail as mbr
                  on cast(tr.crm_member_detail_id as varchar) = cast(mbr.member_id as varchar)
               where 1 = 1
                and source_channel in ('LCS')
                and date(tr.order_paid_date) < current_date
                and ((tr.source_channel = 'LCS' and sales_type <> 3) or (tr.source_channel in ('TMALL', 'DOUYIN', 'DOUYIN_B2B') and tr.order_type = 'normal')) -- specific filtering for LCS, TM and DY
                and if_eff_order_tag = TRUE
          ) ytd_lcs_converted
        ON CAST(wecom.crm_member_id::integer AS text) = ytd_lcs_converted.omni_channel_member_id::text
       AND extract('year' FROM DATE(wecom.event_time)) = extract('year' FROM ytd_lcs_converted.order_paid_date)
GROUP BY 1,2,3;
  