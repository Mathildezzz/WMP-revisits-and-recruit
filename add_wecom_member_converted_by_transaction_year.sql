delete from tutorial.mz_add_wecom_member_converted_by_transaction_year;  -- for the subsequent update
insert into tutorial.mz_add_wecom_member_converted_by_transaction_year

WITH lcs_trans AS (
 select DISTINCT
          date(tr.order_paid_date) as order_paid_date,
          source_channel,
          case when tr.type_name in ('CRM_memberid', 'DY_openid', 'TMALL_kyid') then coalesce(cast(mbr.id as varchar), cast(tr.type_value as varchar)) else null end as omni_channel_member_id,
          if_eff_order_tag,
          sales_qty,
          is_member_order,
          order_rrp_amt
          -- 优先取member_detail_id，缺失情况下再取渠道内部id
from edw.f_omni_channel_order_detail as tr
 left join edw.f_crm_member_detail as mbr
  on cast(tr.crm_member_detail_id as varchar) = cast(mbr.member_id as varchar)
where 1 = 1
and source_channel in ('LCS') --- 只focus LCS
and date(tr.order_paid_date) < current_date
and ((tr.source_channel = 'LCS' and sales_type <> 3) or (tr.source_channel in ('TMALL', 'DOUYIN', 'DOUYIN_B2B') and tr.order_type = 'normal')) -- specific filtering for LCS, TM and DY
),

 wecom AS (
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
),


add_wecom_member_converted_by_transaction_year AS (

SELECT extract('year' FROM order_paid_date)                                                                                                          AS transaction_year,
       'LCS'                                                                                                                                         AS source_channel,
      CASE WHEN wecom.crm_member_id IS NOT NULL AND wecom_current_status.member_detail_id IS NOT NULL THEN 1 ELSE 0 END                              AS if_still_added_wecom,
      CASE WHEN wecom.crm_member_id IS NOT NULL AND added_wecom_through_wmp.crm_member_id IS NOT NULL THEN 1 ELSE 0 END                              AS if_added_wecom_through_wmp,
       COUNT(DISTINCT lcs_trans.omni_channel_member_id)                                                                                              AS member_shopper,
       sum(case when sales_qty > 0 and is_member_order = true then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 and is_member_order = true then abs(order_rrp_amt) else 0 end) as member_sales
FROM lcs_trans 
INNER JOIN (SELECT crm_member_id, MIN(extract('year' FROM wecom.event_time)) AS min_add_wecom_year FROM wecom GROUP BY 1) wecom -- 只看加过企微的人
      ON lcs_trans.omni_channel_member_id::text = CAST(wecom.crm_member_id::integer AS text)
     AND extract('year' FROM lcs_trans.order_paid_date) >= min_add_wecom_year           -- 确保是transaction之前或同一年加了企微
LEFT JOIN (SELECT DISTINCT crm_member_id FROM wecom WHERE latest_wecom_channel_source = '乐高小程序') added_wecom_through_wmp
      ON lcs_trans.omni_channel_member_id::text = CAST(added_wecom_through_wmp.crm_member_id::integer AS text)
LEFT JOIN (SELECT DISTINCT member_detail_id FROM wecom_current_status) wecom_current_status
    ON lcs_trans.omni_channel_member_id::text = CAST(wecom_current_status.member_detail_id::integer AS text)
 GROUP BY 1,2,3,4
 )

 
 
SELECT add_wecom_converted.transaction_year,
       add_wecom_converted.source_channel,
       add_wecom_converted.if_still_added_wecom,
       add_wecom_converted.if_added_wecom_through_wmp,
       add_wecom_converted.member_shopper,
       add_wecom_converted.member_sales,
       ttl_sales_lcs.total_sales                                                  AS YTD_platform_TTL,
       CAST(add_wecom_converted.member_sales AS FLOAT)/ ttl_sales_lcs.total_sales AS sales_share_of_YTD_platform_TTL
FROM add_wecom_member_converted_by_transaction_year add_wecom_converted
LEFT JOIN (
             select 
                      extract('year' FROM date(tr.order_paid_date)) as transaction_year,
                      CASE WHEN source_channel IN ( 'DOUYIN', 'DOUYIN_B2B') THEN 'DOUYIN' ELSE source_channel END                                               AS source_channel,
                      sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end) as total_sales
            from edw.f_omni_channel_order_detail as tr
            where 1 = 1
            and source_channel in ('LCS')
            and date(tr.order_paid_date) < current_date
            and ((tr.source_channel = 'LCS' and sales_type <> 3) or (tr.source_channel in ('TMALL', 'DOUYIN', 'DOUYIN_B2B') and tr.order_type = 'normal')) -- specific filtering for LCS, TM and DY
           GROUP BY 1,2
           ) ttl_sales_lcs
         ON add_wecom_converted.transaction_year = ttl_sales_lcs.transaction_year
        AND add_wecom_converted.source_channel = ttl_sales_lcs.source_channel;
