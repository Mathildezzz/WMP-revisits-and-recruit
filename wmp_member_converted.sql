delete from tutorial.mz_wmp_member_converted;  -- for the subsequent update
insert into tutorial.mz_wmp_member_converted

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
)

SELECT 
      CASE WHEN extract('year' from DATE(join_time)) <= 2022 THEN '2022 and before' ELSE CAST(extract('year' from DATE(join_time)) AS TEXT) END AS reg_year,
      COUNT(DISTINCT mbr.member_detail_id)                                                                                                      AS reg_member,
      COUNT(DISTINCT ltd_omni_converted.omni_channel_member_id)                                                                                 AS ltd_omni_converted_member,
      COUNT(DISTINCT ytd_omni_converted.omni_channel_member_id)                                                                                 AS ytd_omni_converted_member,
      COUNT(DISTINCT CASE WHEN ytd_omni_converted.source_channel = 'LCS' THEN ytd_omni_converted.omni_channel_member_id ELSE NULL END)                             AS ytd_lcs_converted_member,
      COUNT(DISTINCT CASE WHEN ytd_omni_converted.source_channel = 'TMALL' THEN ytd_omni_converted.omni_channel_member_id ELSE NULL END)                           AS ytd_tmall_converted_member,
      COUNT(DISTINCT CASE WHEN ytd_omni_converted.source_channel IN ('DOUYIN', 'DOUYIN_B2B') THEN ytd_omni_converted.omni_channel_member_id ELSE NULL END)         AS ytd_dy_converted_member,
       
      COUNT(DISTINCT CASE WHEN wecom.crm_member_id IS NOT NULL THEN mbr.member_detail_id ELSE NULL END)                                                                        AS add_wecom_member,
      COUNT(DISTINCT CASE WHEN wecom.crm_member_id IS NOT NULL AND ytd_omni_converted.source_channel = 'LCS' THEN ytd_omni_converted.omni_channel_member_id ELSE NULL END)     AS add_wecom_and_ytd_lcs_converted,
       
      COUNT(DISTINCT CASE WHEN wecom.crm_member_id IS NULL THEN mbr.member_detail_id ELSE NULL END)                                                                            AS not_add_wecom_member,
      COUNT(DISTINCT CASE WHEN wecom.crm_member_id IS NULL AND ytd_omni_converted.source_channel = 'LCS' THEN ytd_omni_converted.omni_channel_member_id ELSE NULL END)         AS not_add_wecom_but_ytd_lcs_converted
FROM edw.d_member_detail mbr
 LEFT JOIN (        
            select DISTINCT
                case when tr.type_name in ('CRM_memberid', 'DY_openid', 'TMALL_kyid') then coalesce(cast(mbr.id as varchar), cast(tr.type_value as varchar)) else null end as omni_channel_member_id -- 优先取member_detail_id，缺失情况下再取渠道内部id
                from edw.f_omni_channel_order_detail as tr
            left join edw.f_crm_member_detail as mbr
                  on cast(tr.crm_member_detail_id as varchar) = cast(mbr.member_id as varchar)
                where 1 = 1
                and source_channel in ('LCS', 'TMALL', 'DOUYIN', 'DOUYIN_B2B')
                and date(tr.order_paid_date) < current_date
                and ((tr.source_channel = 'LCS' and sales_type <> 3) or (tr.source_channel in ('TMALL', 'DOUYIN', 'DOUYIN_B2B') and tr.order_type = 'normal')) -- specific filtering for LCS, TM and DY
                and if_eff_order_tag = TRUE
          ) ltd_omni_converted
        ON mbr.member_detail_id::text = ltd_omni_converted.omni_channel_member_id::text
 LEFT JOIN (        
            select DISTINCT
                date(tr.order_paid_date) as order_paid_date,
                source_channel,
                case when tr.type_name in ('CRM_memberid', 'DY_openid', 'TMALL_kyid') then coalesce(cast(mbr.id as varchar), cast(tr.type_value as varchar)) else null end as omni_channel_member_id -- 优先取member_detail_id，缺失情况下再取渠道内部id
                from edw.f_omni_channel_order_detail as tr
                 left join edw.f_crm_member_detail as mbr
                  on cast(tr.crm_member_detail_id as varchar) = cast(mbr.member_id as varchar)
              where 1 = 1
                and source_channel in ('LCS', 'TMALL', 'DOUYIN', 'DOUYIN_B2B')
                and date(tr.order_paid_date) < current_date
                and ((tr.source_channel = 'LCS' and sales_type <> 3) or (tr.source_channel in ('TMALL', 'DOUYIN', 'DOUYIN_B2B') and tr.order_type = 'normal')) -- specific filtering for LCS, TM and DY
                and if_eff_order_tag = TRUE
          ) ytd_omni_converted
        ON mbr.member_detail_id::text = ytd_omni_converted.omni_channel_member_id::text
      AND extract('year' FROM mbr.join_time) = extract('year' FROM ytd_omni_converted.order_paid_date)
 LEFT JOIN wecom
        ON mbr.member_detail_id::integer = wecom.crm_member_id::integer
 WHERE eff_reg_channel  = 'BRANDWMP'
 GROUP BY 1;