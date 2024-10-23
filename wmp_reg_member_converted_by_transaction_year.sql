delete from tutorial.mz_wmp_reg_member_converted_by_transaction_year;  -- for the subsequent update
insert into tutorial.mz_wmp_reg_member_converted_by_transaction_year

WITH trans AS (
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
and source_channel in ('LCS', 'TMALL', 'DOUYIN', 'DOUYIN_B2B')
and date(tr.order_paid_date) < current_date
and ((tr.source_channel = 'LCS' and sales_type <> 3) or (tr.source_channel in ('TMALL', 'DOUYIN', 'DOUYIN_B2B') and tr.order_type = 'normal')) -- specific filtering for LCS, TM and DY
),


wmp_reg_member_converted_by_transaction_year AS (
SELECT extract('year' FROM order_paid_date)                                                                                                      AS transaction_year,
       CASE WHEN source_channel IN ( 'DOUYIN', 'DOUYIN_B2B') THEN 'DOUYIN' ELSE source_channel END                                               AS source_channel,
    --   CASE WHEN extract('year' from DATE(join_time)) <= 2022 THEN '2022 and before' ELSE CAST(extract('year' from DATE(join_time)) AS TEXT) END AS reg_year,
       COUNT(DISTINCT trans.omni_channel_member_id)                                                                                              AS member_shopper,
       sum(case when sales_qty > 0 and is_member_order = true then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 and is_member_order = true then abs(order_rrp_amt) else 0 end) as member_sales
FROM trans
LEFT JOIN (SELECT member_detail_id,
                  join_time
             FROM edw.d_member_detail 
             WHERE eff_reg_channel = 'BRANDWMP' 
          ) mbr
        ON trans.omni_channel_member_id::text = mbr.member_detail_id::text
WHERE omni_channel_member_id::text IN (SELECT member_detail_id::text FROM edw.d_member_detail WHERE eff_reg_channel = 'BRANDWMP')
 GROUP BY 1,2
UNION ALL
 SELECT extract('year' FROM order_paid_date) AS transaction_year, 
       'Omni' AS source_channel,
    --   CASE WHEN extract('year' from DATE(join_time)) <= 2022 THEN '2022 and before' ELSE CAST(extract('year' from DATE(join_time)) AS TEXT) END AS reg_year,
       COUNT(DISTINCT trans.omni_channel_member_id)                                                                                              AS member_shopper,
       sum(case when sales_qty > 0 and is_member_order = true then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 and is_member_order = true then abs(order_rrp_amt) else 0 end) as member_sales
FROM trans
LEFT JOIN (SELECT member_detail_id,
                  join_time
             FROM edw.d_member_detail 
             WHERE eff_reg_channel = 'BRANDWMP' 
          ) mbr
        ON trans.omni_channel_member_id::text = mbr.member_detail_id::text
WHERE omni_channel_member_id::text IN (SELECT member_detail_id::text FROM edw.d_member_detail WHERE eff_reg_channel = 'BRANDWMP')  --- 只看WMP注册的人
 GROUP BY 1,2
 )
 
 
 
 
SELECT wmp_converted.transaction_year,
       wmp_converted.source_channel,
       wmp_converted.member_shopper,
       wmp_converted.member_sales,
       CASE WHEN ttl_sales_by_platform.total_sales IS NOT NULL THEN ttl_sales_by_platform.total_sales ELSE ttl_sales_omni.total_sales END AS YTD_platform_TTL,
       CAST(wmp_converted.member_sales AS FLOAT)/(CASE WHEN ttl_sales_by_platform.total_sales IS NOT NULL THEN ttl_sales_by_platform.total_sales ELSE ttl_sales_omni.total_sales END) AS sales_share_of_YTD_platform_TTL
FROM wmp_reg_member_converted_by_transaction_year wmp_converted
LEFT JOIN (
             select 
                      extract('year' FROM date(tr.order_paid_date)) as transaction_year,
                      CASE WHEN source_channel IN ( 'DOUYIN', 'DOUYIN_B2B') THEN 'DOUYIN' ELSE source_channel END                                               AS source_channel,
                      sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end) as total_sales
            from edw.f_omni_channel_order_detail as tr
            where 1 = 1
            and source_channel in ('LCS', 'TMALL', 'DOUYIN', 'DOUYIN_B2B')
            and date(tr.order_paid_date) < current_date
            and ((tr.source_channel = 'LCS' and sales_type <> 3) or (tr.source_channel in ('TMALL', 'DOUYIN', 'DOUYIN_B2B') and tr.order_type = 'normal')) -- specific filtering for LCS, TM and DY
           GROUP BY 1,2
           ) ttl_sales_by_platform
         ON wmp_converted.transaction_year = ttl_sales_by_platform.transaction_year
        AND wmp_converted.source_channel = ttl_sales_by_platform.source_channel
LEFT JOIN (
             select 
                      extract('year' FROM date(tr.order_paid_date)) as transaction_year,
                      'Omni'                                                                                                                       AS source_channel,
                      sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end) as total_sales
            from edw.f_omni_channel_order_detail as tr
            where 1 = 1
            and source_channel in ('LCS', 'TMALL', 'DOUYIN', 'DOUYIN_B2B')
            and date(tr.order_paid_date) < current_date
            and ((tr.source_channel = 'LCS' and sales_type <> 3) or (tr.source_channel in ('TMALL', 'DOUYIN', 'DOUYIN_B2B') and tr.order_type = 'normal')) -- specific filtering for LCS, TM and DY
           GROUP BY 1,2
           ) ttl_sales_omni
         ON wmp_converted.transaction_year = ttl_sales_omni.transaction_year
        AND wmp_converted.source_channel = ttl_sales_omni.source_channel ;
 