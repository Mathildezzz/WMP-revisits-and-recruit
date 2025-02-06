delete from tutorial.mz_wos_converted_through_wmp_by_transaction_year;  -- for the subsequent update
insert into tutorial.mz_wos_converted_through_wmp_by_transaction_year

WITH omni_trans_fact as
    ( 
        select
        order_paid_time,
        date(tr.order_paid_date) as order_paid_date,
        kyid,
        case
        when tr.type_name in ('CRM_memberid', 'DY_openid', 'TMALL_kyid') then coalesce(cast(mbr.id as varchar), cast(tr.type_value as varchar))
        else null end as omni_channel_member_id, -- 优先取member_detail_id，缺失情况下再取渠道内部id
        cast(mbr.id as varchar) AS member_detail_id,
        mbr_reg.eff_reg_channel,
        tr.parent_order_id,
        tr.lego_sku_id,
        -----------------------------
       tr.cn_line,

        --------------------------
        tr.sales_qty, -- 用于为LCS判断正负单
        tr.if_eff_order_tag, -- 该字段仅对LCS有true / false之分，对于其余渠道均为true
        tr.is_member_order,
        tr.order_rrp_amt,
        payment_type,
        orders.order_scenario
    FROM edw.f_omni_channel_order_detail tr
LEFT JOIN edw.f_crm_member_detail as mbr
       on cast(tr.crm_member_detail_id as varchar) = cast(mbr.member_id as varchar)
LEFT JOIN edw.d_member_detail mbr_reg
       ON  cast(tr.crm_member_detail_id as int) = cast(mbr_reg.member_detail_id AS INT)
LEFT JOIN (SELECT DISTINCT parent_order_id, order_scenario FROM dm_view.offline_lcs_cs__by_sku_fnl) orders
       ON tr.original_order_id = orders.parent_order_id
    WHERE 1 = 1
      and source_channel in ('LCS')
      and date(tr.order_paid_date) < current_date
      and ((tr.source_channel = 'LCS' and sales_type <> 3) or (tr.source_channel in ('TMALL', 'DOUYIN', 'DOUYIN_B2B') and tr.order_type = 'normal')) -- specific filtering for LCS, TM and DY
    )
    
select  
     extract('year' FROM order_paid_date)                                                                                                                                                                                 AS transaction_year,
     NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)),0)                                                                                   AS wos_member_shopper_ttl,
     CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)                AS wos_member_sales_ttl,
     CAST((sum(case when sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)                                                                        AS wos_sales_ttl,
     CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end),0)                  AS wos_member_atv_ttl,
     CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.parent_order_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true then trans.member_detail_id else null end)) ,0)                                                                          AS wos_member_frequency_ttl,
     
     NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND order_scenario = 'WeCom' then trans.member_detail_id else null end)),0)                                                                                                                AS wos_member_shopper_from_wmp,
     CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 AND order_scenario = 'WeCom' then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 AND order_scenario = 'WeCom' then abs(order_rrp_amt) else 0 end)) AS FLOAT)                AS wos_member_sales_from_wmp,
     CAST((sum(case when sales_qty > 0 AND order_scenario = 'WeCom' then order_rrp_amt else 0 end) - sum(case when sales_qty < 0 AND order_scenario = 'WeCom' then abs(order_rrp_amt) else 0 end)) AS FLOAT)                                                                        AS wos_ttl_sales_from_wmp,
     CAST((sum(case when is_member_order IS TRUE AND sales_qty > 0 AND order_scenario = 'WeCom' then order_rrp_amt else 0 end) - sum(case when is_member_order IS TRUE AND sales_qty < 0 AND order_scenario = 'WeCom' then abs(order_rrp_amt) else 0 end)) AS FLOAT)/NULLIF(count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true AND order_scenario = 'WeCom' then trans.parent_order_id else null end),0)   AS wos_member_atv_from_wmp,
     CAST((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true AND order_scenario = 'WeCom' then trans.parent_order_id else null end)) AS FLOAT) / NULLIF((count(distinct case when is_member_order IS TRUE AND if_eff_order_tag = true AND order_scenario = 'WeCom' then trans.member_detail_id else null end)) ,0)                                                                     AS wos_member_frequency_from_wmp,
     
     NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND order_scenario = 'WeCom' AND eff_reg_channel = 'BRANDWMP' then trans.member_detail_id else null end)),0)                           AS wos_member_shopper_wmp_reg,
     NULLIF((count(distinct case when is_member_order = TRUE AND if_eff_order_tag = true AND order_scenario = 'WeCom' AND eff_reg_channel LIKE '%LCS%' then trans.member_detail_id else null end)),0)                           AS wos_member_shopper_lcs_reg

from omni_trans_fact trans
where 1 = 1
and payment_type = 'WOS企微小程序-新场景'
group by 1;