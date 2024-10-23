WITH reg AS (
 SELECT DISTINCT
       DATE(mbr.join_time) AS join_dt,
       member_detail_id
    FROM edw.d_member_detail mbr
   WHERE 1=1
     AND eff_reg_channel LIKE '%WMP%'
    ),
    

initial_purchase AS (
SELECT DISTINCT crm_member_id, 
        date_id AS intial_purchase_dt
 FROM edw.f_member_order_detail
WHERE is_rrp_sales_type = 1
       AND distributor_name <> 'LBR'
       AND if_eff_order_tag IS TRUE
       AND initial_vs_repurchase_lifecycle = '会员生命周期首单'
 
),

days_to_initial_convert AS (
SELECT  DISTINCT
       CASE WHEN extract('year' from join_dt) <= 2022 THEN '2022及以前' ELSE CAST(extract('year' from join_dt) AS text) END AS reg_year ,
       join_dt,
       member_detail_id,
       intial_purchase_dt,
       intial_purchase_dt - join_dt 
  FROM reg
  LEFT JOIN initial_purchase
         ON reg.member_detail_id::integer = initial_purchase.crm_member_id::integer
         )
         
    SELECT  reg_year,
            CASE WHEN intial_purchase_dt - join_dt = 0 THEN '1 - 当天转化' 
                 WHEN intial_purchase_dt - join_dt <= 7 AND intial_purchase_dt - join_dt > 0 THEN '2 - 1-7天转化'
                 WHEN intial_purchase_dt - join_dt <= 14 AND intial_purchase_dt - join_dt > 7 THEN '3 - 8-14天转化'
                 WHEN intial_purchase_dt - join_dt <= 30 AND intial_purchase_dt - join_dt > 14 THEN '4 - 15-30'
                 WHEN intial_purchase_dt - join_dt <= 90 AND intial_purchase_dt - join_dt > 30 THEN '5 - 31-90'
                 WHEN intial_purchase_dt - join_dt <= 180 AND intial_purchase_dt - join_dt > 90 THEN '6 - 91-180'
                 WHEN intial_purchase_dt - join_dt <= 360 AND intial_purchase_dt - join_dt > 180 THEN '7 - 181-360'
                 WHEN intial_purchase_dt - join_dt > 361 THEN '8 - 361+'
            ELSE '未转化' END AS days_to_0_1_convert,
            COUNT(DISTINCT member_detail_id)
      FROM days_to_initial_convert
      WHERE 1 = 1
      GROUP BY 1,2;