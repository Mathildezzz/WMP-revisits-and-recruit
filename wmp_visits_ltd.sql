delete from tutorial.mz_wmp_visits_ltd;  -- for the subsequent update
insert into tutorial.mz_wmp_visits_ltd

WITH reg AS (
SELECT CASE WHEN eff_reg_channel LIKE '%LCS%' THEN 'LCS' ELSE eff_reg_channel END AS eff_reg_channel,
       COUNT(member_detail_id) AS member_count
FROM edw.d_member_detail

GROUP BY 1
),

tmall_historical AS (

      SELECT 'BRANDEC' AS eff_reg_channel,
             COUNT(DISTINCT CASE WHEN member_detail_id IS NULL THEN platform_id_value ELSE NULL END) AS member_count
        FROM edw.d_belong_channel_inc_ec_his_mbr 
      WHERE belong_type = 'registerOrBind'
        AND eff_belong_channel = 'TMall'
),

dy_historical AS (
      SELECT 'DOUYIN' AS eff_reg_channel,
             COUNT(DISTINCT CASE WHEN member_detail_id IS NULL THEN platform_id_value ELSE NULL END) AS member_count
        FROM edw.d_belong_channel_inc_ec_his_mbr 
      WHERE belong_type = 'registerOrBind'
        AND eff_belong_channel = 'Douyin'
),

reg_by_channel AS (
SELECT eff_reg_channel,
       SUM(member_count) AS reg_member_count
FROM (
SELECT * FROM reg
 UNION ALL 
 SELECT * FROM tmall_historical
 UNION ALL
 SELECT * FROM dy_historical
 )
 GROUP BY 1
 ),

wmp_visits as (
SELECT * FROM  tutorial.mz_wmp_member_visits_log
),

wmp_visits_by_channel AS (
SELECT   
       CASE WHEN eff_reg_channel LIKE '%LCS%' THEN 'LCS' ELSE eff_reg_channel END AS eff_reg_channel,
       COUNT(DISTINCT wmp_visits.member_detail_id) AS member_visit_count
   FROM wmp_visits 
   LEFT JOIN (SELECT member_detail_id,
                     DATE(join_time) AS join_date,
                     eff_reg_channel
                FROM edw.d_member_detail
              ) mbr
          ON wmp_visits.member_detail_id::integer = mbr.member_detail_id::integer
GROUP BY 1
)

SELECT reg_by_channel.eff_reg_channel,
       reg_by_channel.reg_member_count,
       wmp_visits_by_channel.member_visit_count
FROM reg_by_channel
LEFT JOIN wmp_visits_by_channel
       ON reg_by_channel.eff_reg_channel = wmp_visits_by_channel.eff_reg_channel;
       
       
      
