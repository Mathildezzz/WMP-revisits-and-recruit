delete from tutorial.mz_wmp_visits_by_visits_year_then_YTD_converted;  -- for the subsequent update
insert into tutorial.mz_wmp_visits_by_visits_year_then_YTD_converted

with mz_wmp_visits as (
   SELECT * FROM  tutorial.mz_wmp_member_visits_log
),

if_converted AS (
select DISTINCT
       event.member_detail_id,
       eff_reg_channel,
       MAX(CASE WHEN YTD_converted.crm_member_id IS NOT NULL THEN 1 ELSE 0 END) AS YTD_converted
from mz_wmp_visits event
 LEFT JOIN (SELECT DISTINCT crm_member_id, date_id
                FROM edw.f_member_order_detail
               WHERE is_rrp_sales_type = 1
               AND distributor_name <> 'LBR'
               AND if_eff_order_tag IS TRUE
              ) YTD_converted
          ON event.member_detail_id::integer = YTD_converted.crm_member_id::integer
         AND extract('year' FROM DATE(event.event_time)) = extract('year' FROM YTD_converted.date_id)  -- YTD converted
  LEFT JOIN (SELECT member_detail_id,
                     DATE(join_time) AS join_date, 
                     eff_reg_channel
                FROM edw.d_member_detail
              ) mbr
          ON event.member_detail_id::integer = mbr.member_detail_id::integer
GROUP BY 1,2
)

SELECT extract('year' FROM DATE(event.event_time))                                  AS visit_year,
        if_converted.YTD_converted                                                  AS YTD_converted,
         CASE WHEN eff_reg_channel LIKE '%LCS%' THEN 'LCS' ELSE eff_reg_channel END AS eff_reg_channel,
       COUNT(DISTINCT event.member_detail_id)                                       AS ttl_member_count
  from mz_wmp_visits event
LEFT JOIN if_converted                      
        ON CAST(event.member_detail_id AS BIGINT) = CAST(if_converted.member_detail_id AS BIGINT)  
GROUP BY 1,2,3;



