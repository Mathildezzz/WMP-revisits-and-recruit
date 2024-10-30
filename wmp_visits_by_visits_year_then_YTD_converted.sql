delete from tutorial.mz_wmp_visits_by_visits_year_then_YTD_converted;  -- for the subsequent update
insert into tutorial.mz_wmp_visits_by_visits_year_then_YTD_converted


with mz_wmp_visits as (
   SELECT * FROM  tutorial.mz_wmp_member_visits_log
),

if_converted AS (
select DISTINCT
       event.member_detail_id,
       extract('year' FROM DATE(event.event_time)) AS visit_year,
       CASE WHEN YTD_converted.crm_member_id IS NOT NULL THEN 1 ELSE 0 END AS YTD_converted
from mz_wmp_visits event
 LEFT JOIN (SELECT DISTINCT crm_member_id, extract('year' FROM date_id) AS trans_year
                FROM edw.f_member_order_detail
               WHERE is_rrp_sales_type = 1
               AND distributor_name <> 'LBR'
               AND if_eff_order_tag IS TRUE
              ) YTD_converted
          ON event.member_detail_id::integer = YTD_converted.crm_member_id::integer
         AND extract('year' FROM DATE(event.event_time)) = YTD_converted.trans_year  -- YTD converted
)

SELECT visit_year,
       YTD_converted,
       CASE WHEN eff_reg_channel LIKE '%LCS%' THEN 'LCS' ELSE eff_reg_channel END  AS eff_reg_channel,
       COUNT(DISTINCT if_converted.member_detail_id)                               AS ttl_member_count
    FROM if_converted
  LEFT JOIN (SELECT member_detail_id,
                     DATE(join_time) AS join_date, 
                     eff_reg_channel
                FROM edw.d_member_detail
              ) mbr
          ON if_converted.member_detail_id::integer = mbr.member_detail_id::integer
GROUP BY 1,2,3;
