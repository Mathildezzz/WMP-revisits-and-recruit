delete from tutorial.mz_wmp_visits_count_distribution_by_visits_year;  -- for the subsequent update
insert into tutorial.mz_wmp_visits_count_distribution_by_visits_year

with wmp_member_visits as (
SELECT * FROM  tutorial.mz_wmp_member_visits_log
),


visit_count_by_member AS (

SELECT wmp_member_visits.member_detail_id,
       extract('year' from wmp_member_visits.event_time)                          AS visit_year,
       extract('year' from join_date)                                             AS join_year,
       CASE WHEN eff_reg_channel LIKE '%LCS%' THEN 'LCS' ELSE eff_reg_channel END AS eff_reg_channel ,
       COUNT(DISTiNCT DATE(event_time))                                           AS visit_day_count
   FROM wmp_member_visits 
   LEFT JOIN (SELECT member_detail_id,
                     DATE(join_time) AS join_date,
                     eff_reg_channel
                FROM edw.d_member_detail
              ) mbr
          ON wmp_member_visits.member_detail_id::integer = mbr.member_detail_id::integer
GROUP BY 1,2,3,4
        )
        
SElECT visit_year,
       CASE WHEN join_year <= 2022 THEN '2022 及以前' ELSE join_year::text END AS join_year,
       eff_reg_channel,
       CASE WHEN visit_day_count >= 5 THEN 'greater than 5' ELSE CAST(visit_day_count AS text) END AS visit_day_count,
       COUNT(DISTINCT member_detail_id)
FROM visit_count_by_member
GROUP BY 1,2,3,4;