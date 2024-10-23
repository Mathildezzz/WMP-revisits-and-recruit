delete from tutorial.mz_wmp_avg_visits_by_visits_year;  -- for the subsequent update
insert into tutorial.mz_wmp_avg_visits_by_visits_year

with wmp_member_visits as (
SELECT * FROM  tutorial.mz_wmp_member_visits_log
),


visits_days_by_member AS (
select member_detail_id,
       extract('year' from event.event_time) AS visit_year,
       COUNT(DISTINCT DATE(event_time)) AS visit_day_count
from wmp_member_visits event
GROUP BY 1,2
)

SELECT  
        visit_year,
        CASE WHEN eff_reg_channel LIKE '%LCS%' THEN 'LCS' ELSE eff_reg_channel END                                                AS eff_reg_channel,
        CASE WHEN extract('year' FROM join_date) <= 2022 THEN '2022 及以前' ELSE CAST(extract('year' FROM join_date) AS text) END AS join_year,
        CAST(SUM(visit_day_count) AS FLOAT)                                                                                       AS ttl_visit_days,
        COUNT(DISTINCT wmp_visits.member_detail_id)                                                                               AS visit_member_count,
        CAST(SUM(visit_day_count) AS FLOAT)/COUNT(DISTINCT wmp_visits.member_detail_id)                                           AS avg_visit_days                                    
   FROM visits_days_by_member wmp_visits
   LEFT JOIN (SELECT member_detail_id,
                     DATE(join_time) AS join_date,
                     eff_reg_channel
                FROM edw.d_member_detail
              ) mbr
          ON wmp_visits.member_detail_id::integer = mbr.member_detail_id::integer
GROUP BY 1,2,3;