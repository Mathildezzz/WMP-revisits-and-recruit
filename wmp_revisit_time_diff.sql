delete from tutorial.mz_wmp_revisit_time_diff;  -- for the subsequent update
insert into tutorial.mz_wmp_revisit_time_diff

with mz_wmp_visits as (
SELECT * FROM  tutorial.mz_wmp_member_visits_log
),

visit_log AS (
SELECT DISTINCT 
       wmp_visits.member_detail_id,
       extract('year' from join_date) AS join_year,
       CASE WHEN eff_reg_channel LIKE '%LCS%' THEN 'LCS' ELSE eff_reg_channel END AS eff_reg_channel,
       extract('year' from date(event_time)) AS visit_year,
       DATE(event_time) AS event_date
   FROM mz_wmp_visits wmp_visits
   LEFT JOIN (SELECT member_detail_id,
                     DATE(join_time) AS join_date,
                     eff_reg_channel
                FROM edw.d_member_detail
              ) mbr
          ON wmp_visits.member_detail_id::integer = mbr.member_detail_id::integer
),
        
visit_rank_by_member AS (
SELECT DISTINCT 
       member_detail_id,
       visit_year,
       join_year,
       eff_reg_channel,
       event_date,
       ROW_NUMBER () OVER (PARTITION BY member_detail_id,visit_year ORDER BY event_date ASC) AS visit_rank
   FROM visit_log 
   ),
        
        
        
revisit_log AS (     
SElECT member_detail_id,
       visit_year,
       join_year,
       eff_reg_channel,
       visit_rank,
       event_date,
       LAG(event_date,1) OVER (PARTITION BY member_detail_id,visit_year ORDER BY event_date) AS last_visit_event_date
FROM visit_rank_by_member
WHERE member_detail_id IN (SELECT DISTINCT member_detail_id FROM visit_rank_by_member WHERE visit_rank >=2)
ORDER BY 1,2,3,4,5,6,7
)

SELECT visit_year,
        CASE WHEN join_year <= 2022 THEN '2022 及以前' ELSE CAST(join_year AS text) END AS join_year,
       eff_reg_channel,
       CASE WHEN event_date - last_visit_event_date >= 1 AND event_date - last_visit_event_date <= 7 THEN '1-7 days'
           WHEN event_date - last_visit_event_date >= 8 AND event_date - last_visit_event_date <= 14 THEN '8-14 days'
           WHEN event_date - last_visit_event_date >= 15 AND event_date - last_visit_event_date <= 30 THEN '15-30 days'
           WHEN event_date - last_visit_event_date >= 31 AND event_date - last_visit_event_date <= 60 THEN '31-60 days'
           WHEN event_date - last_visit_event_date >= 61 AND event_date - last_visit_event_date <= 90 THEN '61-90 days'
           WHEN event_date - last_visit_event_date >= 91 AND event_date - last_visit_event_date <= 180 THEN '91-180 days'
           WHEN event_date - last_visit_event_date >= 181 AND event_date - last_visit_event_date <= 360 THEN '181-360 days'
           WHEN event_date - last_visit_event_date >= 361 THEN '> 361 days'
           ELSE NULL END AS time_diff,
       COUNT(DISTINCT member_detail_id)  AS member_count
FROM revisit_log
WHERE last_visit_event_date IS NOT NULL
  AND visit_rank = 2  -- 只看第一次的revisit
  GROUP BY 1,2,3,4;