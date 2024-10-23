delete from tutorial.mz_wmp_revisit_member_count;  -- for the subsequent update
insert into tutorial.mz_wmp_revisit_member_count

with mz_wmp_visits as (
SELECT * FROM  tutorial.mz_wmp_member_visits_log
),

visit_log AS (
SELECT DISTINCT 
       wmp_visits.member_detail_id,
       extract('year' from join_date) AS join_year,
       CASE WHEN eff_reg_channel LIKE '%LCS%' THEN 'LCS' ELSE eff_reg_channel END AS eff_reg_channel,
       DATE(event_time) AS event_date
   FROM mz_wmp_visits wmp_visits
   LEFT JOIN (SELECT member_detail_id,
                     DATE(join_time) AS join_date,
                     eff_reg_channel
                FROM edw.d_member_detail
              ) mbr
          ON wmp_visits.member_detail_id::integer = mbr.member_detail_id::integer
),
        
visit_count AS (
SELECT  
       member_detail_id,
       extract('year' FROM event_date) AS visit_year,
       join_year,
       eff_reg_channel,
       COUNT(DISTINCT event_date) AS visit_days
   FROM visit_log
   GROUP BY 1,2,3,4
   )
    
SELECT visit_year,
       CASE WHEN join_year <= 2022 THEN '2022 及以前' ELSE CAST(join_year AS text) END AS join_year,
       eff_reg_channel,
       COUNT(DISTINCT member_detail_id)                                              AS visit_member_count,
       COUNT(DISTINCT CASE WHEN visit_days >= 2 THEN member_detail_id ELSE NULL END) AS revisit_member_count
FROM visit_count
  GROUP BY 1,2,3;