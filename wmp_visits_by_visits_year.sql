delete from tutorial.mz_wmp_visits_by_visits_year;  -- for the subsequent update
insert into tutorial.mz_wmp_visits_by_visits_year

with wmp_member_visits as (
SELECT * FROM tutorial.mz_wmp_member_visits_log
)

SELECT    EXTRACT('year' FROM event_time)                                                                                                       AS visit_wmp_year,
          CASE WHEN eff_reg_channel LIKE '%LCS%' THEN 'LCS' ELSE eff_reg_channel END                                                            AS eff_reg_channel,
          CASE WHEN extract('year' FROM join_date) <= 2022 THEN '2022 及以前' ELSE CAST(extract('year' FROM join_date) AS text) END             AS join_year,
          COUNT(DISTINCT wmp_visits.member_detail_id)                                                                                           AS visit_wmp_member_count
   FROM wmp_member_visits wmp_visits
   LEFT JOIN (SELECT member_detail_id,
                     DATE(join_time) AS join_date,
                     eff_reg_channel
                FROM edw.d_member_detail
              ) mbr
          ON wmp_visits.member_detail_id::integer = mbr.member_detail_id::integer
GROUP BY 1,2,3;
          