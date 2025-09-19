
          -- SELECT COUNT(*) FROM (
          --   SELECT company_id_a, company_id_b, COUNT(*) AS c
          --   FROM er.pair_features
          --   GROUP BY 1,2
          --   HAVING COUNT(*) > 1
          -- )

-- select count(*) from (
--   select company_id_a,company_id_b
--   from er.blocking_pairs
--   group by 1,2);

--select * from  er.blocking_pairs_by_kind limit 10;
-- WITH g AS (
--   SELECT company_id_a, company_id_b, COUNT(*) c
--   FROM er.blocking_pairs
--   GROUP BY 1,2
-- )
-- SELECT SUM(CASE WHEN c>1 THEN c-1 ELSE 0 END) AS duplicate_rows FROM g;

