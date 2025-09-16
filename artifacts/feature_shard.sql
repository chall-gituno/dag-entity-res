-- pair_features_shard.sql.j2
-- Params:
--   shard_index      : 0..(shard_modulus-1)
--   shard_modulus    : e.g. 64
--   pairs_table      : er.blocking_pairs
--   companies_table  : silver.companies
-- Notes:
--   We compute only DB-cheap features here; heavier NLP can be done later in Python.

WITH pairs_shard AS (
  SELECT *
  FROM er.blocking_pairs
  WHERE ((hash(company_id_a) & 9223372036854775807) % 64) = 11
),

norm AS (
  SELECT
    c.company_id,
    -- normalized strings
    LOWER(TRIM(c.company_name))                                AS name_l,
    REGEXP_REPLACE(LOWER(c.company_name), '[^a-z0-9]+', '', 'g') AS name_compact,
    LOWER(TRIM(c.domain_name))                                 AS domain_l,
    LOWER(TRIM(c.final_country))                               AS country_l,
    LOWER(TRIM(c.city))                                        AS city_l,
    -- numeric
    c.current_employee_estimate                                AS emp_cur,
    c.total_employee_estimate                                  AS emp_tot
  FROM silver.companies c
),

feat AS (
  SELECT
    p.company_id_a,
    p.company_id_b,

    -- basic string-equality matches (booleans as INT)
    (na.domain_l  IS NOT NULL AND na.domain_l  = nb.domain_l )::INT AS domain_exact,
    (na.country_l IS NOT NULL AND na.country_l = nb.country_l)::INT AS country_exact,
    (na.city_l    IS NOT NULL AND na.city_l    = nb.city_l   )::INT AS city_exact,
    (na.name_l    IS NOT NULL AND na.name_l    = nb.name_l   )::INT AS name_exact,
    (na.name_compact IS NOT NULL AND na.name_compact = nb.name_compact)::INT AS name_compact_exact,

    -- partial “prefix” heuristics (cheap, helps variants)
    CASE
      WHEN na.name_compact IS NOT NULL AND nb.name_compact IS NOT NULL
           AND LEFT(na.name_compact, 4) = LEFT(nb.name_compact, 4)
        THEN 1 ELSE 0
    END AS name_prefix4_eq,
    CASE
      WHEN na.name_compact IS NOT NULL AND nb.name_compact IS NOT NULL
           AND LEFT(na.name_compact, 6) = LEFT(nb.name_compact, 6)
        THEN 1 ELSE 0
    END AS name_prefix6_eq,

    -- numeric diffs (null-safe)
    ABS(COALESCE(na.emp_cur, 0) - COALESCE(nb.emp_cur, 0))                AS emp_cur_absdiff,
    CASE
      WHEN GREATEST(COALESCE(na.emp_cur, 0), COALESCE(nb.emp_cur, 0)) > 0
        THEN ABS(COALESCE(na.emp_cur, 0) - COALESCE(nb.emp_cur, 0))
             ::DOUBLE
             / GREATEST(COALESCE(na.emp_cur, 0), COALESCE(nb.emp_cur, 0))
      ELSE NULL
    END AS emp_cur_reldiff,

    ABS(COALESCE(na.emp_tot, 0) - COALESCE(nb.emp_tot, 0))                AS emp_tot_absdiff,
    CASE
      WHEN GREATEST(COALESCE(na.emp_tot, 0), COALESCE(nb.emp_tot, 0)) > 0
        THEN ABS(COALESCE(na.emp_tot, 0) - COALESCE(nb.emp_tot, 0))
             ::DOUBLE
             / GREATEST(COALESCE(na.emp_tot, 0), COALESCE(nb.emp_tot, 0))
      ELSE NULL
    END AS emp_tot_reldiff,

    -- simple completeness indicators
    (na.domain_l IS NOT NULL AND nb.domain_l IS NOT NULL)::INT   AS both_have_domain,
    (na.name_l   IS NOT NULL AND nb.name_l   IS NOT NULL)::INT   AS both_have_name,

    -- convenience for downstream
    na.country_l AS country_a,
    nb.country_l AS country_b,
    na.city_l    AS city_a,
    nb.city_l    AS city_b
  FROM pairs_shard p
  JOIN norm na ON na.company_id = p.company_id_a
  JOIN norm nb ON nb.company_id = p.company_id_b
)

SELECT * FROM feat;