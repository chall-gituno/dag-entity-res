-- create_blocks_adaptive.sql.j2  (with Stage-3 refinement spliced in)
-- Params:
--   source_table, target_schema, id_col, name_col, domain_col, city_col, country_col
--   max_block_size
--   use_domain, use_name3_country, use_compact5_city, refine_domain_with_country
--   enable_stage3_token  : true/false   -- turn Stage-3 (token-based) on/off
--   stage3_min_token_len : 3            -- min token length to keep
--   stage3_stopwords     : list[string] -- e.g. ["inc","llc","ltd","company","group","global","international","national","community","north","south","east","west","central","american","solutions","systems","the","and","of","blue","green","red","white","black"]
--   enable_stage3_name6  : true/false   -- also add a stricter prefix key (name6|country) in Stage-3

CREATE SCHEMA IF NOT EXISTS er;

-- 0) Base normalization (temp)
CREATE OR REPLACE TEMP TABLE tmp_base AS
SELECT
  company_id::BIGINT                       AS company_id,
  NULLIF(LOWER(TRIM(domain_name)), '')  AS domain_name,
  -- name_compact: lower, strip non-alnum, drop spaces
  REGEXP_REPLACE(REGEXP_REPLACE(LOWER(company_name), '[^a-z0-9\s]', '', 'g'), '\s', '', 'g') AS name_compact,
  -- name_spaces: lower, strip non-alnum, KEEP spaces (for tokenization)
  REGEXP_REPLACE(LOWER(company_name), '[^a-z0-9\s]', '', 'g') AS name_spaces,
  NULLIF(LOWER(TRIM(city)), '')    AS city,
  NULLIF(LOWER(TRIM(final_country)), '') AS country
FROM silver.companies;

-- 1) Stage-1 blocks → tmp_raw1
CREATE OR REPLACE TEMP TABLE tmp_raw1 AS
SELECT company_id, 'domain' AS kind, domain_name AS bkey
FROM tmp_base
WHERE domain_name IS NOT NULL

UNION ALL
SELECT company_id, 'name3_country' AS kind,
       CONCAT(SUBSTR(name_compact,1,3),'|',country) AS bkey
FROM tmp_base
WHERE name_compact IS NOT NULL AND country IS NOT NULL AND LENGTH(name_compact) >= 3

UNION ALL
SELECT company_id, 'compact5_city' AS kind,
       CONCAT(SUBSTR(name_compact,1,5),'|',city) AS bkey
FROM tmp_base
WHERE name_compact IS NOT NULL AND city IS NOT NULL AND LENGTH(name_compact) >= 5;

-- de-dup & size (stage-1)
CREATE OR REPLACE TEMP TABLE tmp_dedup1 AS
SELECT DISTINCT kind, bkey, company_id FROM tmp_raw1;

CREATE OR REPLACE TEMP TABLE tmp_sizes1 AS
SELECT kind, bkey, COUNT(*)::BIGINT AS n
FROM tmp_dedup1 GROUP BY 1,2;

CREATE OR REPLACE TEMP TABLE tmp_kept1 AS
SELECT d.* FROM tmp_dedup1 d JOIN tmp_sizes1 s USING(kind,bkey)
WHERE s.n BETWEEN 2 AND 200;

CREATE OR REPLACE TEMP TABLE tmp_big1 AS
SELECT d.*, s.n FROM tmp_dedup1 d JOIN tmp_sizes1 s USING(kind,bkey)
WHERE s.n > 200;

-- 2) Stage-2 refinement of big₁
CREATE OR REPLACE TEMP TABLE tmp_refined2 AS
-- name3_country → name4_country
SELECT b.company_id, 'name4_country' AS kind,
       CONCAT(SUBSTR(t.name_compact,1,4),'|',t.country) AS bkey
FROM tmp_big1 b
JOIN tmp_base t USING (company_id)
WHERE b.kind = 'name3_country' AND t.country IS NOT NULL AND LENGTH(t.name_compact) >= 4
UNION ALL
-- compact5_city → compact7_city
SELECT b.company_id, 'compact7_city' AS kind,
       CONCAT(SUBSTR(t.name_compact,1,7),'|',t.city) AS bkey
FROM tmp_big1 b
JOIN tmp_base t USING (company_id)
WHERE b.kind = 'compact5_city' AND t.city IS NOT NULL AND LENGTH(t.name_compact) >= 7

UNION ALL
-- domain → domain|country
SELECT b.company_id, 'domain_country' AS kind,
       CONCAT(t.domain_name,'|',t.country) AS bkey
FROM tmp_big1 b
JOIN tmp_base t USING (company_id)
WHERE b.kind = 'domain' AND t.domain_name IS NOT NULL AND t.country IS NOT NULL
;

CREATE OR REPLACE TEMP TABLE tmp_dedup2 AS
SELECT DISTINCT kind, bkey, company_id FROM tmp_refined2;

CREATE OR REPLACE TEMP TABLE tmp_sizes2 AS
SELECT kind, bkey, COUNT(*)::BIGINT AS n
FROM tmp_dedup2 GROUP BY 1,2;

CREATE OR REPLACE TEMP TABLE tmp_kept2 AS
SELECT d.* FROM tmp_dedup2 d JOIN tmp_sizes2 s USING(kind,bkey)
WHERE s.n BETWEEN 2 AND 200;

CREATE OR REPLACE TEMP TABLE tmp_big2 AS
SELECT d.*, s.n FROM tmp_dedup2 d JOIN tmp_sizes2 s USING(kind,bkey)
WHERE s.n > 200;

-- =========================
-- 3) Stage-3 (token-based) refinement of STILL-large Stage-2 blocks
--     Target: name4_country buckets that remain > max_block_size
-- =========================

-- stopwords → temp table
CREATE OR REPLACE TEMP TABLE tmp_stopwords AS
SELECT TRIM(value) AS sw
FROM UNNEST( string_split('inc,llc,ltd,gmbh,corp,co,company,plc,pty,ag,nv,sa,spa,group,global,international,national,community,north,south,east,west,central,american,solutions,systems,services,the,and,of,blue,green,red,white,black', ',') ) AS t(value);

-- candidate set for Stage-3 = still-heavy name4_country records
CREATE OR REPLACE TEMP TABLE tmp_s3_candidates AS
SELECT DISTINCT b.company_id
FROM tmp_big2 b
WHERE b.kind = 'name4_country';

-- explode tokens for those candidates
CREATE OR REPLACE TEMP TABLE tmp_s3_tokens AS
WITH names AS (
  SELECT t.company_id, b.country, b.name_spaces
  FROM tmp_s3_candidates t
  JOIN tmp_base b USING (company_id)
  WHERE b.country IS NOT NULL
),
tok_raw AS (
  SELECT company_id, country, UNNEST(string_split(name_spaces, ' ')) AS tok
  FROM names
),
tok_clean AS (
  SELECT company_id, country, tok
  FROM tok_raw
  WHERE tok <> ''
    AND LENGTH(tok) >= 3
    AND tok NOT IN (SELECT sw FROM tmp_stopwords)
)
SELECT company_id, country, tok           -- 👈 consume the CTEs
FROM tok_clean
; 
-- compute token document frequency per country (df)
CREATE OR REPLACE TEMP TABLE tmp_s3_tok_df AS
SELECT country, tok, COUNT(*) AS df
FROM tmp_s3_tokens
GROUP BY 1,2;

-- pick the rarest informative token per (company, country)
CREATE OR REPLACE TEMP TABLE tmp_s3_pick AS
SELECT tc.company_id, tc.country, tc.tok,
       ROW_NUMBER() OVER (PARTITION BY tc.company_id ORDER BY d.df ASC, tc.tok) AS rn
FROM tmp_s3_tokens tc
JOIN tmp_s3_tok_df d USING (country, tok);

-- Companies that still have NO acceptable token after stopword filtering
CREATE OR REPLACE TEMP TABLE tmp_s3_no_token AS
SELECT c.company_id
FROM tmp_s3_candidates c
LEFT JOIN tmp_s3_pick p ON p.company_id = c.company_id AND p.rn = 1
WHERE p.company_id IS NULL;
-- Fallback 1: longer prefix with country (name8|country)
CREATE OR REPLACE TEMP TABLE tmp_refined3_fallback_name AS
SELECT
  nt.company_id,
  'name8_country' AS kind,
  CONCAT(SUBSTR(b.name_compact, 1, 8), '|', b.country) AS bkey
FROM tmp_s3_no_token nt
JOIN tmp_base b USING (company_id)
WHERE b.country IS NOT NULL AND LENGTH(b.name_compact) >= 8;

-- Fallback 2 (optional): prefix + city (name6|city) to split US-wide blobs by locality
CREATE OR REPLACE TEMP TABLE tmp_refined3_fallback_geo AS
SELECT
  nt.company_id,
  'name6_city' AS kind,
  CONCAT(SUBSTR(b.name_compact, 1, 6), '|', b.city) AS bkey
FROM tmp_s3_no_token nt
JOIN tmp_base b USING (company_id)
WHERE b.city IS NOT NULL AND LENGTH(b.name_compact) >= 6;

-- build Stage-3 refined blocks:
CREATE OR REPLACE TEMP TABLE tmp_refined3_tok AS
SELECT
  p.company_id,
  'token_country' AS kind,
  CONCAT(p.tok,'|',p.country) AS bkey
FROM tmp_s3_pick p
WHERE p.rn = 1;


-- optional stricter prefix alongside tokens: name6|country
CREATE OR REPLACE TEMP TABLE tmp_refined3_name6 AS
SELECT
  c.company_id,
  'name6_country' AS kind,
  CONCAT(SUBSTR(b.name_compact,1,6),'|',b.country) AS bkey
FROM tmp_s3_candidates c
JOIN tmp_base b USING (company_id)
WHERE b.country IS NOT NULL AND LENGTH(b.name_compact) >= 6;


-- union Stage-3 refinements
CREATE OR REPLACE TEMP TABLE tmp_refined3 AS
SELECT * FROM tmp_refined3_tok
UNION ALL
SELECT * FROM tmp_refined3_fallback_name
UNION ALL
SELECT * FROM tmp_refined3_fallback_geo
{% if enable_stage3_name6 %}
UNION ALL
SELECT * FROM tmp_refined3_name6
{% endif %};
;

-- size & keep Stage-3
CREATE OR REPLACE TEMP TABLE tmp_dedup3 AS
SELECT DISTINCT kind, bkey, company_id FROM tmp_refined3;

CREATE OR REPLACE TEMP TABLE tmp_sizes3 AS
SELECT kind, bkey, COUNT(*)::BIGINT AS n
FROM tmp_dedup3 GROUP BY 1,2;

CREATE OR REPLACE TEMP TABLE tmp_kept3 AS
SELECT d.* FROM tmp_dedup3 d JOIN tmp_sizes3 s USING(kind,bkey)
WHERE s.n BETWEEN 2 AND 200;

CREATE OR REPLACE TEMP TABLE tmp_big3 AS
SELECT d.*, s.n FROM tmp_dedup3 d JOIN tmp_sizes3 s USING(kind,bkey)
WHERE s.n > 200;


-- 4) Final outputs
CREATE OR REPLACE TABLE er.blocks_adaptive AS
SELECT * FROM tmp_kept1
UNION ALL SELECT * FROM tmp_kept2
UNION ALL SELECT * FROM tmp_kept3;

CREATE OR REPLACE TABLE er.blocks_adaptive_heavy AS
SELECT * FROM tmp_big2
UNION ALL SELECT * FROM tmp_big3;

CREATE OR REPLACE TABLE er.block_refinement_log AS
SELECT 'stage1' AS stage, kind, bkey, n FROM tmp_sizes1 WHERE n > 200
UNION ALL
SELECT 'stage2' AS stage, kind, bkey, n FROM tmp_sizes2 WHERE n > 200

UNION ALL
SELECT 'stage3' AS stage, kind, bkey, n FROM tmp_sizes3 WHERE n > 200
;