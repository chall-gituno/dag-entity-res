SELECT
  (SELECT COUNT(*) FROM er.block_pairs) AS total_pairs,
  (SELECT COUNT(DISTINCT company_id_a) FROM er.block_pairs) AS distinct_a,
  (SELECT COUNT(DISTINCT company_id_b) FROM er.block_pairs) AS distinct_b,
  (SELECT COUNT(DISTINCT company_id_a) + COUNT(DISTINCT company_id_b)
     FROM er.block_pairs) AS total_unique_ids,
  (SELECT COUNT(*) - COUNT(DISTINCT (company_id_a, company_id_b))
     FROM er.block_pairs) AS duplicate_pairs;