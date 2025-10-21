-- IDs in companies but never in any pair (missed coverage)
WITH pair_ids AS (
  SELECT company_id_a AS id FROM er.block_pairs
  UNION
  SELECT company_id_b       FROM er.block_pairs
)
SELECT COUNT(*) AS missing_in_pairs
FROM silver.companies c
LEFT JOIN pair_ids p ON p.id = c.company_id
WHERE p.id IS NULL;

-- IDs in pairs that don't exist in companies (orphans → bad!)
WITH pair_ids AS (
  SELECT company_id_a AS id FROM er.block_pairs
  UNION
  SELECT company_id_b       FROM er.block_pairs
)
SELECT COUNT(*) AS orphan_in_pairs
FROM pair_ids p
LEFT JOIN silver.companies c ON c.company_id = p.id
WHERE c.company_id IS NULL;