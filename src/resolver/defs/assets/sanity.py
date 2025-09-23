# src/resolver/defs/assets/sanity.py
from __future__ import annotations
import os
from pathlib import Path
import dagster as dg
import duckdb


def _conn(ro=True):
  con = duckdb.connect(
    os.getenv("DUCKDB_DATABASE", "/opt/test-data/experimental.duckdb"),
    read_only=ro,
  )
  con.execute("PRAGMA threads=1")
  return con


def _create_or_replace_view(sql: str) -> int:
  con = _conn(ro=False)
  try:
    con.execute(sql)
    # return a cheap row count when possible; ignore if view uses params
    n = con.execute("SELECT 1").fetchone()[0]
    return int(n)
  finally:
    con.close()


@dg.asset(
  name="sanity_pair_counts",
  group_name="sanity",
  deps=[dg.AssetKey("er_pair_features"),
        dg.AssetKey("er_pair_scores")],
  compute_kind="sql",
  description="Counts & alignment across pair_features / pair_scores / scored view.")
def sanity_pair_counts(context) -> dg.MaterializeResult:
  sql = """
    CREATE OR REPLACE VIEW er.v_sanity_pair_counts AS
    WITH a AS (SELECT COUNT(*) AS n FROM er.pair_features),
         b AS (SELECT COUNT(*) AS n FROM er.pair_scores),
         c AS (
           SELECT COUNT(*) AS n, SUM(model_score IS NULL)::BIGINT AS null_scores
           FROM er.v_pair_features_scored
         )
    SELECT a.n AS pair_features,
           b.n AS pair_scores,
           c.n AS pair_features_scored,
           c.null_scores
    FROM a,b,c;
    """
  _create_or_replace_view(sql)
  # surface the numbers into Dagster metadata for quick glance
  con = _conn()
  pf, ps, pfs, nulls = con.execute("SELECT * FROM er.v_sanity_pair_counts").fetchone()
  con.close()
  return dg.MaterializeResult(
    metadata={
      "pair_features": dg.MetadataValue.int(int(pf)),
      "pair_scores": dg.MetadataValue.int(int(ps)),
      "pair_features_scored": dg.MetadataValue.int(int(pfs)),
      "null_scores": dg.MetadataValue.int(int(nulls)),
      "view": dg.MetadataValue.text("er.v_sanity_pair_counts"),
    })


@dg.asset(
  name="sanity_dup_pairs",
  group_name="sanity",
  deps=[dg.AssetKey("er_pair_features")],
  compute_kind="sql",
  description=
  "Duplicate detection for (company_id_a, company_id_b) in pair_features (should be 0)."
)
def sanity_dup_pairs(context) -> dg.MaterializeResult:
  sql = """
    CREATE OR REPLACE VIEW er.v_sanity_dup_pairs AS
    WITH g AS (
      SELECT company_id_a, company_id_b, COUNT(*) AS c
      FROM er.pair_features
      GROUP BY 1,2
      HAVING COUNT(*) > 1
    )
    SELECT COUNT(*) AS duplicate_groups, COALESCE(SUM(c-1),0)::BIGINT AS duplicate_rows
    FROM g;
    """
  _create_or_replace_view(sql)
  con = _conn()
  groups, rows = con.execute(
    "SELECT duplicate_groups, duplicate_rows FROM er.v_sanity_dup_pairs").fetchone()
  con.close()
  return dg.MaterializeResult(
    metadata={
      "duplicate_groups": dg.MetadataValue.int(int(groups)),
      "duplicate_rows": dg.MetadataValue.int(int(rows)),
      "view": dg.MetadataValue.text("er.v_sanity_dup_pairs"),
    })


@dg.asset(
  name="sanity_score_histogram",
  group_name="sanity",
  deps=[dg.AssetKey("er_pair_scores")],
  compute_kind="sql",
  description="20-bucket histogram of model_score to eyeball drift/threshold bands.")
def sanity_score_histogram(context) -> dg.MaterializeResult:
  sql = """
    CREATE OR REPLACE VIEW er.v_sanity_score_histogram AS
    SELECT width_bucket(model_score, 0, 1, 20) AS bucket,
           COUNT(*)::BIGINT AS n
    FROM er.pair_scores
    GROUP BY 1
    ORDER BY 1;
    """
  _create_or_replace_view(sql)
  # pull top few buckets into metadata
  con = _conn()
  rows = con.execute(
    "SELECT bucket, n FROM er.v_sanity_score_histogram ORDER BY bucket").fetchall()
  con.close()
  # keep metadata small
  preview = [{"bucket": int(b), "n": int(n)} for (b, n) in rows[:5]]
  return dg.MaterializeResult(
    metadata={
      "buckets_preview": dg.MetadataValue.json(preview),
      "view": dg.MetadataValue.text("er.v_sanity_score_histogram"),
    })
