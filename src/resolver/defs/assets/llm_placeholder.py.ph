from __future__ import annotations
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue, AssetIn, AssetKey
import duckdb
import json
from typing import List, Dict, Any


# -----------------------------
# Helper: connect to DuckDB
# -----------------------------
def _connect(db_path: str, read_only: bool = False):
  con = duckdb.connect(db_path, read_only=read_only)
  con.execute("PRAGMA temp_directory='/tmp/duckdb-temp'")
  con.execute("PRAGMA threads=4")
  return con


# -----------------------------
# Placeholder “LLM judge”
# (Replace with real LLM later)
# -----------------------------
def fake_llm_judge(row: Dict[str, Any]) -> Dict[str, Any]:
  """
    Deterministic, cheap stand-in:
    - if strong name + size agreement → same_company=True (confidence 0.7)
    - else → False (confidence 0.6)
    """
  strong = (row.get("name_prefix6_eq", 0) == 1
            and (row.get("emp_tot_reldiff") or 1.0) <= 0.3
            and row.get("country_exact", 0) == 1)
  return {
    "same_company":
    bool(strong),
    "confidence":
    0.7 if strong else 0.6,
    "reasons": [
      "placeholder decision based on name prefix + relative size + country",
      "swap this for real LLM call later"
    ],
    "normalized_names": {
      "a": row.get("country_a", ""),
      "b": row.get("country_b", "")
    }
  }


# =========================================================
# ASSET 1: er_pair_llm_votes
#   - Reads er.pair_features
#   - Filters to “ambiguous” slice (by model_score if present; else heuristic)
#   - Produces er.pair_llm_votes with JSON-style columns
# =========================================================
@asset(
  name="er_pair_llm_votes",
  group_name="er",
  deps=["er_pair_features"],
  compute_kind="python",
)
def er_pair_llm_votes(context) -> MaterializeResult:
  cfg = (context.op_execution_context.run_config or {})
  # Allow both "assets" and "ops" config scopes, take whichever is present:
  cfg_assets = cfg.get("assets", {}).get("er_pair_llm_votes", {})
  cfg_ops = cfg.get("ops", {}).get("er_pair_llm_votes", {})
  cfg = {**cfg_ops, **cfg_assets}

  db_path: str = cfg.get("database", "file:/opt/test-data/experimental.duckdb")
  features_table: str = cfg.get("features_table", "er.pair_features")
  out_table: str = cfg.get("out_table", "er.pair_llm_votes")

  # Ambiguity band (for real model_score); defaults are conservative
  lo: float = float(cfg.get("ambig_lo", 0.35))
  hi: float = float(cfg.get("ambig_hi", 0.65))
  max_pairs: int = int(cfg.get("max_pairs", 50_000))  # cap per run

  # Fallback heuristic thresholds if model_score absent
  heuristic_max_reldiff: float = float(cfg.get("heuristic_max_reldiff", 0.6))

  with _connect(db_path, read_only=False) as con:
    # Detect if model_score exists
    cols = [
      r[0] for r in con.execute(f"PRAGMA table_info('{features_table}')").fetchall()
    ]
    has_model_score = "model_score" in cols

    if has_model_score:
      # Use model score band to pick ambiguous pairs
      sel_sql = f"""
              SELECT *
              FROM {features_table}
              WHERE model_score BETWEEN {lo} AND {hi}
              LIMIT {max_pairs}
            """
    else:
      # Fallback: ambiguous if some signals conflict (cheap heuristic)
      # - same country AND (prefix hit OR city mismatch) AND not wildly different size
      sel_sql = f"""
              SELECT *
              FROM {features_table}
              WHERE country_exact = 1
                AND (name_prefix6_eq = 1 OR city_exact = 0)
                AND COALESCE(emp_tot_reldiff, 1.0) <= {heuristic_max_reldiff}
              LIMIT {max_pairs}
            """

    ambig = con.execute(sel_sql).fetch_df()
    context.log.info(
      f"Selected {len(ambig)} ambiguous pairs for placeholder LLM voting")

    # Produce placeholder votes
    votes: List[Dict[str, Any]] = []
    for _, row in ambig.iterrows():
      rdict = row.to_dict()
      v = fake_llm_judge(rdict)
      votes.append({
        "company_id_a": int(row["company_id_a"]),
        "company_id_b": int(row["company_id_b"]),
        "vote_same": v["same_company"],
        "vote_confidence": float(v["confidence"]),
        "reasons_json": json.dumps(v["reasons"]),
        "normalized_names_json": json.dumps(v["normalized_names"]),
        # Snapshot some useful features for audit:
        "country_exact": int(row.get("country_exact", 0) or 0),
        "name_prefix6_eq": int(row.get("name_prefix6_eq", 0) or 0),
        "emp_tot_reldiff": float(row.get("emp_tot_reldiff", 1.0) or 1.0),
        "model_score": float(row.get("model_score", -1.0)),
      })

    # Persist to DuckDB
    con.execute("CREATE SCHEMA IF NOT EXISTS er")
    con.register("votes_df", ambig.head(0))  # quick way to prime the connection
    # Build from Python list (cleaner types)
    con.execute(
      f"""
          CREATE OR REPLACE TABLE {out_table} AS
          SELECT * FROM read_json_auto($json)
        """, {"json": json.dumps(votes)})

    total = con.execute(f"SELECT COUNT(*) FROM {out_table}").fetchone()[0]

  return MaterializeResult(
    metadata={
      "votes_written": MetadataValue.int(total),
      "used_model_score": MetadataValue.bool(has_model_score),
      "ambig_band": MetadataValue.text(f"[{lo}, {hi}]"),
      "out_table": MetadataValue.text(out_table),
    })


# =========================================================
# ASSET 2: er_pair_matches
#   - Merges model_score + llm votes to final decision
#   - Three-way outcome: match / nonmatch / review
# =========================================================
@asset(
  name="er_pair_matches",
  group_name="er",
  ins={
    "pair_features_dep": AssetIn(key=AssetKey("er_pair_features")),
    "llm_votes_dep": AssetIn(key=AssetKey("er_pair_llm_votes")),
  },
  compute_kind="sql+python",
)
def er_pair_matches(context: AssetExecutionContext) -> MaterializeResult:
  cfg = (context.op_execution_context.run_config or {})
  cfg_assets = cfg.get("assets", {}).get("er_pair_matches", {})
  cfg_ops = cfg.get("ops", {}).get("er_pair_matches", {})
  cfg = {**cfg_ops, **cfg_assets}

  db_path: str = cfg.get("database", "file:/opt/test-data/experimental.duckdb")
  features_table: str = cfg.get("features_table", "er.pair_features")
  votes_table: str = cfg.get("votes_table", "er.pair_llm_votes")
  out_table: str = cfg.get("out_table", "er.pair_matches")

  # Decision thresholds
  hi: float = float(cfg.get("hi", 0.85))  # confident match
  lo: float = float(cfg.get("lo", 0.15))  # confident non-match
  llm_conf_min: float = float(cfg.get("llm_conf_min", 0.70))

  with _connect(db_path, read_only=False) as con:
    # Ensure model_score presence; if missing, create a placeholder score (0.5) to exercise the logic
    cols = [
      r[0] for r in con.execute(f"PRAGMA table_info('{features_table}')").fetchall()
    ]
    has_model_score = "model_score" in cols
    score_expr = "model_score" if has_model_score else "0.5 AS model_score"

    con.execute(f"CREATE SCHEMA IF NOT EXISTS er")

    # Build a working view combining features + votes (left join; not all pairs will have votes)
    con.execute(f"""
          CREATE OR REPLACE TEMP VIEW _pairs_with_votes AS
          SELECT
            f.company_id_a,
            f.company_id_b,
            {score_expr},
            v.vote_same,
            v.vote_confidence
          FROM {features_table} f
          LEFT JOIN {votes_table} v
            ON f.company_id_a = v.company_id_a
           AND f.company_id_b = v.company_id_b
        """)

    # Apply cascade:
    # 1) model_score >= hi → match
    # 2) model_score <= lo → nonmatch
    # 3) else if vote_confidence >= llm_conf_min → follow LLM vote
    # 4) else → review
    con.execute(f"""
          CREATE OR REPLACE TABLE {out_table} AS
          SELECT
            company_id_a,
            company_id_b,
            CASE
              WHEN model_score >= {hi} THEN 'match'
              WHEN model_score <= {lo} THEN 'nonmatch'
              WHEN vote_confidence >= {llm_conf_min} AND vote_same THEN 'match'
              WHEN vote_confidence >= {llm_conf_min} AND NOT vote_same THEN 'nonmatch'
              ELSE 'review'
            END AS decision,
            model_score,
            vote_same,
            vote_confidence,
            CASE
              WHEN model_score >= {hi} THEN 'model_hi'
              WHEN model_score <= {lo} THEN 'model_lo'
              WHEN vote_confidence >= {llm_conf_min} THEN 'llm_vote'
              ELSE 'ambiguous'
            END AS decision_source
          FROM _pairs_with_votes
        """)

    counts = dict(
      con.execute(f"""
          SELECT decision, COUNT(*) FROM {out_table} GROUP BY 1
        """).fetchall())
    total = sum(counts.values())

  return MaterializeResult(
    metadata={
      "rows_total": MetadataValue.int(total),
      "by_decision": MetadataValue.json(counts),
      "hi": MetadataValue.float(hi),
      "lo": MetadataValue.float(lo),
      "llm_conf_min": MetadataValue.float(llm_conf_min),
      "out_table": MetadataValue.text(out_table),
    })
