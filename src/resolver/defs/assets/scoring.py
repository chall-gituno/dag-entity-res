import os
from pathlib import Path
import dagster as dg

import joblib
import pyarrow as pa
import pyarrow.parquet as pq
from resolver.defs.resources import DuckDBResource
from resolver.defs.settings import ERSettings
from resolver.defs.sql_utils import connect_duckdb
from resolver.defs.ai_utils import call_ai, render_prompt


@dg.asset(
  name="er_pair_scores",
  group_name="er",
  deps=[dg.AssetKey("er_pair_features")],
  compute_kind="python",
)
def er_pair_scores(context, duckdb: DuckDBResource, settings: ERSettings):
  """
    score our features using our model. We stream our features through the 
    model in batches to keep our system responsive while keeping processing
    time somewhat sane.
  """

  # ------- Configs (override via run config if desired) -------
  features_table: str = settings.features_table
  model_path: str = settings.model_path
  out_parquet: str = settings.score_parquet_path
  batch_size: int = settings.batch_size
  scores_table: str = settings.scores_table
  scored_feature_pairs = settings.scored_feature_pairs
  # ------------------------------------------------------------

  # Load model (expects a Pipeline with a ColumnTransformer named "pre")
  if not Path(model_path).exists():
    raise FileNotFoundError(f"Model file not found: {model_path}")
  clf = joblib.load(model_path)
  pre = clf.named_steps.get("pre")
  if pre is None:
    raise ValueError(
      "Pipeline must include a preprocessor named 'pre' (ColumnTransformer).")

  # Figure out the raw columns the preprocessor expects (num + cat)
  raw_cols: list[str] = []
  for _name, _trans, selector in pre.transformers_:
    if selector in (None, "drop"):
      continue
    if isinstance(selector, (list, tuple)):
      raw_cols.extend(selector)
    else:
      raw_cols.append(selector)
  # De-dup while preserving order
  seen = set()
  raw_cols = [c for c in raw_cols if not (c in seen or seen.add(c))]

  # Stream rows from DuckDB
  con_ro = connect_duckdb(settings.db_uri, read_only=True)
  # don't think we'd need the order by here...it will just
  # cause a delay in processing as it munges through it
  select_sql = f"""
      SELECT company_id_a, company_id_b, {", ".join(raw_cols)}
      FROM {features_table}
      -- ORDER BY company_id_a, company_id_b
    """
  context.log.info(
    f"Streaming select from {features_table} with batch_size={batch_size}; "
    f"{len(raw_cols)} raw columns for preprocessor.")
  reader = con_ro.execute(select_sql).fetch_record_batch(batch_size)

  # Prepare Parquet writer
  out_path = Path(out_parquet)
  out_path.parent.mkdir(parents=True, exist_ok=True)
  writer: pq.ParquetWriter | None = None
  total_rows = 0
  batch_count = 0
  row_count = 0
  heartbeat_every = 10_000_000  # log once every 10M rows
  try:
    while True:
      try:
        batch = reader.read_next_batch()
      except StopIteration:
        break
      if batch.num_rows == 0:
        context.log.info(f"No more data...Done.")
        break
      batch_count += 1
      row_count += batch.num_rows
      # Small pandas DF so ColumnTransformer (OHE) has column names
      pdf = batch.to_pandas(types_mapper=None)
      if row_count >= heartbeat_every:
        context.log.info(
          f"Processed {row_count:,} rows across {batch_count:,} batches so far...")
        #row_count = 0  # reset or leave running total if you prefer
      # Ensure all expected columns are present
      missing = [c for c in raw_cols if c not in pdf.columns]
      if missing:
        raise ValueError(f"Missing expected raw columns: {missing}")

      probs = clf.predict_proba(pdf[raw_cols])[:, 1]

      out_tbl = pa.table({
        "company_id_a": batch.column("company_id_a"),
        "company_id_b": batch.column("company_id_b"),
        "model_score": pa.array(probs),
      })

      if writer is None:
        writer = pq.ParquetWriter(str(out_path), out_tbl.schema)
      writer.write_table(out_tbl)
      total_rows += out_tbl.num_rows

  finally:
    if writer:
      writer.close()
    con_ro.close()

  with duckdb.get_connection() as con:
    context.log.info("Creating final tables/views")
    con.execute("CREATE SCHEMA IF NOT EXISTS er")
    con.execute(f"""
      CREATE OR REPLACE TABLE {scores_table} AS
      SELECT * FROM parquet_scan({out_path})
      """)
    # Prefer a VIEW to avoid materializing 100M+ rows again
    # this is strictly informational so if you are curious
    # AND patient, you can have a look at how our features
    # align with the scores.
    con.execute(f"""
          CREATE OR REPLACE VIEW {scored_feature_pairs} AS
          SELECT f.*, s.model_score
          FROM {features_table} f
          LEFT JOIN {scores_table} s USING (company_id_a, company_id_b)
        """)
    scored_rows = con.execute(f"SELECT COUNT(*) FROM {scores_table}").fetchone()[0]
  context.log.info("Done")
  #stats = collect_scoring_stats(context, duckdb, settings)
  result = {
    "scores_rows": int(scored_rows),
    "scores_table": scores_table,
    "scored_view": scored_feature_pairs,
    "model_path": model_path,
  }

  return dg.Output(
    value=result,
    metadata=result,
  )


@dg.op
def validate_scoring(context, duckdb: DuckDBResource, settings: ERSettings) -> str:
  stats = collect_scoring_stats(context, duckdb, settings)
  context.log.info(f"Stats:\n{stats}")
  prompt = render_prompt("scoring_check.text.j2", {
    "stats": stats,
  })
  ai_comment = call_ai(prompt)
  context.log.info(ai_comment)
  return ai_comment


@dg.job
def scoring_check_job():
  """
  Manually run a check of our scoring.
  """
  validate_scoring()


def collect_scoring_stats(context, duckdb, settings: ERSettings) -> dict:
  stats = {}
  with duckdb.get_connection() as con:
    # Basic row counts
    stats["pair_features_total"] = con.execute(
      f"SELECT COUNT(*) FROM {settings.features_table}").fetchone()[0]
    stats["pair_scores_total"] = con.execute(
      f"SELECT COUNT(*) FROM {settings.scores_table}").fetchone()[0]
    # stats["pair_features_scored_total"] = con.execute(
    #   "SELECT COUNT(*) FROM er.pair_features_scored").fetchone()[0]

    # Null or missing scores
    stats["null_scores"] = con.execute(f"""
      SELECT COUNT(*) FROM {settings.scores_table} WHERE model_score IS NULL
    """).fetchone()[0]

    # Score distribution
    score_summary = con.execute(f"""
      SELECT 
        MIN(model_score), 
        MAX(model_score), 
        AVG(model_score),
        quantile_cont(model_score, 0.5),
        quantile_cont(model_score, 0.1),
        quantile_cont(model_score, 0.9)
      FROM {settings.scores_table}
    """).fetchone()

    (score_min, score_max, score_mean, score_median, score_p10,
     score_p90) = score_summary

    stats.update({
      "score_min": float(score_min or 0),
      "score_max": float(score_max or 0),
      "score_mean": float(score_mean or 0),
      "score_median": float(score_median or 0),
      "score_p10": float(score_p10 or 0),
      "score_p90": float(score_p90 or 0),
    })

    # Simple ratios for sanity
    stats["features_vs_scores_ratio"] = round(
      stats["pair_scores_total"] /
      stats["pair_features_total"], 4) if stats["pair_features_total"] else None
  return stats
