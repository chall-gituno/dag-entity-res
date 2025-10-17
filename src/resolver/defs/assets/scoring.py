import os
from pathlib import Path
import dagster as dg

import joblib
import pyarrow as pa
import pyarrow.parquet as pq
from resolver.defs.resources import DuckDBResource
from resolver.defs.settings import ERSettings
from resolver.defs.sql_utils import connect_duckdb


@dg.asset(
  name="er_pair_scores",
  group_name="er",
  deps=[dg.AssetKey("er_pair_features")],
  compute_kind="python",
  description=(
    "Stream-score er.pair_features with a saved sklearn Pipeline using batches. "
    "Writes Parquet → creates er.pair_scores, and a view er.v_pair_features_scored."),
)
def er_pair_scores(context, duckdb: DuckDBResource,
                   settings: ERSettings) -> dg.MaterializeResult:
  # ------- Configs (override via run config if desired) -------
  features_table: str = "er.pair_features"
  model_path: str = settings.model_path
  out_parquet: str = settings.score_parquet_path
  batch_size: int = settings.batch_size
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
  con_ro = connect_duckdb(os.getenv("DUCKDB_DATABASE"), read_only=True)
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
    con.execute(
      "CREATE OR REPLACE TABLE er.pair_scores AS "
      "SELECT * FROM parquet_scan($p)",
      {"p": str(out_path)},
    )
    # Prefer a VIEW to avoid materializing 100M+ rows again
    con.execute("""
          CREATE OR REPLACE VIEW er.v_pair_features_scored AS
          SELECT f.*, s.model_score
          FROM er.pair_features f
          LEFT JOIN er.pair_scores s USING (company_id_a, company_id_b)
        """)
    scored_rows = con.execute("SELECT COUNT(*) FROM er.pair_scores").fetchone()[0]
  context.log.info("Done")

  return dg.MaterializeResult(
    metadata={
      "written_parquet": dg.MetadataValue.path(str(out_path)),
      "scores_rows": dg.MetadataValue.int(int(scored_rows)),
      "batch_size": dg.MetadataValue.int(batch_size),
      "features_table": dg.MetadataValue.text(features_table),
      "model_path": dg.MetadataValue.text(model_path),
      "scores_table": dg.MetadataValue.text("er.pair_scores"),
      "scored_view": dg.MetadataValue.text("er.v_pair_features_scored"),
    })
