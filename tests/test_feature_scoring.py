import duckdb, joblib, pyarrow as pa, pyarrow.parquet as pq
from pathlib import Path

DB = "/opt/test-data/experimental.duckdb"
FEATURES_TABLE = "er.pair_features"
MODEL_PATH = "models/er_pair_clf.joblib"
OUT_PARQUET = "data/er/pair_scores_shards/score.parquet"
BATCH_SIZE = 100_000

clf = joblib.load(MODEL_PATH)
pre = clf.named_steps["pre"]  # your ColumnTransformer


def required_raw_cols(pre):
  cols = []
  for name, trans, sel in pre.transformers_:
    if sel in (None, "drop"):
      continue
    cols.extend(list(sel) if isinstance(sel, (list, tuple)) else [sel])
  # dedupe preserve order
  seen = set()
  out = []
  for c in cols:
    if c not in seen:
      seen.add(c)
      out.append(c)
  return out


RAW_COLS = required_raw_cols(pre)  # includes both num_cols + cat_cols


def write_scores():
  con = duckdb.connect(DB, read_only=True)
  reader = con.execute(f"""
    SELECT company_id_a, company_id_b, {", ".join(RAW_COLS)}
    FROM {FEATURES_TABLE}
    ORDER BY company_id_a, company_id_b
  """).fetch_record_batch(BATCH_SIZE)

  writer = None
  while True:
    try:
      batch = reader.read_next_batch()
    except StopIteration:
      break
    if batch.num_rows == 0:
      break

    # Arrow → pandas (per-batch, small), keeping names for the ColumnTransformer
    pdf = batch.to_pandas(types_mapper=None)

    # The pipeline handles impute + OHE
    X = pdf[RAW_COLS]
    scores = clf.predict_proba(X)[:, 1]

    out_tbl = pa.table({
      "company_id_a": batch.column("company_id_a"),
      "company_id_b": batch.column("company_id_b"),
      "model_score": pa.array(scores),
    })
    if writer is None:
      Path(OUT_PARQUET).parent.mkdir(parents=True, exist_ok=True)
      writer = pq.ParquetWriter(OUT_PARQUET, out_tbl.schema)
    writer.write_table(out_tbl)

  if writer: writer.close()
  con.close()


def load_scores():
  conw = duckdb.connect(DB)
  conw.execute("CREATE SCHEMA IF NOT EXISTS er")
  conw.execute(
    """
  CREATE OR REPLACE TABLE er.pair_scores_test AS
  SELECT * FROM parquet_scan($p)
  """, {"p": str(OUT_PARQUET)})

  # Prefer a VIEW to avoid materializing 170M rows:
  conw.execute("""
  CREATE OR REPLACE VIEW er.v_pair_features_scored_test AS
  SELECT f.*, s.model_score
  FROM er.pair_features f
  LEFT JOIN er.pair_scores s USING (company_id_a, company_id_b)
  """)
  conw.close()


if __name__ == "__main__":
  load_scores()
