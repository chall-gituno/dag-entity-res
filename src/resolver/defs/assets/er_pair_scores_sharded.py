# src/resolver/defs/assets/er_pair_scores_sharded.py
from __future__ import annotations
from dagster import (
  graph_asset,
  AssetIn,
  AssetKey,
  op,
  In,
  Out,
  Nothing,
  DynamicOut,
  DynamicOutput,
  MaterializeResult,
  MetadataValue,
  define_asset_job,
  multiprocess_executor,
)
from pathlib import Path
import os
import duckdb
import joblib
import pandas as pd

# ---------- DuckDB helpers (consistent config per process) ----------
_DUCKDB_URI = os.getenv("DUCKDB_PATH", "/opt/test-data/experimental.duckdb")
_DUCKDB_CONFIG = {
  "threads": "4",  # safe default; job-level parallelism controls process fanout
  "temp_directory": "/tmp/duckdb-temp",
  # don't set memory_limit here unless you know your box; per-process RAM can stack up
}


def connect_duckdb(read_only: bool = False):
  con = duckdb.connect(_DUCKDB_URI, read_only=read_only, config=_DUCKDB_CONFIG)
  # PRAGMAs after connect (don’t affect the “same config” rule)
  con.execute("PRAGMA threads=4")
  con.execute("PRAGMA temp_directory='/tmp/duckdb-temp'")
  return con


# ---------- Config defaults (can be overridden via run_config) ----------
DEFAULT_FEATURES_TABLE = "er.pair_features"
DEFAULT_SCORES_TABLE = "er.pair_scores"
DEFAULT_SCORED_FEATURES_TABLE = "er.pair_features_scored"
DEFAULT_MODEL_PATH = "models/er_pair_clf.joblib"
DEFAULT_OUT_DIR = "data/er/pair_scores_shards"
#This will thrash a bit and swap a lot using 64 shards on my m3 mac
#DEFAULT_SHARDS = int(os.getenv("SHARD_MODULUS", "64"))
DEFAULT_SHARDS = 128
DEFAULT_DROP_ID = ["company_id_a", "company_id_b", "label"]  # must mirror your training


# ---------- Cheap producer: emit shard indices (dependency-only trigger) ----------
@op(ins={"_trigger": In(Nothing)}, out=DynamicOut(int))
def emit_score_shard_indices():
  for i in range(DEFAULT_SHARDS):
    yield DynamicOutput(i, mapping_key=str(i))


# ---------- Heavy worker: score one shard and write Parquet ----------


@op(out=Out(str, io_manager_key="ephemeral_parquet_io"))
def score_pairs_shard(
  shard_index: int,
  features_table: str = DEFAULT_FEATURES_TABLE,
  model_path: str = DEFAULT_MODEL_PATH,
  out_dir: str = DEFAULT_OUT_DIR,
  shard_modulus: int = DEFAULT_SHARDS,
  drop_id_cols: list[str] = DEFAULT_DROP_ID,
):
  out_base = Path(out_dir)
  out_base.mkdir(parents=True, exist_ok=True)
  out_path = out_base / f"shard={shard_index}.parquet"

  # idempotence: if file exists, skip computation (still yield the path)
  if out_path.exists() and out_path.stat().st_size > 0:
    return str(out_path)

  # Load model pipeline (includes preprocessing)
  if not Path(model_path).exists():
    raise FileNotFoundError(
      f"Model file not found: {model_path}. Save your sklearn Pipeline with joblib.dump(...)."
    )
  clf = joblib.load(model_path)

  con = connect_duckdb(read_only=True)
  try:
    # Deterministic shard filter by hashing the pair key (non-negative modulo)
    df = con.execute(f"""
            SELECT *
            FROM {features_table}
            WHERE (((hash(CAST(company_id_a AS VARCHAR) || '|' || CAST(company_id_b AS VARCHAR))
                    & 9223372036854775807) % {shard_modulus}) = {shard_index})
        """).fetch_df()
  finally:
    con.close()

  if df.empty:
    out_path.touch()
    return str(out_path)

  X = df.drop(columns=drop_id_cols, errors="ignore")
  df["model_score"] = clf.predict_proba(X)[:, 1]
  df[["company_id_a", "company_id_b", "model_score"]].to_parquet(out_path, index=False)

  return str(out_path)


# ---------- Fan-in: union shards → DuckDB tables; cleanup shards ----------
@op
def union_scores_and_cleanup(
  shard_paths: list[str],
  features_table: str = DEFAULT_FEATURES_TABLE,
  scores_table: str = DEFAULT_SCORES_TABLE,
  scored_features_table: str = DEFAULT_SCORED_FEATURES_TABLE,
) -> MaterializeResult:
  from glob import glob
  # Identify the directory and glob
  if not shard_paths:
    raise RuntimeError("No shard paths provided.")
  shard_dir = Path(shard_paths[0]).parent
  glob_pat = str(shard_dir / "shard=*.parquet")

  con = connect_duckdb(read_only=False)
  try:
    con.execute("CREATE SCHEMA IF NOT EXISTS er")
    # Build scores table from shards
    con.execute(
      f"CREATE OR REPLACE TABLE {scores_table} AS "
      f"SELECT * FROM read_parquet($glob)",
      {"glob": glob_pat},
    )
    # Join scores onto features
    con.execute(f"""
            CREATE OR REPLACE TABLE {scored_features_table} AS
            SELECT f.*, s.model_score
            FROM {features_table} f
            LEFT JOIN {scores_table} s
              USING (company_id_a, company_id_b)
        """)
    total_scores = con.execute(f"SELECT COUNT(*) FROM {scores_table}").fetchone()[0]
    total_scored_features = con.execute(
      f"SELECT COUNT(*) FROM {scored_features_table}").fetchone()[0]
  finally:
    con.close()

  # Best-effort cleanup of shard files (optional—comment out if you want to keep them)
  for p in shard_paths:
    try:
      Path(p).unlink(missing_ok=True)
    except Exception:
      pass

  return MaterializeResult(
    metadata={
      "scores_rows": MetadataValue.int(total_scores),
      "scored_features_rows": MetadataValue.int(total_scored_features),
      "scores_table": MetadataValue.text(scores_table),
      "scored_features_table": MetadataValue.text(scored_features_table),
      "shard_dir": MetadataValue.path(str(shard_dir)),
    })


# ---------- Graph-backed asset wiring ----------
@graph_asset(
  name="er_pair_scores",
  group_name="er",
  # depend on upstream features so +er_pair_scores pulls it in
  ins={"er_pair_features": AssetIn(dagster_type=Nothing)},
)
def er_pair_scores_graph(er_pair_features):
  # consume upstream dep as a trigger (Nothing) to enforce ordering
  indices = emit_score_shard_indices(er_pair_features)

  # map scoring across shards (parallel when using multiprocess executor)
  shard_paths = indices.map(score_pairs_shard)

  # fan-in + cleanup
  result = union_scores_and_cleanup(shard_paths.collect())
  return result
