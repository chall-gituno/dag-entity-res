import os, glob
from pathlib import Path
from dagster import (
  asset,
  op,
  graph_asset,
  DynamicOut,
  DynamicOutput,
  Out,
  AssetExecutionContext,
  MaterializeResult,
  MetadataValue,
)
from typing import List
import duckdb as duckdblib
from resolver.defs.resources import DuckDBResource
from resolver.defs.sql_utils import render_sql

SHARD_MODULUS = int(os.getenv("SHARD_MODULUS", "64"))
PAIRS_TABLE = "er.blocking_pairs"
COMPANIES_TABLE = "silver.companies"
OUT_DIR = Path("data/er/tmp_features_shards")


@op(out=DynamicOut(int))
def emit_shard_indices(context):
  # this op is cheap; it just yields integers
  for i in range(SHARD_MODULUS):
    yield DynamicOutput(i, mapping_key=str(i))


@op(out=Out(str, io_manager_key="ephemeral_parquet_io"))
def build_feature_shard(shard_index: int,
                        pairs_table: str = "er.blocking_pairs",
                        companies_table: str = "silver.companies",
                        out_dir: str = "data/er/tmp_features_shards"):

  out = Path(out_dir)
  out.mkdir(parents=True, exist_ok=True)
  out_path = out / f"shard={shard_index}.parquet"

  sql = render_sql(
    "pair_features_shard.sql.j2",
    shard_index=shard_index,
    shard_modulus=SHARD_MODULUS,
    pairs_table=pairs_table,
    companies_table=companies_table,
  )
  inner = sql.rstrip().rstrip(";").rstrip()

  con = duckdblib.connect(os.getenv("DUCKDB_DATABASE"), read_only=True)
  try:
    con.execute("PRAGMA temp_directory='/tmp/duckdb-temp'")
    con.execute("PRAGMA memory_limit='4GB'")
    con.execute("PRAGMA threads=2")
    con.execute(
      f"COPY ({inner}) TO '{out_path.as_posix()}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)"
    )
  finally:
    con.close()

  return str(out_path)


@op
def union_and_cleanup(paths: list[str]) -> int:
  con = duckdblib.connect(os.getenv("DUCKDB_DATABASE"))
  try:
    con.execute("PRAGMA threads=4")
    con.execute("CREATE SCHEMA IF NOT EXISTS er")
    # If you prefer, use the explicit list of paths instead of globs:
    # read_parquet(list_of_paths) works too.
    glob_pat = str(Path(paths[0]).parent / "shard=*.parquet")
    con.execute(
      "CREATE OR REPLACE TABLE er.pair_features AS SELECT * FROM read_parquet($g)",
      {"g": glob_pat})
    total = con.execute("SELECT COUNT(*) FROM er.pair_features").fetchone()[0]
  finally:
    con.close()

  # Delete temp files now that we’ve materialized the union
  for p in paths:
    try:
      os.remove(p)
    except FileNotFoundError:
      pass

  # Also nuke the registry dir for this run (optional)
  # (If you want, write a helper that derives run_id from an env var in this process)
  tmp_dir = Path(OUT_DIR)
  for extra in glob.glob(str(tmp_dir / "run_*/*.lst")):
    try:
      os.remove(extra)
    except FileNotFoundError:
      pass

  return total


@graph_asset(name="er_pair_features", group_name="er")
def er_pair_features_graph():
  # cheap, sequential
  indices = emit_shard_indices()
  # fanout so we can do parallel work
  shard_paths = indices.map(build_feature_shard)
  # fan in from all the shard files to the final table
  total = union_and_cleanup(shard_paths.collect())  # single writer + cleanup
  return total

  # # Fan-out (each dynamic output carries a Parquet path managed by the IOManager)
  # shard_paths = produce_feature_shards().collect()

  # # Fan-in (union) + cleanup
  # total = union_and_cleanup(shard_paths)
  # return total
