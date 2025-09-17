import os
from pathlib import Path
from typing import List
from resolver.defs.resources import DuckDBResource
from resolver.defs.sql_utils import render_sql
from dagster import (
  op,
  graph_asset,
  DynamicOut,
  DynamicOutput,
  Out,
  In,
  AssetIn,
  AssetKey,
  Nothing,
)
import duckdb as duckdblib

SHARD_MODULUS = int(os.getenv("SHARD_MODULUS", "64"))

CAP_PER_A = None  # e.g. 200 to cap; None = no cap
OUT_DIR = Path("data/er/tmp_blocking_pairs_shards")  # per-shard files


@op(ins={"_trigger": In(Nothing)}, out=DynamicOut(int))
def emit_pair_shard_indices():
  # this op is cheap; it just yields integers
  for i in range(SHARD_MODULUS):
    yield DynamicOutput(i, mapping_key=str(i))


@op(out=Out(str, io_manager_key="ephemeral_parquet_io"))
def build_pairs_shard(shard_index: int):
  # DuckDB can only have one process writing to the .duckdb at a time
  # To avoid locking conflicts, we'll write shards to parquet and then
  # union them all as a single write
  OUT_DIR.mkdir(parents=True, exist_ok=True)
  out_path = OUT_DIR / f"shard={shard_index}.parquet"

  sql = render_sql(
    "blocking_pairs_shard_parquet.sql.j2",
    target_schema="er",
    blocks_table="er.blocks_adaptive",
    shard_index=shard_index,
    shard_modulus=SHARD_MODULUS,
    cap_per_a=CAP_PER_A,

    #bkey_hash_col='bkey_hash',
  )
  # IMPORTANT: open DB in read-only mode so concurrent readers don’t fight
  con = duckdblib.connect(os.getenv("DUCKDB_DATABASE"), read_only=True)
  try:
    con.execute("PRAGMA memory_limit='6GB'")
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA temp_directory='/tmp/duckdb-temp'")
    con.execute(
      f"COPY ({sql}) TO '{out_path.as_posix()}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE)"
    )
  finally:
    con.close()
  return str(out_path)


@op
def er_blocking_pairs_union(paths: list[str], duckdb: DuckDBResource):
  # Single writer process now; safe to write to the DB
  with duckdb.get_connection() as con:
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA temp_directory='/tmp/duckdb-temp'")
    glob_pat = str(Path(paths[0]).parent / "shard=*.parquet")
    con.execute(
      "CREATE OR REPLACE TABLE er.blocking_pairs AS SELECT * FROM read_parquet($g)",
      {"g": glob_pat})
    total = con.execute("SELECT COUNT(*) FROM er.blocking_pairs").fetchone()[0]
    # Delete temp files now that we’ve materialized the union
  for p in paths:
    try:
      os.remove(p)
    except FileNotFoundError:
      pass
  return total


@graph_asset(
  name="er_blocking_pairs",
  group_name="er",
  ins={"er_company_blocking": AssetIn(dagster_type=Nothing)},
  #ins={"er_company_blocking": AssetIn(dagster_type=Nothing)},
)
def er_blocking_pairs_graph(er_company_blocking):
  """
  Generate our pairs from our blocks
  """
  indices = emit_pair_shard_indices(er_company_blocking)
  # fanout so we can do parallel work
  shard_paths = indices.map(build_pairs_shard)
  # fan in from all the shard files to the final table
  total = er_blocking_pairs_union(shard_paths.collect())  # single writer + cleanup
  return total
