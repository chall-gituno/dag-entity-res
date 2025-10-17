import os
from pathlib import Path
import dagster as dg
#from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from typing import List
from resolver.defs.resources import DuckDBResource
from resolver.defs.sql_utils import render_sql

SHARD_MODULUS = int(os.getenv("SHARD_MODULUS", "64"))

CAP_PER_A = None  # e.g. 200 to cap; None = no cap
OUT_DIR = Path("/tmp/data/er/blocking_pairs_shards")  # per-shard files


def make_pairs_shard(i: int, duckdb):
  # DuckDB can only have one process writing to the .duckdb at a time
  # To avoid locking conflicts, we'll write shards to parquet and then
  # union them all as a single write
  OUT_DIR.mkdir(parents=True, exist_ok=True)
  out_path = OUT_DIR / f"shard={i}.parquet"

  pairs_shard_table = f"ephem.blocking_pairs_{i}"
  sql = render_sql(
    "blocking_pairs_shard.sql.j2",
    target_schema="ephem",
    blocks_table="er.company_blocks",
    shard_table=pairs_shard_table,
    shard_index=i,
    shard_modulus=SHARD_MODULUS,
    cap_per_a=CAP_PER_A,
    #bkey_hash_col='bkey_hash',
  )

  with duckdb.get_connection() as con:
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA temp_directory='/tmp/duckdb-temp'")
    con.execute(sql)

    # Dump result table to a per-shard Parquet file
    con.execute(f"""
          COPY (SELECT * FROM {pairs_shard_table})
          TO '{out_path.as_posix()}'
          (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE);
      """)
  return out_path.as_posix()
  #return pairs_shard_table


def cleanup_this_mess(duckdb):
  with duckdb.get_connection() as conn:
    conn.execute("DROP SCHEMA IF EXISTS ephem CASCADE")


@dg.asset(name="er_company_blocking_pairs", deps=["er_company_blocking"])
def er_company_blocking_pairs(context, duckdb: DuckDBResource):
  pair_shards: List = [make_pairs_shard(i, duckdb) for i in range(SHARD_MODULUS)]

  out_table = "er.company_blocking_pairs"
  with duckdb.get_connection() as con:
    con.execute(f"""
            CREATE OR REPLACE TABLE {out_table} AS
            SELECT * FROM read_parquet('{OUT_DIR}/shard=*.parquet');
        """)
    total = con.execute(f"SELECT COUNT(*) FROM {out_table}").fetchone()[0]

  outcome = {
    "rows": total,
    "total_shards": len(pair_shards),
    "ouput_table": out_table,
  }
  cleanup_this_mess(duckdb)
  context.add_output_metadata(outcome)


# @asset(deps=pair_shard_assets, name="er_blocking_pairs")
# def er_blocking_pairs_union(context: AssetExecutionContext,
#                             duckdb: DuckDBResource) -> MaterializeResult:
#   parts = " UNION ALL ".join(
#     [f"SELECT * FROM er.blocking_pairs_{i}" for i in range(SHARD_MODULUS)])
#   sql = f"CREATE SCHEMA IF NOT EXISTS er; CREATE OR REPLACE TABLE er.blocking_pairs AS {parts};"
#   with duckdb.get_connection() as con:
#     con.execute(sql)
#     total = con.execute("SELECT COUNT(*) FROM er.blocking_pairs").fetchone()[0]
#   return MaterializeResult(metadata={"total_pairs": MetadataValue.int(total)})

# @asset(deps=pair_shard_assets, name="er_blocking_pairs")
# def er_blocking_pairs_union(context: AssetExecutionContext,
#                             duckdb: DuckDBResource) -> MaterializeResult:
#   parts = " UNION ALL ".join(
#     [f"SELECT * FROM er.blocking_pairs_{i}" for i in range(SHARD_MODULUS)])
#   sql = f"""
#     CREATE SCHEMA IF NOT EXISTS er;
#     CREATE OR REPLACE TABLE er.blocking_pairs AS {parts};
#     """
#   with duckdb.get_connection() as con:
#     con.execute(sql)
#     total = con.execute("SELECT COUNT(*) FROM er.blocking_pairs").fetchone()[0]

#     # Little skew report (top heavy shards)
#     skew = con.execute("""
#             WITH per AS (
#               SELECT (hash(company_id_a) % 64) AS p, COUNT(*) AS n
#               FROM er.blocking_pairs
#               GROUP BY 1
#             )
#             SELECT p, n
#             FROM per
#             ORDER BY n DESC
#             LIMIT 10
#         """).fetch_df()

#   return MaterializeResult(
#     metadata={
#       "total_pairs": MetadataValue.int(total),
#       "union_of_shards": MetadataValue.int(SHARD_MODULUS),
#       "skew_top10": MetadataValue.table(skew),
#     })
