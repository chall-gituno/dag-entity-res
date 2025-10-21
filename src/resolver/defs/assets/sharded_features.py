import os
from pathlib import Path
import dagster as dg
from typing import List
from resolver.defs.resources import DuckDBResource
from resolver.defs.sql_utils import render_sql
from resolver.defs.settings import ERSettings


def make_feature_shard(i: int, duckdb, settings: ERSettings):
  out_dir = Path(settings.features_parquet_out)
  out_dir.mkdir(parents=True, exist_ok=True)
  out_path = out_dir / f"shard={i}.parquet"
  pairs_table = settings.pairs_table
  comp_table = settings.clean_companies

  sql = render_sql(
    "pair_features_shard.sql.j2",
    shard_index=i,
    shard_modulus=settings.shard_modulus,
    pairs_table=pairs_table,
    companies_table=comp_table,
  )
  # our rendered sql is standalone (ends with ';')
  # but in this asset, we are embedding it in a COPY
  # so we need to get rid of that semi-colon
  inner = sql.rstrip().rstrip(';').rstrip()

  with duckdb.get_connection() as con:
    # con.execute("PRAGMA threads=2")
    # con.execute("PRAGMA memory_limit='4GB'")
    con.execute("PRAGMA temp_directory='/tmp/duckdb-temp'")
    # Export directly from SELECT to Parquet (no table creation needed)
    con.execute(f"""
              COPY (
                  {inner}
              ) TO '{out_path.as_posix()}'
              (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE);
          """)
    # count rows for metadata
    cnt = con.execute(
      f"SELECT COUNT(*) FROM read_parquet('{out_path.as_posix()}')").fetchone()[0]
  return out_path


@dg.asset(deps=["er_company_pairs"], name="er_pair_features")
def er_pair_features_union(
  context: dg.AssetExecutionContext,
  duckdb: DuckDBResource,
  settings: ERSettings,
):
  """
  Create our features table from our pairs. Once this is materialized, we take a pause and work on our model.
  This is a fairly demanding process so we do it in shards to keep our system from getting hammered.
  """
  context.log.info("Creating feature shards...this may take a while")
  feature_shard_assets: List = [
    make_feature_shard(i, duckdb, settings) for i in range(settings.shard_modulus)
  ]
  features_table = settings.features_table
  with duckdb.get_connection() as con:
    context.log.info("Feature shards done...assembling final table")
    con.execute("PRAGMA threads=4")
    con.execute(f"""
            CREATE OR REPLACE TABLE {features_table} AS
            SELECT *
            FROM read_parquet('{settings.features_parquet_out}/shard=*.parquet');
        """)
    total = con.execute(f"SELECT COUNT(*) FROM {features_table}").fetchone()[0]

    # quick feature sanity: how many exact domain matches?
    dom = con.execute(f"""
            SELECT SUM(domain_exact) AS domain_exact_matches
            FROM {features_table}
        """).fetchone()[0]

  result = {
    "total_rows": int(total),
    "domain_exact_matches": int(int(dom or 0)),
    "ouput_table": features_table,
  }
  return dg.Output(value=result, metadata=result)
