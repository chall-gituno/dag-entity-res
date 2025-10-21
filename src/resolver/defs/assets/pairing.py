import dagster as dg
from typing import List
from resolver.defs.resources import DuckDBResource
from resolver.defs.sql_utils import render_sql
from resolver.defs.settings import ERSettings
from resolver.defs.ai_utils import call_ai, render_prompt


@dg.asset(
  name="er_company_pairs",
  deps=["er_company_blocking"],
  group_name="er",
  tags={"quality": "working"},
)
def er_company_pairs(context, duckdb: DuckDBResource, settings: ERSettings):
  """
  Create our pairs table.  this will actually produce two tables. One has kind/bkey information
  and the other is just the raw pairs we can use for features. our pairs are capped to a max
  number per A so we don't let any one company overwhelm the candidate set.
  """
  with duckdb.get_connection() as con:
    sql = render_sql(
      "blocking_pairs.sql.j2",
      pairs_with_kind=settings.pairs_with_kind_table,
      pairs_table=settings.pairs_table,
      blocks_table=settings.blocks_table,
      cap_per_a=200,
    )

    con.execute(sql)
    total = con.execute(f"SELECT COUNT(*) FROM {settings.pairs_table}").fetchone()[0]
  ai_opinion = validate_pairing(context, duckdb, settings)

  outcome = {
    "rows": total,
    "ouput_table": settings.pairs_table,
    "ai_analysis": ai_opinion
  }

  context.add_output_metadata(outcome)
  return dg.Output(value=outcome, metadata=outcome)


def collect_stats(duckdb: DuckDBResource, settings: ERSettings) -> dict:
  with duckdb.get_connection() as con:
    sql = render_sql("blocking_overlap.sql.j2", blocks_table=settings.blocks_table)
    con.execute(sql)
    # Fetch summary
    summary_row = con.execute("SELECT * FROM tmp_company_overlap_summary").fetchone()
    summary = {
      "total_companies": summary_row[0],
      "single_kind_companies": summary_row[1],
      "multi_kind_companies": summary_row[2],
      "pct_multi_kind": summary_row[3],
    }

    # fetch top overlaps
    top_overlaps = con.execute("SELECT * FROM tmp_pairwise_overlap").fetch_df().to_dict(
      "records")

  return {"summary": summary, "top_overlaps": top_overlaps}


### Sharded Approach ###
def make_pairs_shard(i: int, duckdb, settings: ERSettings):
  out_dir = settings.pairs_parquet_out
  out_dir.mkdir(parents=True, exist_ok=True)
  out_path = out_dir / f"shard={i}.parquet"

  #pairs_shard_table = f"ephem.blocking_pairs_shard_{i}"
  # setting shard table to none will mean we won't create it
  # assumes we are not wanting them around for later inspection
  pairs_shard_table = None
  sql = render_sql(
    "blocking_pairs_shard.sql.j2",
    blocks_table=settings.blocks_table,
    shard_table=pairs_shard_table,
    shard_index=i,
    shard_modulus=settings.shard_modulus,
    cap_per_a=200,
  )

  inner = sql.rstrip().rstrip(';').rstrip()
  with duckdb.get_connection() as con:
    con.execute("PRAGMA temp_directory='/tmp/duckdb-temp'")
    if pairs_shard_table:
      # create ephemeral schema if we are keeping tables
      con.execute("CREATE SCHEMA IF NOT EXISTS ephem")
    exec_comm = f"""
          COPY (
              {inner}
          ) TO '{out_path.as_posix()}'
          (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE);
      """
    #
    con.execute(exec_comm)
    # if you wanna keep shards in the database for inspection
    # you can use the following.
    # con.execute(sql)
    # con.execute(f"""
    #       COPY (SELECT * FROM {pairs_shard_table})
    #       TO '{out_path.as_posix()}'
    #       (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE);
    #   """)
  return out_path.as_posix()
  #return pairs_shard_table


def cleanup_this_mess(duckdb):
  """If you persisted shards to the DB, you can call this to clean em out"""
  with duckdb.get_connection() as conn:
    conn.execute("DROP SCHEMA IF EXISTS ephem CASCADE")


@dg.asset(
  name="er_sharded_blocking_pairs",
  deps=["er_company_blocking"],
  group_name="wip_experimental",
  tags={"quality": "wip"},
)
def er_company_blocking_pairs(context, duckdb: DuckDBResource, settings: ERSettings):
  """
  Create our pairs table.  This will use a sharding approach to keep memory flat.
  TODO: might need attention...need to verify boundaries are as expected
  """

  pair_shards: List = [
    make_pairs_shard(i, duckdb, settings) for i in range(settings.shard_modulus)
  ]

  out_table = settings.pairs_table
  with duckdb.get_connection() as con:
    con.execute(f"""
            CREATE OR REPLACE TABLE {out_table} AS
            SELECT distinct * FROM read_parquet('{settings.pairs_parquet_out}/shard=*.parquet');
        """)
    total = con.execute(f"SELECT COUNT(*) FROM {out_table}").fetchone()[0]

  outcome = {
    "rows": total,
    "total_shards": len(pair_shards),
    "ouput_table": out_table,
  }
  cleanup_this_mess(duckdb)
  return dg.Output(
    value=outcome,
    metadata=outcome,
  )


def validate_pairing(context, duckdb: DuckDBResource, settings: ERSettings) -> str:
  stats = collect_stats(duckdb, settings)
  #print(stats)
  prompt = render_prompt("blocking_pair_check.text.j2", stats)
  #context.log.info(f"Prompt:\n{prompt}")
  ai_comment = call_ai(prompt)
  context.log.info(f"\n{ai_comment}")
  return ai_comment


@dg.op
def validate_pairing_op(context, duckdb: DuckDBResource, settings: ERSettings) -> str:
  return validate_pairing(context, duckdb, settings)


@dg.job
def pairing_check_job():
  """
  Manually run a check of our pairing.
  """
  validate_pairing_op()
