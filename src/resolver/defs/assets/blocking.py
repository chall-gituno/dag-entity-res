import os
import dagster as dg

from dagster_duckdb import DuckDBResource
from resolver.defs.sql_utils import render_sql
import json
from resolver.defs.ai_utils import call_ai, render_prompt
from resolver.defs.utils import normalize_metadata

stopwords_env = os.getenv("ER_STOPWORDS", "")
stopwords_list = [w.strip() for w in stopwords_env.split(",") if w.strip()]


@dg.asset(deps=["clean_companies"], group_name="er", tags={"quality": "working"})
def er_company_blocking(context, duckdb: DuckDBResource):
  """
     This will create ONLY blocks using a staged/adaptive approach. The idea here is to avoid
     human trial and error to determine optimal blocking strategy.
  """
  blocks_table = "er.company_blocks"
  # Most of these params are for experimenting, debugging, refining
  # TODO: get them from the runtime config
  sql = render_sql(
    "create_blocks_adaptive.sql.j2",
    source_table="silver.companies",
    target_schema="er",
    blocks_table=blocks_table,
    id_col="company_id",
    name_col="company_name",
    domain_col="domain_name",
    city_col="city",
    country_col="final_country",
    max_block_size=200,
    use_domain=True,
    use_name3_country=True,
    use_compact5_city=True,
    refine_domain_with_country=True,
    enable_stage3_token=True,
    stage3_min_token_len=3,
    stage3_stopwords=stopwords_list,
    enable_stage3_name6=True,
  )
  with duckdb.get_connection() as con:
    con.execute("PRAGMA temp_directory='/tmp/duckdb-temp'")
    con.execute("PRAGMA memory_limit='10GB'")
    # Produce our blocking tables
    con.execute(sql)
    # Collect metrics
    kept_cnt = con.execute(f"SELECT COUNT(*) FROM {blocks_table}").fetchone()[0]
    heavy_cnt = con.execute(f"SELECT COUNT(*) FROM {blocks_table}_heavy").fetchone()[0]

    # block size stats for kept
    kept_stats = con.execute(f"""
        WITH s AS (
          SELECT kind, bkey, COUNT(*) AS n
          FROM {blocks_table}
          GROUP BY 1,2
        )
        SELECT
          COUNT(*)                           AS blocks_total,
          COALESCE(MAX(n),0)                 AS max_block_size_kept,
          COALESCE(AVG(n),0)                 AS avg_block_size_kept,
          COALESCE(quantile_cont(n,0.90),0)  AS p90_block_size_kept,
          SUM((n*(n-1))/2)                   AS est_pairs_total
        FROM s
    """).fetchone()

    (blocks_total, max_kept, avg_kept, p90_kept, est_pairs_total) = kept_stats

    # top 10 heavy blocks (still above cap after refinement)
    top_heavy = con.execute(f"""
        WITH s AS (
          SELECT kind, bkey, COUNT(*) AS n
          FROM {blocks_table}_heavy
          GROUP BY 1,2
        )
        SELECT kind, bkey, n
        FROM s
        ORDER BY n DESC
        LIMIT 10
    """).fetch_df()

    outcome = {
      "blocks_kept_rows": kept_cnt,
      "blocks_heavy_rows": heavy_cnt,
      "kept_blocks_total": int(blocks_total),
      "kept_max_block_size": int(max_kept),
      "kept_avg_block_size": float(avg_kept),
      "kept_p90_block_size": float(p90_kept),
      "est_pairs_total": int(est_pairs_total),
      "heavy_blocks_top10_json": top_heavy.to_dict("records"),
      "sql_template": "create_blocks_adaptive.sql.j2",
    }
    return dg.Output(
      value=outcome,
      metadata={
        "blocks_kept_rows": kept_cnt,
        "blocks_heavy_rows": heavy_cnt,
        "blocks_table_prefix": blocks_table,
        "blocks_schema": "er",
      },
    )


# Attach a check on our asset...if we really cared about
# AI's opnion here, we could set blocking=True
@dg.asset_check(
  asset=er_company_blocking,
  name="ai_block_quality_check",
  description="Get AI's opinion on our blocking efforts",
)
def ai_block_quality_check(context, er_company_blocking):
  if not os.getenv("OPENAI_API_KEY"):
    context.log.info("AI validation will be skipped")
    return dg.AssetCheckResult(passed=True)
  ai_comment = get_ai_opinion(er_company_blocking)
  context.log.info(f"\n***AI OPINION***\n{ai_comment}")
  return dg.AssetCheckResult(
    passed=True,
    metadata={"ai_analysis": dg.MetadataValue.text(ai_comment)},
  )


def get_ai_opinion(metadata):
  exclude = {"heavy_blocks_top10_json", "sql_template"}
  filtered = {k: v for k, v in metadata.items() if k not in exclude}
  stats = json.dumps(filtered, indent=2)
  heavy = json.dumps(metadata.get("heavy_blocks_top10_json"), indent=2)

  prompt = render_prompt("blocking_check.text.j2", {
    "type": "company",
    "stats": stats,
    "heavy_blocks": heavy,
  })

  ai_comment = call_ai(prompt)
  return ai_comment


@dg.op
def validate_blocking(context) -> str:
  if not os.getenv("OPENAI_API_KEY"):
    context.log.info("AI validation will be skipped")
    return

  # Get the latest materialization event for your asset
  asset_key = dg.AssetKey(["er_company_blocking"])

  # Get the latest materialization event
  last_mat_event = context.instance.get_latest_materialization_event(asset_key)

  if not last_mat_event:
    context.log.warning("No materialization found for asset")
    return

  # Access the metadata from the materialization
  materialization = last_mat_event.asset_materialization
  md = materialization.metadata
  normalized = normalize_metadata(md)
  ai_comment = get_ai_opinion(normalized)
  context.add_output_metadata({"ai_summary": dg.MetadataValue.text(ai_comment)})


@dg.job
def ai_blocking_check_job():
  """
  Manually run a check of our blocking strategy using AI.
  """
  validate_blocking()


# @dg.asset(deps=["clean_companies"])
# def company_block_pair_with_stats(duckdb: DuckDBResource):
#   sql = render_sql(
#     "blocking_stats.sql.j2",
#     source_table="silver.companies",
#     target_schema="er",
#     id_col="company_id",
#     name_col="company_name",
#     domain_col="domain_name",
#     city_col="city",
#     country_col="final_country",
#     max_block_size=1000,
#     use_domain=True,
#     use_name3_country=True,
#     use_compact5_city=True,
#     # pairs_table="er.blocking_pairs",  # pass this if you already materialized pairs
#   )
#   with duckdb.get_connection() as con:
#     con.execute(sql)
