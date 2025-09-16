import os
import dagster as dg
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue
from dagster_duckdb import DuckDBResource
from resolver.defs.sql_utils import render_sql

stopwords_env = os.getenv("ER_STOPWORDS", "")
stopwords_list = [w.strip() for w in stopwords_env.split(",") if w.strip()]


@dg.asset(deps=["clean_companies"])
def create_blocks(duckdb: DuckDBResource):
  """
     This will create ONLY blocks - will need to shard and pair downstream
     We do this to avoid blowing out memory
   """
  sql = render_sql(
    "create_blocks.sql.j2",
    source_table="silver.companies",
    target_schema="er",
    id_col="company_id",
    name_col="company_name",
    domain_col="domain_name",
    city_col="city",
    country_col="final_country",
    use_domain=True,
    use_name3_country=True,
    use_compact5_city=True,
    max_block_size=1000,
  )
  with duckdb.get_connection() as con:
    con.execute("PRAGMA temp_directory='/tmp/duckdb-temp'")
    con.execute(sql)
  return "er.company_blocks"


@dg.asset(deps=["clean_companies"])
def company_blocks_adaptive(context: AssetExecutionContext,
                            duckdb: DuckDBResource) -> MaterializeResult:
  """
     This will create ONLY blocks using a staged/adaptive approach. The idea here is to avoid
     human trial and error to determine optimal blocking strategy
     
   """
  sql = render_sql(
    "create_blocks_adaptive.sql.j2",
    source_table="silver.companies",
    target_schema="er",
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
    stage3_stopwords=stopwords_list,  # <--- inject here
    enable_stage3_name6=True,
  )
  with duckdb.get_connection() as con:
    con.execute("PRAGMA temp_directory='/tmp/duckdb-temp'")
    con.execute("PRAGMA memory_limit='10GB'")
    con.execute("PRAGMA threads=4")
    con.execute(sql)
    # Collect metrics
    kept_cnt = con.execute("SELECT COUNT(*) FROM er.blocks_adaptive").fetchone()[0]
    heavy_cnt = con.execute(
      "SELECT COUNT(*) FROM er.blocks_adaptive_heavy").fetchone()[0]

    # block size stats for kept
    kept_stats = con.execute("""
        WITH s AS (
          SELECT kind, bkey, COUNT(*) AS n
          FROM er.blocks_adaptive
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
    top_heavy = con.execute("""
        WITH s AS (
          SELECT kind, bkey, COUNT(*) AS n
          FROM er.blocks_adaptive_heavy
          GROUP BY 1,2
        )
        SELECT kind, bkey, n
        FROM s
        ORDER BY n DESC
        LIMIT 10
    """).fetch_df()

    # 3) Emit rich metadata to Dagster
    return MaterializeResult(
      metadata={
        "blocks_kept_rows": MetadataValue.int(kept_cnt),
        "blocks_heavy_rows": MetadataValue.int(heavy_cnt),
        "kept_blocks_total": MetadataValue.int(int(blocks_total)),
        "kept_max_block_size": MetadataValue.int(int(max_kept)),
        "kept_avg_block_size": MetadataValue.float(float(avg_kept)),
        "kept_p90_block_size": MetadataValue.float(float(p90_kept)),
        "est_pairs_total": MetadataValue.int(int(est_pairs_total)),
        #"heavy_blocks_top10_md": MetadataValue.md(top_heavy.to_markdown(index=False)),
        "heavy_blocks_top10_json": MetadataValue.json(top_heavy.to_dict("records")),
        "sql_template": MetadataValue.text("create_blocks_adaptive.sql.j2"),
      })
