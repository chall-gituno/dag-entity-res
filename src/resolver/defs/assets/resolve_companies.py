import os
import dagster as dg

from dagster_duckdb import DuckDBResource
from resolver.defs.sql_utils import render_sql
import json
from resolver.defs.ai_utils import call_ai, render_prompt
from resolver.defs.utils import normalize_metadata
from resolver.defs.settings import ERSettings


@dg.asset(deps=["er_entities"], group_name="gold", tags={"quality": "gold"})
def er_resolve_companies(context, duckdb: DuckDBResource, settings: ERSettings):
  """
     Produce resolved companies table from out entity clusters.  We are calling this
     ouput 'gold' for the sake of this experiment but a proper pass on the entire
     pipeline is really in order prior to classifying it as such.
  """
  sql = render_sql("resolve_companies.sql.j2")
  with duckdb.get_connection() as con:
    con.execute("PRAGMA memory_limit='6GB'")
    con.execute("Create schema if not exists gold")
    con.execute(sql)
    cnt = con.execute("Select count(*) from gold.resolved_companies").fetchone()[0]

  ai_opinion = validate_resolved(context, duckdb, settings)
  outcome = {
    "output_table": "gold.resolved_companies",
    "rows": int(cnt),
    "ai_analysis": ai_opinion,
  }
  return dg.Output(value=outcome, metadata=outcome)


def validate_resolved(context, duckdb: DuckDBResource, settings: ERSettings) -> str:
  stats = get_resolved_summary(duckdb, settings)
  prompt = render_prompt("final_canonicalization_review.text.j2", stats)
  #context.log.info(f"Prompt:\n{prompt}")
  ai_comment = call_ai(prompt)
  context.log.info(f"\n{ai_comment}")
  return ai_comment


@dg.op
def validate_resolved_op(context, duckdb: DuckDBResource, settings: ERSettings) -> str:
  return validate_resolved(context, duckdb, settings)


@dg.job
def validate_resolved_job():
  """
  Manually run a check of our resolved companies using ai.
  """
  validate_resolved_op()


def get_resolved_summary(duckdb: DuckDBResource, settings: ERSettings):
  """Collect stats for resolved entities and canonicalized companies."""
  stats = {}
  with duckdb.get_connection() as con:
    stats["total_entities"] = con.execute(
      "SELECT COUNT(DISTINCT entity_id) FROM er.entities").fetchone()[0]

    stats["total_records"] = con.execute(
      "SELECT COUNT(*) FROM er.entities").fetchone()[0]

    stats["avg_cluster_size"] = con.execute("""
      SELECT ROUND(AVG(cluster_size), 2)
      FROM (
        SELECT COUNT(*) AS cluster_size
        FROM er.entities
        GROUP BY entity_id
      )
    """).fetchone()[0]

    stats["max_cluster_size"] = con.execute("""
      SELECT MAX(cluster_size)
      FROM (
        SELECT COUNT(*) AS cluster_size
        FROM er.entities
        GROUP BY entity_id
      )
    """).fetchone()[0]

    stats["singletons"] = con.execute("""
      SELECT COUNT(*)
      FROM (
        SELECT entity_id
        FROM er.entities
        GROUP BY entity_id
        HAVING COUNT(*) = 1
      )
    """).fetchone()[0]

    stats["multi_member_entities"] = stats["total_entities"] - stats["singletons"]
    stats["pct_singletons"] = round(
      100 * stats["singletons"] / stats["total_entities"],
      2,
    )
    stats["resolved_rows"] = con.execute(
      "SELECT COUNT(*) FROM gold.resolved_companies").fetchone()[0]

  return stats
