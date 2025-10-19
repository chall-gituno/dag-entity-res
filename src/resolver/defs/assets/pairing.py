import dagster as dg
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
  Create our pairs table
  """
  with duckdb.get_connection() as con:
    sql = render_sql("blocking_pairs.sql.j2",
                     pairs_table=settings.pairs_table,
                     blocks_table=settings.blocks_table)
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
    df = con.execute(f"""
    WITH per_kind AS (
      SELECT
        kind,
        COUNT(*)::BIGINT AS total_pairs,
        COUNT(DISTINCT (company_id_a, company_id_b))::BIGINT AS distinct_pairs
      FROM {settings.pairs_table}
      GROUP BY 1
    )
    SELECT
      kind,
      total_pairs,
      distinct_pairs,
      ROUND(100.0 * distinct_pairs / total_pairs, 2) AS distinct_ratio
    FROM per_kind
    ORDER BY distinct_ratio ASC;
    """).fetch_df()

  return df.to_dict("records")


def validate_pairing(context, duckdb: DuckDBResource, settings: ERSettings) -> str:
  stats = collect_stats(duckdb, settings)
  prompt = render_prompt("blocking_pair_check.text.j2", {
    "stats": stats,
  })
  ai_comment = call_ai(prompt)
  context.log.info(ai_comment)
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
