import os
import zipfile
from pathlib import Path

import dagster as dg
from dagster import asset, AssetExecutionContext
from dagster_duckdb import DuckDBResource
from resolver.defs.sql_utils import render_sql


def get_companies() -> str:
  """
  Would usually get it from an api or whatever (this came from kaggle)
  """
  path = os.getenv("COMPANY_FILE", "/opt/test-data/companies.zip")
  return path


def expand_companies(companies_raw: str) -> dict:
  """
  Expands the zip from companies_raw into a per-run temp dir.
  Returns a JSON-serializable dict with the dir and list of CSV files.
  """
  zip_path = Path(companies_raw)
  if not zip_path.exists():
    raise dg.Failure(f"Zip not found: {zip_path}")

  # Per-run temp dir under Dagster's storage directory
  #base = Path(context.instance.storage_directory()) / "expansions" / context.run_id[:8]

  base = Path("/tmp/dagster/expansions/companies")
  base.mkdir(parents=True, exist_ok=True)

  # Unzip
  with zipfile.ZipFile(zip_path) as zf:
    zf.extractall(base)

  # Collect CSVs
  csvs = sorted(str(p) for p in base.rglob("*.csv"))
  if not csvs:
    # Surface a crisp failure so it shows clearly in the UI
    raise dg.Failure(f"No CSV files found after expanding {zip_path} into {base}")
  payload = {"dir": str(base), "csvs": csvs}
  # context.log.info(f"Expanded {zip_path} to {base} with {len(csvs)} CSVs")
  # context.add_output_metadata({
  #   "expanded_dir": str(base),
  #   "csv_count": len(csvs),
  #   "sample_files": csvs[:5],
  # })
  return payload


@dg.asset(group_name="ingested")
def companies(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> None:
  """
  Loads the expanded CSVs into DuckDB (bronze.companies).
  Adjust schema/table naming as you like.
  """
  path = get_companies()
  companies_info = expand_companies(path)
  expanded_dir = Path(companies_info["dir"])
  # Use a glob so multiple CSVs are supported
  glob_pattern = str(expanded_dir / "*.csv")

  # Connect via DuckDBResource
  con = duckdb.get_connection()
  with duckdb.get_connection() as con:
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    # Create/replace with all CSV rows
    con.execute(f"""
      CREATE OR REPLACE TABLE bronze.companies AS
      SELECT * FROM read_csv_auto('{glob_pattern}', header=true,
        types = {{
    'year founded': 'INTEGER',
    'current employee estimate': 'INTEGER',
    'total employee estimate': 'INTEGER'
       }})
    """)
    n = con.execute("SELECT COUNT(*) FROM bronze.companies").fetchone()[0]
    context.add_output_metadata({"rows": n, "source_glob": glob_pattern})
    context.log.info(f"bronze.companies loaded with {n} rows from {glob_pattern}")


@dg.asset(deps=["companies"])
def clean_companies(context: AssetExecutionContext, duckdb: DuckDBResource) -> str:
  """Apply cleaning and normalization to our raw data.  output result to silver schema"""
  sql = render_sql("clean_kag_comp.sql.j2",
                   source_schema="bronze",
                   target_schema="silver")
  with duckdb.get_connection() as con:
    con.execute("create schema if not exists silver")
    con.execute(sql)
  return "silver.companies"


@dg.asset(deps=["clean_companies"])
def company_block_pairs(context: AssetExecutionContext, duckdb: DuckDBResource):
  """
  Create a table in the er schema (entity resolution) with block pairs
  we can use to assist in our entity matching features
  """
  sql = render_sql(
    "company_blocking.sql.j2",
    source_table="silver.companies",
    target_schema="er",
    target_table="blocking_pairs",
    id_col="company_id",
    name_col="company_name",
    domain_col="domain_name",
    city_col="city",
    country_col="final_country",
    max_block_size=1000,
    use_domain=True,
    use_name3_country=True,
    use_compact5_city=True,
  )
  with duckdb.get_connection() as con:
    con.execute(sql)


@dg.asset(deps=["clean_companies"])
def company_block_pair_with_stats(duckdb: DuckDBResource):
  sql = render_sql(
    "blocking_stats.sql.j2",
    source_table="silver.companies",
    target_schema="er",
    id_col="company_id",
    name_col="company_name",
    domain_col="domain_name",
    city_col="city",
    country_col="final_country",
    max_block_size=1000,
    use_domain=True,
    use_name3_country=True,
    use_compact5_city=True,
    # pairs_table="er.blocking_pairs",  # pass this if you already materialized pairs
  )
  with duckdb.get_connection() as con:
    con.execute(sql)
