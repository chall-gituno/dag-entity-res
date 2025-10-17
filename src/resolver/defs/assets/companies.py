import os
import zipfile
from pathlib import Path

import dagster as dg
from dagster import asset, AssetExecutionContext
from dagster_duckdb import DuckDBResource
from resolver.defs.sql_utils import render_sql
from resolver.defs.utils import get_latest_meta


def get_companies() -> str:
  """
  Would usually get it from an api or whatever (this came from kaggle)
  I think it came from https://www.kaggle.com/datasets/rsaishivani/companies-database
  but there are several others with similar name/size/fields.  This isn't
  about kaggle integration, so will need to manually download it.
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
    raise dg.Failure(f"No CSV files found after expanding {zip_path} into {base}")
  payload = {"dir": str(base), "csvs": csvs}
  return payload


@dg.asset(group_name="bronze", tags={"quality": "raw"})
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


@dg.asset(deps=["companies"], group_name="silver", tags={"quality": "clean"})
def clean_companies(context: AssetExecutionContext, duckdb: DuckDBResource) -> str:
  """Apply cleaning and normalization to our raw data.  output result to silver schema"""
  clean_comp = "silver.companies"
  bronze_count = get_latest_meta(context, "companies").get("rows")

  sql = render_sql("clean_kag_comp.sql.j2",
                   source_schema="bronze",
                   target_schema="silver")
  with duckdb.get_connection() as con:
    con.execute("create schema if not exists silver")
    con.execute(sql)
    n = con.execute(f"SELECT COUNT(*) FROM {clean_comp}").fetchone()[0]
  context.log.info(f"{clean_comp} saved with {n} rows")
  # should probably add asset_check for this...
  if n != bronze_count:
    context.log.warning("Lost some rows during cleaning!")
  context.add_output_metadata({"rows": n})

  return clean_comp
